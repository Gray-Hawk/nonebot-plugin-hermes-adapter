# DELIVERY t_64505f4f — 阶段2A：ACP 流式正文与运行中 steer 接管

分支 `feat/antigravity-acp`，基线 `b885e0e`（候选提交以分支 HEAD 为准）。只提交
候选、测试与交付说明；
未部署、未重启、未上传、未跑真实 `agy` 模型（主实例也没有启动 agy）。
真实 A→B→C 验收由主代理稍后只在肥鱼执行。

## 1. 做了什么

### 1.1 ACP 流式正文（`backends/antigravity/client.py`）

- `prompt()` 新增 `on_unit` 回调：只消费 `agent_message_chunk` 的正文本，用
  `core.runs_text.take_closed_units` 增量拆**已闭合的 § 单位**，一到就同步回调；
  `thought`／`plan`／工具状态仍然不进 QQ。
- 每个 prompt RPC 是一条 `ACPTurn`（本文件内的最小接管状态，不是第二套交付系统）：
  段内全文去重（跨轮同文照发）、未闭合碎片留到终态。
- 终态只补发本批各轮**尚未回调过**的闭合单位（`split_text_units` + `turn.sent`
  全文匹配），已流式投递的正文绝不整段重发；缺开头/缺末尾的段照旧丢弃。
- 轮次收尾后 `_turns/_latest_turn/_streaming_turn` 全部清理，游离 chunk 不再归属
  任何一轮（不能混入下一轮）。
- `steer(session_key, text)`：在同 ACP session 上直接发第二条 `session/prompt`，
  不排队等第一条 RPC。实测语义落地：
  - 第一条 RPC 提前 `end_turn` **不**当作会话输出结束；prompt 循环继续等 newest
    轮，终态归属最新 prompt；
  - 旧正文在第一条 RPC 结束后继续流出，仍写旧轮；
  - `user_message_chunk` 是轮次边界：边界之后的新 chunk 才归最新 prompt；
  - 连续 B/C steer 都复用同一 `sessionId`，按到达顺序推进；
  - 本 scope 正在收尾（`_finishing`）、进程未起或无在跑轮时返回 `False`，调用方
    按既有排队语义重投，零丢失。
- 正文单位不合法仍 fail-closed（本轮零发送，`ACPError` → 既有传输失败面）。

### 1.2 接进既有结构化正文队列（`core/hermes_client.py`）

- antigravity 分支把 `on_unit` 接到现有 `_RunsReplyQueue`（`run_id=""`，不伪造
  Runs 身份）：同一 3 秒条间节奏、`decision_callback` 即时发送、`streamed_sink`
  记录。没有决策回调（非 handler 直调）时不挂队列，正文仍走终态解析，行为与阶段1
  一致。
- 终态不重新解析整段：`terminal_decisions = streamed_decisions`（同一批 dict），
  由 handler 只做控制/收口，正文不重发。
- `reply_queue.finish()` 在 ACP 分支 `finally` 收口，未完成发送不会被下一轮截断。

### 1.3 handler 链路（`handlers/message_turns.py`）

- 新增 `_make_acp_live_decisions`：无 Runs 台账的即时发送器，复用同一套
  `send_text_with_media`／`send_local_files`／历史／反馈原语，**不伪造 run_id**；
  每次出站前用 `DeliveryContext(generation_guard=…)` 复核 `/clear` 代际（与
  runs 路同口径）。不设置 `reply_batch_busy`：ACP steer 允许生成中再发 prompt。
- 私聊 passive、群 passive、群 reactive 都启用即时发送：
  `_live_enabled = use_private_decision and interface in ("runs","antigravity")`；
  reactive 的 `decision_callback` 同步放开 antigravity。
- 抽 `_generation_guard_context()`（终态 `_generation_fenced_delivery` 改为复用）。
- reactive 终态循环：无台账后端的已即时发送份**只补轮级控制与历史/收口，不重发
  正文**；即时发送失败的份才落到正常发送路径重试一次。

### 1.4 steer 呈递复用（`handlers/message_relay.py`、`handlers/message_turns.py`）

- `_dispatch_chat` 的直注入口从 `runs` 扩到 `runs/antigravity`。
- 新增 `_acp_active_turn()`：按本模块 `plugin_config` 判接口，再用既有
  `SessionManager.get_session_key` 找在跑 ACP 轮（只读、不启动进程）。
- `_steer_text_into_live_run` 增加能力分发：**有在跑的 ACP 轮**才走
  `_steer_text_into_acp_turn`，否则完全保持既有 Runs 台账路径（runs 测试不受影响）。
- `_steer_text_into_acp_turn`：复用同一份 `render_steer_batch` 呈递（m:id、作者
  身份、聊天区外观），发到同 session 的后续 prompt；不写 Runs 台账、不伪造 run_id；
  `steer_seen` 呈现去重与无号行语义沿用；失败返回 `ok=False` 落回正常排队。
- 把逐行 m:id 去重抽成 `_steer_rows_for_send()`，Runs 台账路与 ACP 路共用（同号
  异人/异文仍整批回退）。
- `_steer_eligible_ids` 群聊资格补上「在跑 ACP 轮的触发者」；
  `_render_steer_presentation_for_scope` 的 bot 身份在无 Runs 行时改读
  `turn.meta["bot_self_id"]`（与 current 行同一事实源）。

## 2. 硬边界遵守

- 没有 MCP、媒体、hook、SDK、后台恢复、独立桥、通用后端框架或新数据库；
- 没有伪造 Runs `run_id`，没有复活 JSON 决策信封；
- 没有改 persona/model 配置与 Hermes 其他后端行为；
- `request_permission` 仍是统一 `reject_once`（权限审批与 /run-stop 留给下一片）；
- 主实例未运行 `agy`，测试只用 `tests/fixtures/fake_agy_acp.py`。

## 3. TDD：RED → GREEN

先 RED 后 GREEN，按 tracer bullet 分片推进（客户端流式 → 客户端 steer 接管 →
handler 私聊/群 passive/reactive → steer B/C → clear 旧代）。

- RED 证据：`EVIDENCE/antigravity_stream_steer_red.txt`
  - 把改动前的 HEAD 树导出到临时目录，只覆盖新测试与夹具后运行；
  - 15 项里 14 项失败/报错：8 项
    `TypeError: AntigravityACPClient.prompt() got an unexpected keyword argument 'on_unit'`
    （含流式权限回归项），1 项 `AttributeError: ... has no attribute 'steer'`，5 项
    handler 链路 `A1 未出现，turn 已结束: platform=['A1','A2']`（正文只在终态整段
    发送，证明测试抓的正是「流式未接通」）；唯一通过的是静默轮（阶段1 已支持，
    不是新能力）。
- GREEN 证据：`EVIDENCE/antigravity_stream_steer_green.txt`
  - focused：`tests.test_antigravity_stream_steer` 15 项 OK；
  - 相关：antigravity（39）+ steer/runs 流式共 233 项 OK；
  - turn 门：`turn_tests.test_envelope_entry` 6 项 OK；
  - 全量：`unittest discover -s tests` 769 项 OK（skipped=1）；
  - `verify.py` 22/22 PASS（含「交付树干净」「无 pyc」）。收尾时共享工作区出现
    并行评审目录 `review_t_bf528528/`（非本片产出），为拿到清洁树证据，上述全量与
    verify 在候选代码树的临时 git worktree 中运行（与最终提交的代码/测试逐字节
    相同，仅交付说明与证据文件不同），输出已一并存进 GREEN 证据。

### 覆盖的行为

| 场景 | 断言 |
| --- | --- |
| 普通流式 | 首份正文在 turn 结束前到平台；终态补的 A2 只发一次 |
| 第一条 RPC 提前 end_turn | 旧正文继续流出、终态归第二 RPC；同 session |
| B/C 连续 steer | 顺序 A1/A2/B1/B2/C1，三次 prompt 同 session |
| 共享 § 边界 | chunk 跨标记拼接仍只拆出闭合单位（不重复/不漏） |
| 跨轮同文 | 段内去重不跨轮：新一轮同文照发两条 |
| 游离 chunk | 轮次收尾后的 chunk 不进入下一轮 |
| 静默 | 无单位、零发送、`should_reply=False` |
| 权限拒绝 | 流式中 `request_permission` 仍在原 RPC id 回 `reject_once`，正文不受阻 |
| clear 旧代 | clear 前已发的旧正文保留；clear 后在途旧代正文/终态补发零出站；新代正常发送 |
| 私聊 / 群 passive / 群 reactive | 三条真实 handler 链路都流式、不重发 |

## 4. 肥鱼真实 A→B→C 验收步骤（主代理执行）

前置：`AGENT_API_INTERFACE=antigravity`，`AGENT_ANTIGRAVITY_PERSONA=nano`
（或既有验证配置），重启肥鱼后确认 ACP 握手正常。全程不触碰主实例。

1. **A 普通流式**：私聊发一条需要多段回答的消息（明确要求分 2–3 条短回复）。
   预期：第一条 § 单位在回答结束前就出现在 QQ，条间约 3 秒；最后一段只出现一次。
2. **B 运行中 steer**：A 正在输出时，立刻再发一条私聊消息（例如「补充：只回答
   第二点」）。预期：旧正文继续流出，随后 B 的内容接着出现；B 不丢、不重复；
   ACP 日志有 `steer 已接管在跑轮`，wire 上第二次 `session/prompt` 的
   `sessionId` 与第一次相同。
3. **C 连续 steer**：B 还在输出时再发「再补一句 C」。预期顺序 A→B→C，无交错、
   无重复；终态由 C 的 RPC 收尾。
4. **群聊对照**：群内 @bot 触发同样多段回答，再 @bot 插入 B/C（同一触发者在资格集
   合内）。预期同私聊；未 @ 的闲聊不直注，走既有排队。
5. **/clear 竞态**：A 输出第一段后立即 `/clear`。预期：clear 前已发的第一段保留，
   clear 后旧代剩余正文/终态补发零出站；新会话正文正常。
6. 全程检查平台侧：无重复消息、无旧代消息；ACP stderr 无权限卡死（仍 reject_once）。

回归对照（可选）：把 `AGENT_API_INTERFACE` 切回 `runs` 跑一轮普通对话，确认
runs 路 steer/流式行为与基线一致（本片未改 runs 分支）。

## 5. 已知边界

- `request_permission` 仍统一拒绝；需要工具调用的任务会被拒绝（下一片处理）。
- 没有 `/run-stop`；中断仅靠 ACP 超时后的 `session/cancel`。
- ACP 仍只支持纯文本：图片/文件在调用前本地拒绝。
- steer 只沿既有入口：私聊恒资格；群聊仅委托（@bot）且发送者在资格集合内。
