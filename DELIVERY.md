# DELIVERY t_302b7950 — 阶段2B：ACP 权限 once/deny 与 /run-stop 取消

分支 `feat/antigravity-acp-2b`，基线 `d64c845`（阶段2A＋steer 免防抖），候选提交
**`8fc8156`**。只改独立工作区；未部署、未重启、未上传、未跑真实 `agy` 模型、
未触碰肥鱼与主实例。真实机验收由主代理在肥鱼执行。

## 1. 逐文件改动

### 1.1 `backends/antigravity/client.py`

- `session/request_permission` 从「统一自动拒绝」扩为审批链：
  - 登记 `ACPPendingPermission`（内存，key=`(session_key, str(原 RPC id))`，`request_id`
    保留原始对象；真实 `agy` 的 `jetski-acp-front:2` 字符串原样回写，不转 int、不重编号）。
  - 只调用**本轮** `approval_callback`（`ACPTurn.approval_callback`，steer 轮继承同轮回调）；
    通知在独立 task 里跑，reader 不被 QQ 发送阻塞。
  - 无 callback／未知 session／无 id：`_reject_option` 安全拒绝（沿用首片行为，不扩大授权）。
  - `answer_permission(session_key, request_id, choice)`：`once` 只按真实 options 的
    `kind=allow_once` 取 optionId，`deny` 走既有 reject 优先链；**写帧成功才返回 True**。
    未知 request／已过期／重复应答／没有对应 kind／写管道失败一律 False（失败关闭，留单）。
  - 过期：按既有 `misc_config.approval_expiry_sec`（与 `/approve` 同源）起定时任务，到期
    自动 reject_once，agent 不会永久挂起；应答/取消/收尾都会取消定时器。
- 新增 `ACPCancelledError`（`ACPError` 子类）。
- 新增 `cancel(session_key)`：无在跑轮 / 进程未起 / 正在收尾返回 False；否则发**无 id**
  `{"method":"session/cancel","params":{"sessionId":...}}`，随后按 reject_once 关掉该
  session 挂起的权限 RPC（agent 卡在权限上时也能收尾）。不伪造 run_id。
- `stopReason=cancelled` 在 `_dispatch` 派发点识别：标记本 session 所有在跑 prompt 为
  cancelled 并取消兄弟轮 future（steer B/C 同时挂起时也能立刻结束等待链，不等到超时）。
- `prompt()` 新增 `approval_callback` 参数；取消时 `emit=False` 丢弃尚未出站的余量，抛
  `ACPCancelledError`，不返回半截正文；`_finalize_turns` 统一清理 pending 权限与定时器；
  `_terminate_process` 清理权限内存与通知 task。

### 1.2 `core/hermes_client.py`

- ACP `prompt()` 传本轮已有 `approval_callback`。
- 新增 `except ACPCancelledError`：返回 `ChatResult(raw_text="", is_interrupted=True)`，
  走既有被停 turn 硬静默；`finally` 里 `reply_queue.finish(cancelled=True)` 丢弃 reply_queue
  未发余量（实际已发 QQ 消息保留）。Runs／completions／AstrBot 路径未改。

### 1.3 `core/approvals.py`

- `PendingApproval` 新增 `backend`（缺省 `runs`）与可选 `session_key`，随现有 JSON 原子写
  持久化；`add_pending` 只加两个关键字参数，Runs 调用点签名与行为不变。
- `build_admin_notice` 按后端分支：ACP 通知无 `run:` 行，选项与回复提示只有 once／deny，
  绝不出现 session／always；Runs 文案逐字节不变。
- `bind_storage` 与 `renotify_pending` 跳过非 runs 条目：NB 重启后旧 ACP 权限请求已随旧
  子进程失效，不装回、不重通知；Runs 持久恢复不变。

### 1.4 `handlers/message_feedback.py`

- 新增 `_normalize_acp_approval`／`_acp_command_summary`：规范化 ACP 的 title／rawInput／
  options／request_id／session_key，只从 `rawInput.CommandLine`（或 `command`）取命令行
  摘要（截 200 字），其余参数不进通知与存档。
- `_make_approval_notify` 按 `approval["backend"]` 分支复用同一 `add_pending` +
  `notify_admins`；ACP 条目 `run_id=""`、choices 固定 `once/deny`、带 `session_key`。

### 1.5 `handlers/commands.py`

- `/approve` 按后端分支：
  - ACP 的 usage、管理员通知、命令面只出现 `once|deny`；手动输入 session／always 等其他值
    只回当前后端正确用法，不报“不支持”、不调 ACP、不删条目。
  - 条目按 `entry.backend` 应答：ACP → `answer_permission`（原 session_key＋request_id），
    Runs → 既有 `hermes_client.respond_approval`（HTTP 路径与文案不变）。
  - 保持同一 `answer_lock`、`peek → 应答成功 → acknowledge`、失败留单。astrbot 仍走原
    `_HERMES_CONTROL_UNAVAILABLE` 门。
- `/run-stop` 在 ACP 下：`session_manager.get_session_key(...)` 取当前会话，调
  `AntigravityACPClient.cancel`；有在跑轮回“已取消…（协作式，正在收尾）”，无则回“没有可取消
  的对象”，回执写当前会话且不含伪造 run_id。AstrBot/Runs 原提示与流程不变。

### 1.6 `handlers/message_turns.py`

- passive 与 reactive 的 `approval_callback` 条件从 `== "runs"` 扩到
  `in ("runs", "antigravity")`（仅接线，未改其他行为）。

### 1.7 测试与夹具

- 新增 `tests/test_antigravity_permission.py`（19 项）：真 handler→真 ACP client→真 approvals
  →真 `/approve`；once／deny 的原 id 与 option kind、成功才删单、通知只 once|deny、其他输入
  零后端调用且留单、非管理员隐身、通知失败留单、群 reactive 作用域、重启不恢复 ACP、Runs
  审批／停止回归。
- 新增 `tests/test_antigravity_cancel.py`（9 项）：有／无在跑轮 stop、无 id session/cancel、
  cancelled→is_interrupted 硬静默、reply_queue 余量丢弃、steer B/C 途中取消、权限等待中取消
  （reject_once 关原 RPC）、取消后新 prompt 复用同一进程与会话、群 `/run-stop` 不串私聊。
- 新增 `tests/acp_2b_harness.py`（非测试模块）：共用「假 ACP→真 client→真 handler→真出站」
  夹具，只替换平台发送边界、管理员白名单与隔离配置。
- `tests/fixtures/fake_agy_acp.py`：权限请求可配原 RPC id／title／rawInput／options；
  `session/cancel` 按无 id 通知处理（本 session 在等 prompt 逐条回 `stopReason=cancelled`）；
  出站帧另记 `*.sent`。

## 2. TDD：RED → GREEN

RED（`EVIDENCE/acp2b_red.txt`，在基线上只放新测试与新夹具运行）：

```
Ran 26 tests — FAILED (failures=1, errors=21)
14× TypeError: AntigravityACPClient.__init__() got an unexpected keyword argument 'permission_timeout'
 4× ImportError: cannot import name 'ACPCancelledError'
 2× TypeError: add_pending() got an unexpected keyword argument 'backend'
 1× AttributeError: 'AntigravityACPClient' object has no attribute 'cancel'
 1× AssertionError: 2 != 1 : 旧 ACP 请求随旧子进程失效，重启不得装回
```

GREEN（真实命令与输出，全部为工作区 venv `python -B`）：

```
$ python -B -m unittest tests.test_antigravity_permission tests.test_antigravity_cancel -v
Ran 28 tests in 3.476s
OK                                        → EVIDENCE/acp2b_green_focused.txt

$ python -B -m unittest tests.test_antigravity_acp tests.test_antigravity_stream_steer \
    tests.test_antigravity_clear_fence tests.test_antigravity_permission \
    tests.test_antigravity_cancel tests.test_control_regressions tests.test_steer_pre_debounce
Ran 112 tests in 12.760s
OK                                        → EVIDENCE/acp2b_green_related.txt

$ python -B -m unittest discover -s tests -q
Ran 814 tests in 45.641s
OK (skipped=1)                            → EVIDENCE/acp2b_green_full.txt

$ python -B -m unittest tests._bootstrap turn_tests.test_envelope_entry -q
Ran 6 tests in 0.051s
OK                                        → EVIDENCE/acp2b_green_turn.txt

$ python -B verify.py    # 在候选提交 8fc8156 的临时 git worktree 内（与提交逐字节相同）
ALL PASS: 22/22 项通过                    → 含「交付树干净」「无 pyc」
```

补充（主工作区、提交后）：`verify.py` 同样 22/22（交付文档单独提交后见
`EVIDENCE/acp2b_green_verify.txt`）。

## 3. 边界与已知行为

- 只取消仍在 ACP 执行的轮；正文已全部生成、仅停在 QQ 3 秒节奏队列的余量，在
  `is_interrupted` 时按取消丢弃，不新建 Runs 式跟踪。实际已发的 QQ 消息保留。
- ACP 过期秒数沿用 `misc_config.approval_expiry_sec`，ACP 侧到期自动 reject_once；审批条目
  本身仍按既有口径懒剔除（与 Runs 相同，失败应答提示“条目保留，可重试”）。
- 没做：run-status、MCP、媒体、跨重启在跑恢复、后台回报、通用控制框架、新配置键。
- 没有把 ACP 权限映射成 allow_always／session：真实 options 没有这些等级。

## 4. 肥鱼真实验收步骤（主代理执行，不在本卡范围）

前置：肥鱼 `AGENT_API_INTERFACE=antigravity`，重启后确认 ACP 握手正常；只动肥鱼。

1. **deny**：私聊让它跑一个需要工具的无害命令（如 `pwd`）。预期管理员私聊收到审批通知，
   选项与提示只有 `once / deny`（无 session／always）；`/feiyu approve deny` 后回执
   “已拒绝”，agent 工具失败但正常收尾，随后普通消息仍能回答（进程存活）。
2. **once**：同类请求回 `/feiyu approve once`。预期回执“已放行一次”，工具实际执行，turn
   正常结束；再次需要权限时必须重新审批（不是永久放行）。
3. **用法分支**：ACP 下 `/feiyu approve session` 只回 `<once|deny>` 用法，不删条目、不调 ACP；
   再 `/feiyu approve deny` 可正常答掉同一条。
4. **长输出取消**：让它输出多段长正文，首段到达 QQ 后发 `/feiyu run-stop`。预期立即回执
   “已取消当前 ACP 会话正在执行的轮”，后续未发余量不再出现，无“调用失败”兜底文本；随后
   再发一条普通消息证明同一 ACP 会话与进程可继续使用。
5. **无在跑轮**：空闲时 `/feiyu run-stop` 回“没有在跑的 ACP 轮，没有可取消的对象”，不发
   `session/cancel`。
6. **权限等待中取消**：出现审批后不回答，直接 `/feiyu run-stop`。预期原权限 RPC 被
   reject_once 关闭、turn 以取消收尾、无挂起审批残留（过期前条目可被 sweep 掉）。
