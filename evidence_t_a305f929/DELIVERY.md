# t_a305f929：返修 R1（/clear 后旧代 ACP 正文绕过交付栅栏）

基线 `c0aa1d9`（阶段1 候选），分支 `feat/antigravity-acp`。未部署、未重启、未上传、未运行真实
agy/模型（含主实例）；只做候选内最小修改、离线测试与本地提交。真实 QQ 平台发送与真实提示词生效
未在本轮验证。

## 提交

- `348c1ac` test(antigravity): /clear 后旧代出站围栏回归（先 RED）
- `e796c4a` fix(antigravity): /clear 后旧代 ACP 正文继续受交付代际栅栏拦截
- 其后一个 `test(evidence)` 提交：R1 返修 RED/GREEN 原始输出与改动说明（本目录）

`348c1ac` 只含测试；在独立 worktree 上对 `348c1ac` 跑新回归得到 RED（3 failures / exit 1），
证明测试先于实现失败。`e796c4a` 只含生产代码。

## 改动文件与行号（`e796c4a`）

| 文件 | 位置 | 内容 |
|---|---|---|
| `core/hermes_client.py` | `ChatResult.delivery_generation`（1220-1226） | ACP 结果携带本轮入场代际；其他后端不设置（恒 None） |
| `core/hermes_client.py` | `chat()._out`（1671-1674） | `antigravity` 接口时把本轮 `delivery_generation` 挂到结果上 |
| `handlers/message_turns.py` | import（72） | 引入 `DeliveryContext` |
| `handlers/message_turns.py` | `_generation_fenced_delivery`（788-807） | 无 Runs 台账时用既有 `session_manager.get_delivery_generation` 构造只带代际 guard 的投递上下文 |
| `handlers/message_turns.py` | 私聊/群 passive 终态（1600-1608）、群 reactive 终态（2780-2788） | `_terminal_delivery` 为空（ACP 无 run 身份）时回落到代际栅栏；已有 Runs 台账路径优先，行为不变 |
| `core/outbound.py` | `DeliveryContext` 文档（43-50）、`_delivery_begin`（62-93） | 支持 `tracker=None` 的窄形态：出站前只核一次 `generation_guard`，无组件认领/回写；旧代失败关闭 |
| `core/outbound.py` | `_delivery_mark_unknown`（118）、`send_local_files`（502/528）、`_send_unimsg`（609）、表情分支（694） | 无台账形态跳过回执写入 |
| `tests/fixtures/fake_agy_acp.py` | `--gate-prompt=<p>`（18-20、69-88） | 夹具在普通 prompt 回复前等待释放文件，供 clear 竞态测试使用；无该 flag 时行为不变 |
| `tests/test_antigravity_clear_fence.py` | 新增 4 项 | 真 `_run_passive_turn`/`_run_reactive_turn` → 真 `HermesClient.chat` → 假 ACP 子进程 → 真 `core.outbound.send_text_with_media`；只替换官方平台边界 `alconna.UniMessage.send` 与隔离配置/存储 |

## 语义

- 复用既有会话键/代际：`SessionManager.get_delivery_generation` 在消息入场时已由 handler 取章并传入
  `chat(delivery_generation=...)`；返修只把它带到出站侧，在真实 `send_text_with_media` 前复核当前代际。
- 三个实际发送入口（私聊 passive、群 passive、群 reactive）都会在 `delivery is None` 时拿到代际栅栏；
  guard 失败 → 既有 `_delivery_begin` 抛 `DeliveryClaimBusy` → outbound 返回 `ok=False`，平台零调用。
- 未伪造 Runs run_id、未新增数据库/clear 状态机/通用后端框架；未改 runs 台账、恢复、后台轮询、MCP。
- `completions`/`astrbot` 不设置该字段，行为与之前一致；runs 路仍以 `run_id`+台账 `generation_guard` 为准。

## 原始结果（同目录日志）

- `clear_fence_RED_at_test_commit.log`：在 `348c1ac`（仅测试）上 `Ran 4 tests` / `FAILED (failures=3)` /
  `AssertionError: Lists differ: ['OLD'] != []` ×3，exit 1。
- `clear_fence_reviewer.log`：独立复审脚本 `review_clear_real_outbound_t_e6332b0b.py` → 4 tests OK；
  三路 clear 均 `late_old_body_sends: []`，`acp_session_ids: sess-1 → sess-2`，`new_sessions: 2`，
  `all_platform_sends` 末尾为新代 `OLD`；不 clear 对照 `all_platform_sends: ["OLD"]`。
- `clear_fence_new.log`：新增候选回归 4 tests OK。
- `focused.log`：`tests.test_antigravity_acp` 26 tests OK（历史绿灯复跑）。
- `turn.log`：`tests._bootstrap turn_tests.test_envelope_entry` 6 tests OK（历史绿灯复跑）。
- `reviewer_entry.log`：复审入口 6 tests OK；`reviewer_chain.log`：链路探针 exit 0（历史绿灯复跑）。
- `verify_final.log`：`verify.py` 22/22 PASS；其中全量 `Ran 745 tests ... OK (skipped=1)`
  （原 741 + 新增 4），网关插件 `48/21 OK`，交付树干净。

## 明确未做

部署、重启、上传、真实 agy/模型调用、真实 QQ 回执验证、真实提示词生效验证；不宣称可部署或生产验收通过。
