# DELIVERY t_dfb56bd9 — 阶段2A 接管边界最小返修（R1/R2）

分支 `feat/antigravity-acp`，返修前 HEAD `3a1eb63`（产品四文件与复核候选 `a566e98`
逐字节一致）。只改 `backends/antigravity/client.py` 的轮次边界与回归测试；**未**部署、
未重启、未推送/上传、未跑真实 `agy` 或模型、未动生产配置，也未修改
`review_t_bf528528/` 复核材料。

## 1. 改了什么（两处，均在同一 ACP 客户端内）

### R1：确定旧轮边界时先封出剩余合法单位（`_on_notification` + `_seal_turn_remainder`）

`user_message_chunk` 到达即是旧轮正文结束的确定边界。此前只把 `_streaming_turn`
切到新轮，旧轮帧尾那份「没有末尾换行」的合法 `§` 只能等到整批 `_finalize_turns`
才补出，于是晚于新轮正文出站（A1/B1/A2）。

现在边界处先 `_seal_turn_remainder(旧轮)`，再切换正文归属：

- 仍复用 `core.runs_text` 单一解析器（`split_text_units`，与终态补发同一个函数），
  没有第二套解析规则；
- 边界确定后，帧尾那个独立整行 `§` 就是合法结尾 → `§\nA2\n§` 封出 `A2`；
- 真正缺末尾标记（`§\nTAIL-NO-CLOSE`）照旧丢弃，不补造正文；
- 不触碰 `take_closed_units` 的流式判据（`frame §` 仍不当标记），因此跨帧拼接语义不变；
- 单轮终态补发、`turn.sent` 段内去重、`_text_errors` fail-closed 全部沿用：
  已发出的单位不会重发，正文单位不合法时边界照样零发送。

### R2：按已提交 prompt 的轮序归属回显（`_boundary_turn`）

此前每个 `user_message_chunk` 无条件绑定 `_latest_turn`。B 的回显到来前 C 已提交时，
latest 已是 C，B/C 正文都归 C，B 轮保持空；两轮同文又被 `turn.sent` 段内去重吞掉一份。

现在边界归属取「**当前正文轮之后的第一轮**」——`self._turns[session_key]` 本身就是
提交顺序追加的列表，因此回显按 B→C 的真实轮序各归其轮：

- 不拿「最新轮」代替「此次开始的轮」，不按正文哈希/时间猜身份；
- 没有新增队列、台账或交付数据库：待接管顺序直接来自既有 `_turns`，
  `steer()` 提交失败回退时 `turns.remove(turn)` 自动保证顺序正确；
- 只有当前轮之后再无待接管轮时（首条 prompt 自己的回显）才沿用最新轮，行为与修复前一致；
- 跨轮同文各自一条，段内同文仍然只一条。

## 2. TDD：先在**未修**候选重演 RED，再最小 GREEN

RED 统一在「产品文件回退到 `3a1eb63`（= `a566e98`）、只保留新测试」的树上跑，
GREEN 用同一命令在修复后跑（原始输出见 `evidence_t_dfb56bd9/`）。

| 步骤 | 命令 | 结果 |
| --- | --- | --- |
| 复核独立验收入口（未修树） | `REVIEW_TREE=$PWD python -B review_t_bf528528/review_boundary_tests.py` | Ran 9，failures=8（`boundary_acceptance_RED.log`） |
| 5 条新测试（未修树） | 见 §4 第 2 条 | Ran 5，failures=8（`new_tests_RED.log`） |
| 复核独立验收入口（修复后） | 同上 | Ran 9，OK（`boundary_acceptance_GREEN.log`） |
| 5 条新测试（修复后） | 同上 | Ran 5，OK（`new_tests_GREEN.log`） |

未修的失败签名（与复核报告一致，非夹具错误）：

```
EOF 边界（客户端）      ['A1', 'B1', 'A2'] != ['A1', 'A2', 'B1']
EOF 边界（三路平台）    private/passive/reactive 全部 ['A1', 'B1', 'A2']
快 B/C（客户端）        [('A1',1),('SAME',3)]，第二份被吞
快 B/C（三路平台）      private/passive/reactive 全部 ['A1', 'SAME']
```

修复后复核入口的原始行为行：

```
EOF_BOUNDARY ['A1', 'A2', 'B1']
PLATFORM_EOF private/passive/reactive ['A1', 'A2', 'B1']
FAST_BC [('A1', 1), ('SAME', 2), ('SAME', 3)]
PLATFORM_FAST_BC private/passive/reactive ['A1', 'SAME', 'SAME']
```

## 3. 新增测试（`tests/test_antigravity_stream_steer.py`）

| 用例 | 覆盖 | 修复前 |
| --- | --- | --- |
| `ACPSteerTakeoverTests::test_prior_turn_terminal_marker_surfaces_before_new_turn` | 旧轮末份无末尾换行 `§` 在边界先封出 | RED |
| `ACPSteerTakeoverTests::test_boundary_seal_never_promotes_unclosed_tail` | 护栏：真缺末尾标记不得被补造（基线同样通过，记录在案） | PASS（护栏） |
| `ACPSteerTakeoverTests::test_fast_b_c_before_b_echo_keeps_each_turn_boundary` | 回显前已提交 C：两份同文按轮序各归其轮（`_turn_seq` 不同） | RED |
| `HandlerStreamTests::test_prior_turn_marker_precedes_steer_turn_all_routes` | 私聊/群 passive/群 reactive 三路真实 handler→outbound | RED（3 路） |
| `HandlerStreamTests::test_fast_b_c_before_echo_all_routes` | 三路真实 handler 的快 B/C 两份同文 | RED（3 路） |

链路仍是既有离线夹具：假 `agy acp` stdio 子进程 → 真 `AntigravityACPClient` →
真 `HermesClient.chat` → 真 passive/reactive handler → 真 `core.outbound.send_text_with_media`，
只替换平台最终发送边界与隔离配置/存储。

## 4. 回归与门禁（原始输出同目录）

1. focused ACP：`python -B -m unittest tests.test_antigravity_stream_steer tests.test_antigravity_acp tests.test_antigravity_clear_fence`
   → **Ran 59, OK**（复核报告 a566e98 的 53 + `ef589f1` 权限拒绝补充 1 + 本片新增 5）。
2. 新测试 5 项：
   `python -B -m unittest tests.test_antigravity_stream_steer.ACPSteerTakeoverTests.test_prior_turn_terminal_marker_surfaces_before_new_turn ...test_boundary_seal_never_promotes_unclosed_tail ...test_fast_b_c_before_b_echo_keeps_each_turn_boundary tests.test_antigravity_stream_steer.HandlerStreamTests.test_prior_turn_marker_precedes_steer_turn_all_routes ...test_fast_b_c_before_echo_all_routes`
3. 相关面（antigravity 三套 + `test_runs_*` / `test_steer_*`）→ **Ran 338, OK**。
4. 全量：`python -B -m unittest discover -s tests -t .` → **Ran 774, OK (skipped=1)**
   （基线 768 + 5 新 + `ef589f1` 权限补充 1）。
5. turn 门：`python -B -m unittest turn_tests.test_envelope_entry` → **Ran 6, OK**。
6. `verify.py`：在候选提交导出的干净快照树（自带独立 Git 根）里运行 → **ALL PASS 22/22**
   （含「交付树干净」「无 pyc」，网关插件 48 / 21 分别 OK）；
   全量与 turn 门在该快照树复跑同为 774(skip 1) / 6。
   原始输出：`verify_clean_snapshot.log`、`full_tests_snapshot.log`、`turn_snapshot.log`。
   （共享树里有非本片产出的 `review_t_bf528528/`，其干净树检查必然不过，故用快照树。）

## 5. 保留的控制（复核验收 9/9 全绿的同一入口）

- 普通单轮流式：首份正文在 turn 结束前到平台，终态补份只发一次。
- 单轮末尾无换行的合法 `§` 仍在终态补发（`'§\nEOF\n§'` → `['EOF']`）；
  真正缺闭合仍是零发送（`'§\nNO_CLOSE'` → `[]`）。
- C 等 B1 之后才发（既有用例）与 C 在 B 边界前（新增）两种窗口都对。
- 跨轮同文两份都发；段内同文仍只一份。
- 私聊 / 群 passive / 群 reactive 三路真实 handler 均覆盖。
- `/clear` 旧代：已发旧正文保留，clear 后在途旧代零出站，新代正常（三路）。
- 慢平台：平台发送卡住时 ACP reader 仍读完 RPC 并清理轮次，handler 仍等
  `reply_queue.finish` 排空；放行后 A1/A2 有序。
- 真实 3 秒条间节奏（`REAL_PACING_CONTROL ['A1','A2','A3'] [3.0043, 3.0036]`）。

## 6. 已知边界（本轮不扩范围）

- `user_message_chunk` 若被 agent 拆成**多份**流式送达同一 prompt，每份都会推进一次
  边界（无法在不猜身份的前提下区分「同一 prompt 的后续碎片」与「下一条 prompt 的回显」）。
  复核证据与真实观测都是每 prompt 一份回显；本轮不新增猜测式判据，按已知边界记录。
- 平台发送失败不自动重试、人工 stub 直接抛 SQLite 等仍是复核报告已标明的非 blocker
  能力边界，本轮未加规则、未加可靠交付系统。
- 没有 MCP / 媒体 / 权限审批 / run-stop / 后台恢复等后续功能；没有改 handler、
  `core/` 队列、outbound 或其它后端。

## 7. 复跑入口

```bash
cd /home/neonsky/nonebot-plugin-hermes/work/nph-20-work/antigravity-acp-20261113
REVIEW_TREE="$PWD" /home/feiyu/NeonBot/.venv/bin/python -B review_t_bf528528/review_boundary_tests.py
/home/feiyu/NeonBot/.venv/bin/python -B -m unittest discover -s tests -t .
```

验收完成后由主代理另行安排独立复核；本片不自行部署，也不宣称已完成真实模型验收。
