# t_049df793 — 最小返修：interim 共享标记边界、插件验收遗漏、部署文档事实

对象：候选 4df5c60 / b3b1fac，本轮基线 **6fbcceb**（已含 t_f9767f18 的 steer 区域化，未 reset、
未覆盖）。只修 reviewer t_4a749150 的三项发现；**未部署、未重启、未上传**，未新增状态机、
兼容层、逐帧发送或终态折叠。

## 1. 改动

| 文件 | 改动 |
| --- | --- |
| `core/runs_text.py` | 新增 `continue_after_shared_prefix(text, remainder)`：interim 是已缓冲 delta 的前缀时，若 interim 本身以独立整行 `§` 结尾，则把这个**两份正文共享的**标记按原输入接回尾部；裸正文没有标记可共享，原样返回。 |
| `core/hermes_client.py` | `message.interim` 的 `pending.startswith(text)` 分支改调上面的函数（原为裸切片 `pending[len(text):]`）。一行调用替换，发送节奏、批号、封段点、段内去重一律未动。 |
| `tests/test_runs_stream.py` | +4 个解析器用例：共享标记保留（含 interim 尾部换行两种写法）、连续多份、尾巴未闭合仍按 `missing_close` 拒绝、同文只发一次。 |
| `tests/test_queued_lane_interim_client.py` | +5 个真 client/handler 用例：R1 复现帧 `[A,B,C]`（组件 `primary:0/1/2`）、反序对照、换行有无三变体、尾巴未闭合只 `[A,C]`、同文只发一次。 |
| `gateway_plugins/queued-lane-interim-thin/tests/test_plugin.py` | R2：夹具从裸正文迁到分节符信封；stock 对照改成当前事实（部署副本已是信封解析器，不再拼 A+P）；+2 个用例（共享标记边界、尾巴未闭合仍拒绝）。**断言未削、插件生产代码未改。** |
| `verify.py` | 新增「网关插件独立测试（不在 tests/ 全量里）」：用 Hermes venv、离开仓库根目录、以 48/21 两个用例数下界单独设闸；`check_acceptance_cases` 纳入 `test_queued_lane_interim_client.py` 与本轮两个边界用例名。 |
| `DELIVERY_t_0ecd8f77.md` | R3：更正「只上提示词会多发一条空消息」的文案（实测见 §3），保留同批部署/回滚、只上新解析器拒绝旧裸正文、在飞旧格式 run 零发送三条结论。 |

### R1 修正判据（最小、可复核）

- 只保留**原输入里已经存在**的共享标记：`§\nA\n§\nB\n§` 裁掉 `§\nA\n§` 前缀后，把两者共享的那个
  `§` 还给尾巴 → `§\nB\n§`，完整 B 在工具边界发一次。
- 没给旧裸正文补信封：`text` 不以独立整行 `§` 结尾时原样返回（裸正文/半行标记行为不变）。
- 真缺开头/缺闭合仍拒绝：尾巴本身没闭合（`§\nA\n§\nB`）照旧不发，日志 `reason=missing_close`。
- 发送节奏未变：interim 仍是发送点、工具边界仍是封段点，B 不是「逐 delta 立即发」，而是在下一个
  封段点随 delta 尾巴发出。

### R2 迁移口径

- 插件夹具里的正文全部写成信封（`§\n正文\n§`、连发 `§\nA\n§\nB\n§`）；`NbClient` 仍按真实
  `RunsEventParser` + `decisions_from_text` 计数，断言比的仍是平台出口的**正文**。
- `test_stock_parser_replays_the_reviewer_gap` → `test_stock_parser_closes_at_interim_under_the_envelope`：
  部署副本 2026-11-11 14:34 已切信封解析器，旧用例「stock = 裸正文、不结段 → A+P」的前提不成立。
  当前事实：同一帧序在部署副本下也是 `[A, P, C]`；部署副本不是信封解析器时该用例显式 skip。
- 新增 `test_shared_marker_between_units_survives_queued_interim`：R1 的插件级复现，
  额外断言 `suppressed == []`（共享标记不再被记成 `missing_open`）。
- **全量数字口径**：`tests/` 的 682 **不包含**插件用例。完整口径是
  `tests/ 682 OK (skipped=1)` + `queued-lane-interim-thin 48 OK` + `steer-presentation-thin 21 OK`，
  由 `verify.py` 一个闸位同时把关。

### R3 更正后的部署文案（实测）

`git f5bbec8:core/runs_text.py`（旧解析器，只把 `§` 当分隔符）：

| 输入 | 旧解析器输出 |
| --- | --- |
| `§\n正文\n§` | `['正文']` |
| `§\n§` | `[]` |
| `§\nA\n§\nB\n§` | `['A', 'B']` |
| `§\nA`（未闭合） | `['A']` ← 半段也发 |
| `你好`（旧裸正文） | `['你好']` |

即：**只上新提示词不会多发空消息**。真正的差别是口径——旧解析器不校验闭合（被中断的半段仍会
发出），新旧不同代时回滚边界与「缺开／缺闭不发」的保证对不上。据此保留硬约束：解析器与提示词
同批部署、同批回滚；只上新解析器 → 旧裸正文（含在飞旧格式 run）全部 `no_marker` 零发送。

## 2. 真实测试输出

| 命令 | 结果 | 证据 |
| --- | --- | --- |
| 修前 6fbcceb + 本轮新测试（同一命令） | 8 failures / 60（R1 复现） | `EVIDENCE/r1_interim_shared_marker.txt` |
| 修后同一命令 | 60 OK（skipped=1） | 同上 |
| 全量 `tests/` | **682 tests OK (skipped=1)** | `EVIDENCE/full_tests_r1_fix.txt` |
| 插件 `queued-lane-interim-thin` | 修前 46 跑 11 失败 → 修后 **48 OK** | `EVIDENCE/plugin_suite_r1_fix.txt` |
| 部署副本对照 `steer-presentation-thin` | 21 OK（verify 闸内） | `EVIDENCE/verify_r1_fix.txt` |
| `verify.py` | **22/22 PASS**（清洁树） | 同上 |
| 基线解析器实测（R3） | 见上表 | `EVIDENCE/baseline_parser_f5bbec8.txt` |

唯一 skip 是 `tests/test_queued_lane_interim_client.py::test_deployed_parser_still_seals_at_interim`：
它要求部署副本仍是「裸正文 stock」，而部署副本 14:34 已切信封解析器，前提不成立——显式 skip，
没有改弱断言（同 t_f9767f18 已记录的环境变化）。

## 3. 明确没做的事（沿用已确认的非返工项）

- 不逐 delta 发送；interim/工具边界节奏一行未改。
- 终态与上一段同文照发的现有规则未改（`run.completed` 只认 `output`，同文重发不折叠）。
- queued-lane 薄插件自身的同文终态抑制保留（未动插件生产代码）。
- 提示词只改了 `DELIVERY_t_0ecd8f77.md` 的部署文案；`core/prompt_*.py` 一字未动。
- 没有新增兼容层/状态机：`continue_after_shared_prefix` 只回答「这个标记是否原输入里就有」。

## 4. 本轮新发现（未修，需产品/部署决定，不在本卡范围）

- stock 的「人类轮误回静默标记」替换文案（`gateway.errors.unexpected_silence`，如
  `⚠️ 对于一条需要回复的消息，模型只返回了静默标记。…`）是**裸正文**，被 queued-lane 薄插件
  原样发布成 `message.interim`；新信封解析器会按 `no_marker` 丢弃它（用户看不到这条提示）。
  这是信封协议与该 stock 逃生分支的交互，修它要么改插件生产逻辑（本卡禁止），要么改 stock，
  二者都需要单独决定；本卡只把插件用例保持成「帧级发布」事实，没有改插件、也没有假装修好。
- `gateway_plugins/session-compress/tests` 在本机 Hermes venv 下缺 `aiohttp` 无法导入（1 error），
  与本轮无关，未纳入 verify 闸位（闸位只含 queued-lane 48 与 steer-presentation 21）。

## 5. 回滚

删本次提交即可；没有部署动作、没有运行时状态、没有新配置项。`core/runs_text.py` 的新函数
只被 `core/hermes_client.py` 一处调用，回滚两者即恢复到 6fbcceb 行为。
