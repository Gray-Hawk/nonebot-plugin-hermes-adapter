# t_049df793 — 信封候选收口：入队前结构化正文队列（候选）

对象：4df5c60 / b3b1fac 起的最小返修链；基线 **6fbcceb**（已含 t_f9767f18 的 steer 区域化，
未 reset、未覆盖）。主人 2026-11-11 15:23 定案：放弃 `continue_after_shared_prefix` 这类共享
前缀兼容补丁，改为**入队前结构化正文队列**。本轮按此重做 R1；R2/R3 结论保留。**未部署、未
重启、未上传**，生产继续使用主人选择保留的现版。

## 1. 改动规模与结构

| 文件 | 改动 |
| --- | --- |
| `core/runs_text.py` | 新增 `take_closed_units()`（增量拆分：只把闭合的整行信封拆成正文单位，返回未闭合碎片）、`rebase_after_interim()`（段边界只决定碎片去留）、`_marker_tail`／`_terminate_marker_line` 两个小工具；新增 `TEXT_UNIT_KIND = "text"`。**删除** `continue_after_shared_prefix`。`split_text_units` 一字未改。 |
| `core/hermes_client.py` | `RunsEventParser`：`message.delta` 改为 `_ingest_delta`（每形成完整闭合信封立刻按顺序进现有 `_stream_pending`），`_seg_text` 只留未闭合碎片；interim 按完整单位全文去重后用 `rebase_after_interim` 决定碎片；新增 `terminal_decisions()`（终态只压掉**本段**已发出的同一份正文）。`_RunsReplyQueue`／FIFO／批号／3 秒节奏／控制工具一行未改。 |
| `core/runs_terminal.py` | `_text_unit` 的发送单元 dict 增加轻量 `kind="text"`（未来工具显示可复用同一入口）。计划字段与 `_unit_plan_entry` 未变。 |
| `tests/test_runs_envelope.py` | 新增 `EnvelopeIncrementalQueueTests`：闭合即入队、帧尾未终结 `§` 不是标记、共享标记复用、CRLF/CR、缺开头、未闭合、interim 补全半份碎片、`kind`、终态同段去重原因。 |
| `tests/test_runs_stream.py` | 原 `continue_after_shared_prefix` 单测改为新结构判据（`take_closed_units` + `rebase_after_interim`）。 |
| `tests/test_queued_lane_interim_client.py` | 真 client/handler 新增「终态不重发本段已发正文」；R1 复现、反序、换行、CRLF/CR、四个行内反例、缺闭合、queued-lane ABC 全部保留。 |
| `tests/test_runs_reply_pacing.py` | 取消用例按新结构校正一处期望：delta 里已闭合的 `乙` 在取消帧前就已入队且是新批首份（立即发出）；被 3 秒节奏挡住的 `丙` 仍不发、闸仍释放。 |
| `gateway_plugins/queued-lane-interim-thin/tests/test_plugin.py` | R2 夹具：共享标记用例的 `suppressed` 断言改成精确 `[("segment_duplicate", A)]` 并保留「不得出现 missing_open」；插件生产代码未改。 |

合计 **8 文件 +370/−83**（含测试与注释；生产逻辑只落在 `runs_text.py` / `hermes_client.py` /
`runs_terminal.py`：新增 2 个协议函数（`take_closed_units`、`rebase_after_interim`）+ 2 个内部
行工具、4 个解析器方法（`_queue_units`、`_ingest_delta`、`_fail_text`、`terminal_decisions`），
删除 1 个兼容函数（`continue_after_shared_prefix`）；`_RunsReplyQueue` 零改动）。

**没有第二套发送队列／状态机：**

- 全仓只有一个 `_stream_pending`（`RunsEventParser` 原字段），`drain_stream_decisions()` 原样消费；
- `git diff 610a9d4..HEAD -- core/hermes_client.py` 对 `_RunsReplyQueue` 只有注释提及，类体零改动；
- 没有新增队列类、注册表、抽象基类、插件或第二套协议；`take_closed_units` 与
  `split_text_units` 共用同一套「独立整行 §」判据。

## 2. 判据（与上一版的差别）

1. **delta 闭合即拆**：`take_closed_units` 每看到两份相邻的整行 `§`（且闭合标记行已被换行
   终结）就把中间正文作为独立单位入队；相邻正文共享的 `§` 在拆分这一步一次处理完，不再对
   任何原串做前缀裁剪，也不给它补标记。
2. **帧尾未终结的 `§` 不是标记**：它可能是下一帧 `§B` 的行首字符。这条同时保证「不逐帧
   发送」与四个行内反例仍是旧结果。
3. **`_seg_text` 只留未闭合碎片**：从最后一个已终结的整行 `§` 起（它既闭合上一份正文，也
   可能开启下一份）。完整待发正文不再二次缓冲。
4. **interim 只做完整单位全文去重**：它拆出的单位与 delta 已入队的单位按全文匹配；碎片只在
   段边界按原输入行结构决定去留（`rebase_after_interim`）——interim 末尾的 `§` 只有右邻
   是行尾时才留给尾巴当开头（`§B`／`§ ` 绝不补换行造标记）；碎片是 interim 前缀时换成
   interim 自己的结构剩余，避免半份碎片和下一帧拼成原文没有的正文。
5. **终态 output**：只压掉**本段** delta/interim 已发出的同一份正文（同段文本的两种切帧），
   原因记 `terminal_same_segment`（不是 `segment_duplicate`，不触发贴表情观察口）；跨段同文
   照发、不折叠的既有规则不变。这是 delta 即时入队后必须的适配，否则普通回复会发两遍。
6. **发送单元 `kind`**：现有 dict 增加 `kind="text"`（解析器入队、`_text_unit`、终态决策三处
   同源）；没有工具显示/注册表/插件机制。

## 3. 真实解析器矩阵（原始输出：`EVIDENCE/structured_queue_matrix.txt`）

每例都是真 `RunsEventParser`、真 SSE 行、每帧后 drain；`C = §\nC\n§`。

| 帧序 | 发送 | 终态 | 说明 |
| --- | --- | --- | --- |
| delta `§\nA\n§\nB\n§` → interim `§\nA\n§` → tool → C | `[A,B]` | `[C]` | R1 复现；A 只一次 |
| 同上，delta/interim 带帧尾换行 | `[A,B]` | `[C]` | 换行有无不影响 |
| interim A → delta `§\nA\n§\nB\n§` → tool → C | `[A,B]` | `[C]` | 反序 |
| CRLF／纯 CR 的 R1 复现 | `[A,B]` | `[C]` | 与 `split_text_units` 同口径归一 |
| delta `§B\n§` → interim `§` → tool → C | `[]` | `[C]` | 行内 §，真缺开头仍拒绝 |
| delta `§ \nB\n§` → interim `§` → tool → C | `[]` | `[C]` | 带空格 § 同上 |
| delta `§\nA\n§B\n§` → interim A → tool → C | `[A]` | `[C]` | 行内 §B 不另成正文 |
| delta `§\nA\n§ \nB\n§` → interim A → tool → C | `[A]` | `[C]` | 带空格 § 同上 |
| delta `§\nA\n§\nB` → tool | `[A]` | — | B 缺闭合不发 |
| delta `§\n§\n` → tool | `[]` | — | 静默 |
| delta `§\n你好\n§` → completed 同文 | `[]` | `[你好]` | 帧尾未终结 → 终态发一次 |
| delta `§\n你好\n§\n` → completed 同文 | `[你好]` | `[]` | 已入队，终态去重 |
| delta `§\nA\n§\nB\n§\n` → tool(`submit_control`) | `[A,B]`，批号 0 | — | 控制只封段不切批 |

## 4. 真实测试输出

| 命令 | 结果 | 证据 |
| --- | --- | --- |
| 全量 `tests/`（`unittest discover`） | **697 OK (skipped=1)** | `EVIDENCE/full_tests_structured_queue.txt` |
| 插件 `queued-lane-interim-thin` | **48 OK** | `EVIDENCE/plugin_suites_structured_queue.txt` |
| 插件 `steer-presentation-thin` | **21 OK** | 同上 |
| 真 client/handler（`tests.test_queued_lane_interim_client`，10 tests） | **OK (skipped=1)** | `EVIDENCE/repo_client_regressions_structured_queue.txt` |
| 解析器矩阵（本节表格） | 见上 | `EVIDENCE/structured_queue_matrix.txt` |
| `verify.py` | **22/22 PASS**（清洁树） | `EVIDENCE/verify_structured_queue.txt` |

`tests/` 的 697 **不含**插件用例；完整口径是 `tests/ 697 OK (skipped=1)` + `queued-lane 48 OK` +
`steer-presentation 21 OK`，由 `verify.py` 的插件闸位单独把关（R2）。唯一 skip 仍是
`test_deployed_parser_still_seals_at_interim`：部署副本已是信封解析器，该「stock = 裸正文」
前提不成立，显式 skip（同 t_f9767f18 记录的环境变化，未改弱断言）。

## 5. 与 steer 候选的兼容

- 基线 6fbcceb（t_f9767f18）是本提交的祖先，未回退、未 reset；
- 本轮未改 `core/prompt_builder.py`、`core/steer_presentation.py`、网关 `steer-presentation-thin`；
- 插件闸位 `steer-presentation-thin 21 OK` 仍在 verify 内通过。

## 6. 明确没做的事（沿用已确认的非返工项）

- 不逐帧发送：delta 只驱动结构拆分，只有完整闭合（且闭合标记行已终结）的正文才入队；节奏
  仍由 `_RunsReplyQueue` 的 FIFO／批号／3 秒间隔控制。
- 终态与**上一段**同文照发的规则不变；只有**本段**已发出的同一份正文不在终态重发。
- queued-lane 薄插件自身的同文终态抑制保留；插件生产代码未改。
- 本轮未改提示词与部署文案（R3 已在上一轮更正：只上新提示词不会多发空消息；解析器与提示词
  必须同批部署/回滚、只上新解析器拒绝旧裸正文、在飞旧格式 run 零发送）。
- 没有兼容层/状态机/第二套队列；`core/runs_text.py` 的两个新函数都只是同一协议的函数式工具。

## 7. R2／R3（上一轮结论，保留）

- R2：插件测试不在 `tests/` 发现内，verify 用 Hermes venv、离开仓库根目录、以 48/21 下界
  单独设闸；`tests/` 数字不再冒充包含插件。插件夹具迁移到信封，断言未削；共享标记用例的
  观察口断言按本轮结构改成精确 `segment_duplicate(A)`，并保留「不得 missing_open」。
- R3：`DELIVERY_t_0ecd8f77.md` 的文案已按实测更正（f5bbec8 把 `§` 当分隔符：单条
  `['正文']`、静默 `[]`、多条 `['A','B']`、未闭合 `['A']`），同批部署/回滚等硬约束保留。

## 8. 回滚

回滚本提交即恢复 6fbcceb 行为；没有部署动作、运行时状态或新配置项。`take_closed_units`／
`rebase_after_interim` 只被 `RunsEventParser` 调用，`TEXT_UNIT_KIND` 只是 dict 字段值。
生产更新仍需解析器与提示词同批（R3）。
