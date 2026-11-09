# t_979bff13 交付说明：Runs 与 completions 正文信封退役

- 分支：`feat/plain-text-protocol`（起点 `a27ca76`，`2843c65` 保留在 `feat/control-mcp`，未 reset、未带入）
- 提交：`f3c7fbc`（主体）+ `8b27b0c`（续跑消费修正）+ `5210912`（终态不折叠 + 观察口）+ 本轮（Round 1 审查修复 + 卫生项，哈希见 `git log -1`）
- 范围：**只本地提交**。未部署、未重启、未上传。等 GPT 代码审查 + 糯米验收后才开部署卡。

## 1. 生产代码规模

```
core/continuation.py            |  63 +++       新
core/runs_text.py               |  89 +++       新
mcp/tools/continuation.py       |  86 +++       新
core/hermes_client.py           | 455 +-        改（删旁白回显通道 + 纯文本状态机）
handlers/message_turns.py       | 363 +-        改（删信封自愈/打回，逐份正文+控制 FIFO）
core/runs_terminal.py           |  93 +-         改（只走纯文本计划）
core/prompt_protocol.py         |  84 +-         改（双模式纯文本协议）
core/prompt_builder.py          |  40 +-         改（正文口径）
mcp/server.py                   |  35 +++        改（注册 request_continuation）
mcp/tools/control.py            |  19 +-         改（completions 也放行）
tasks/run_resume.py             |  11 +-         改（恢复链同一份切分）
core/control_intent.py          |   9 +-         改
handlers/message_feedback.py    |  29 +++        改（_fire_continuation）
core/npha_terminal.py           | 472 ---        删
scripts/replay_*.py (3)         | 299 ---        删
```

新增生产代码 238 行（89+63+86），落在设计给的 250～340 行区间内（含改写后的净增量）；
未引入新持久状态、未建第二套协议、未做兼容层。

## 2. 真实测试结果

证据文件：`EVIDENCE/plain_text_protocol_tests.txt`（含命令、原始输出、逐用例 `-v` 名单）。

| 运行 | 结果 |
| --- | --- |
| `python -m unittest discover -s tests -t .` | **Ran 451 tests / OK**（本轮修复后复跑） |
| `cd turn_tests && discover -s . -t .` | **Ran 6 tests / OK** |
| `python verify.py`（当前验收入口） | **19/19 PASS**（本轮重写后，证据见 `EVIDENCE/round2_review_fixes.txt`） |
| 关键用例 `-v`（纯文本解析/客户端链/续跑/ completions 终态） | **Ran 42 tests / OK** |
| 简化复核定向 `-v`（终态重复段/去重/跨段/失败重试/观察口） | **Ran 10 tests / OK** |

要求的覆盖面对应关系：

- 单句零工具零额外调用：`test_single_sentence_zero_extra_calls`（`seen == ["你好"]`，终态一份）。
- `§` 跨帧闭合与连发：`test_separator_split_across_delta_frames`、`test_three_units_split_by_standalone_separator`。
- ` §`/`§ `/` § ` 与代码围栏：`test_separator_with_spaces_does_not_split`、`test_spaced_separator_across_frames_does_not_split`；围栏不是例外——`test_fenced_separator_still_splits` 断言 ``` 内的整行 § 同样分隔；`"\\§"` 原样（不设转义协议）。
- 静默／进度／最终：`test_silent_output_sends_nothing`、`test_interim_progress_then_final`、`test_progress_then_final_sent_once_each`。
- 段内 interim/delta 同文去重、跨段同文两份：`test_interim_and_delta_same_text_sent_once_per_segment`、`test_interim_prefix_then_delta_adds_only_new_unit`、`test_same_text_across_segments_sends_twice`。
- 控制 FIFO 与作用域：`tests/test_control_mcp.py`（FIFO、scope、控制工具封段不 bump batch）。
- 未配对控制丢弃：`test_silent_turn_leaves_control_unconsumed` + `test_discard_drops_leftovers`。
- completions 纯文本 / push_message / request_continuation：`tests/test_completions_regression.py`、`tests/test_continuation.py`、`turn_tests` 的续跑消费用例。
- 流式／终态／恢复一致：`ResumeConsistencyTests`（恢复链与终态得到同一 `plan_json`）。
- 断流失败关闭：`test_stream_break_sends_no_half_content`（半截 delta 不发送，run 留给恢复链）。
- 旧 JSON 逐字一条、不识别控制：`test_legacy_json_sent_exactly_once_verbatim`。
- 超长 fail-closed：`test_overflow_terminal_fails_closed`（`text_body_overflow`，零发送）。
- 终态重复上一段照发：`test_terminal_repeating_last_segment_is_resent`（`seen == ["正文","正文"]`）；同形 parser 用例 `test_terminal_repeating_last_segment_reports_no_suppression` 断言不产生任何抑制事件。
- 抑制可观察：`test_segment_duplicate_suppression_is_observable`、`test_terminal_dropping_unsealed_delta_is_observable`。
- 命中统计（真实留存 SSE + 回归场景）：`SuppressionObservationTests`，原始输出见 `EVIDENCE/plain_text_protocol_tests.txt`，探针 `EVIDENCE/probes/suppression_measure.py`。

## 3. 与设计的差异清单

1. **`expect_structured` 没有整体删除**。设计写“改传 `expect_structured=False` 或短路到纯文本分支”，实现选后者：runs/completions 在 `hermes_client.chat` 内由
   `plugin_config.hermes_api_interface in ("runs","completions")` 显式走纯文本终态支路，`decode_terminal_batch` 与 `npha_terminal.py` 已删；形参保留是因为它还承载“本轮要决策”的路由语义与 live 投递开关。astrbot 后端的旧 JSON 决策协议未动（设计未要求迁移该后端）。
2. **删除范围比设计多**：除 `npha_terminal` 解码链与 3 个 replay 脚本外，一并删了 `InterimSegmenter`、`_INTERIM_MIN_CHARS`、`_stream_chat`、`InterimCallback` 与 `chat(interim_callback=...)` 形参——这条“旁白回显通道”在纯文本协议下已无调用点（两个 call site 都传 None），留着会形成第二套 interim 语义。
3. **`§` 转义规则按主人补充取消**：`runs_text.py` 只有 `line == "§"` 一条判定，`\§` 不做转义恢复，测试断言原样发送。本轮按主人 18:21 追加要求删除了代码围栏特判（围栏内的整行 § 同样分隔），解析器只剩这一条规则。
4. **续跑消费点**：设计只说“completions 用 request_continuation”。实现上 completions 的群 active 轮在 `_run_reactive_turn` 收尾消费、passive 轮在投递尾部消费；两处都先 `take()` 再 `_fire_continuation`，并在**轮首** `discard` 上一轮残留，避免陈旧标记凭空续跑。这是设计未展开的边界，已在 `8b27b0c` 补齐。
5. **终态重复段不过滤**：曾实现 `final_segment_pending` + `last_sealed`，在“最后一段无新文本且 output 重复上一段”时吞掉重复；按主人 18:05 审定已整体删除——终态 output 一律 `decisions_from_text` 后原样发送，段内 interim/delta 的完整单元全文匹配去重保留，跨段一律不按文本折叠（真实重复在用户看来就是重发，不靠猜测吞掉）。
6. **修了一处实现 bug**：`ContinuationQueue.discard` 原先 `bool(set.discard(...))` 恒为 False，已改为显式判断（测试 `test_discard_drops_unconsumed_request` 发现）。
7. **`verify.py` 已按本轮要求重写**：它原绑定 `BASELINE=498e7a6`（该 commit 在本仓库不存在）并断言已删除的 `npha_terminal` / `decode_terminal_batch` / `fold_stream_decisions` / `strip_complete_decisions` / `scan_stream_decisions` 与两个 replay 脚本。现在断言当前事实：唯一纯文本解析器、协议块无信封字段、控制/续跑各一条链路、关键验收用例存在、工作树干净；不再引用任何已删模块或基线提交。
8. **抑制可观察（窄接口）**：`RunsEventParser.on_suppressed(reason, text, source)` 默认 `None`，全仓生产代码不接线，不建库、不加日志、不加开关；只覆盖两处既有跳过：段内全文匹配去重（`segment_duplicate`）与终态丢弃未封口 delta（`unsealed_delta_tail`）。命中统计：
   - 真实留存样品（`SAMPLES/`，2026-11-09 k3.1-pro 旧 JSON 协议期抓包，2 条 / 69 帧）：正文段 3、发送单元 3、段内去重 0、终态尾丢弃 2。两条都命中尾丢弃（终态 output 覆盖已流式的同文 delta，属预期）；抓包无 `message.interim`，去重 0 是样本局限而非生产结论。
   - 回归场景（高保真 SSE）：interim+delta 同文 → 去重 1；未封口尾 → 丢弃 1；终态重复上一段 → 抑制 0、实际发送 2 份；A→工具→A → 2 段 2 份、抑制 0；单段多单元 → 去重 2、共 4 单元。
   - 样本量小且文本为旧协议产物，不能外推成生产概率；统计只用于确认抑制点可观察、命中位置与设计一致。
9. **失败重试用例随简化更新**：`_FailedSendRetry.test_failed_live_send_is_not_mistaken_for_success_at_terminal` 原断言依赖“终态重复上一段被吞”，现改为 `['甲','乙','甲']`（终态重复段照发 + 失败即时份仍重试），组件数 4→6；这是语义变更的必然结果，不是放宽断言。

## 4. 本轮（Round 1 审查修复 + 卫生项）

### 4.1 阻塞项 1：AstrBot 决策轮零发送（`_decision_body` 只读 body）

- 归一化点：`core/hermes_client.py::_normalize_astrbot_decision`，只在 AstrBot 的
  路径 B（`_try_parse_first_json_block` 之后）与本地拒答结果上调用：
  `reply_text → body`，`reply_to / should_exit_active / pin_anchor / release_anchor /
  answer_anchor / anchor_merge → _control`。Runs/completions 不进这条分支，
  没有加兼容分叉、没有改 `_decision_body` 的接口判定。
- 保留字段：`should_reply`、`not_final` 原样留在决策上（AstrBot 后端语义未迁移）。
- 测试：`tests/test_astrbot_text_compat.py`
  - `test_path_b_normalizes_reply_text`（真实 `HermesClient.chat`，只换 AstrBot 传输）
  - `test_astrbot_private_reply_text_delivered`（真实 passive harness，断言正文送达 + `resolve_reply` 收到 `m:5`）
  - `test_astrbot_group_reply_text_delivered`（真实 reactive harness，同上）
  - 另有归一化纯函数与「既有 body 优先 / false 不占控制」用例。

### 4.2 阻塞项 2：passive 零正文轮吞掉 request_continuation

- `handlers/message_turns.py` 两处静默早退（`empty_output` 1257 附近、`empty_content`
  1278 附近）在 `return` 前按 reactive 同口径 `take_continuation` + `_fire_continuation`。
- 测试：`tests/test_continuation.py::PassiveContinuationConsumptionTests`
  （零正文 → `silent/empty_output`、空 body → `silent/empty_content`，两者都真实
  触发续跑且队列清空）。

### 4.3 卫生项（Round 1 附列 6 条，全部收掉）

1. `tasks/run_resume.py`：删除 `feed()` 返回值的 interim echo 与 `last_seq` 推进
   （feed 现在恒返回 None）；恢复语义不变——订阅期间只读帧，正文只按终态
   `output` 经 `units_from_output` 补发一次，并由既有交付台账幂等兜底。
2. `interim_echo_enabled` 退役：`config.py`（`steer_reply_push_active` 去掉该依赖、
   runs 恒开启）、`core/misc_config.py`、`core/config_templates.py`（含 OLD_KEYS
   → `deleted`）、`misc_config.example.yaml`、`scripts/smoke_misc_media.py`、
   `scripts/smoke_config_templates.py` 的键数台账（54→53，misc 15→14）、
   `tests/test_steer_presentation.py`、`tests/test_runs_terminal.py`。
3. 代码围栏特判删除：`core/runs_text.py` 只剩「整行严格等于 §」一条规则，
   `test_fenced_separator_still_splits` 断言围栏内同样分隔。
4. `tests/test_control_mcp.py`：`test_empty_body_still_consumes_one_control` 改为
   `test_silent_round_does_not_consume_control`，真实断言零正文轮不消费控制、
   未配对控制留待轮末丢弃（旧断言实际测的是「下一份正文拿到控制」）。
5. 提示词残影：插话原则 `→ true/false` → `→ 输出正文／不输出正文`；
   `决定 正文 范围` 空格修正；Runs 头部不再提 `push_message`（模式行拆分）；
   Runs/ completions 块不再教 `should_exit_active=false` 输出字段；steer 约定行
   从「两行 JSON 写进 body」改为「直接输出正文」。
6. `verify.py` 重写为当前纯文本协议的验收入口（详见 §3 第 7 条）。

### 4.4 实现前提（写给下一位，别以为有显式护栏）

- 控制归属依赖 **`tool.started` 先于 MCP 工具执行**：`submit_control` 在工具
  执行时才登记，解析器在 `tool.started` 已封段并入队，因此控制前的正文拿不到
  控制之后才登记的控制。设计 v6 的 `_pre_control` 显式标记没有写，当前时序下
  行为等价；如果上游改成先执行工具后发帧，这条前提会失效。
- `on_suppressed` 仍然只供测试观察，生产零接线；主人提到的「接贴表情」尚待
  主会话确认触发范围，未擅自实现。

### 4.5 已知残留（本轮未改，均为设计选择或已声明的历史留痕）

- AstrBot 仍用旧 `submit_decision` JSON 提示词（该后端不在本次迁移范围）；
  归一化只保证投递语义与纯文本链一致。
- `handlers/message_feedback.py::_check_interim_feedback` 已无调用点（旁白通道
  退役的连带死函数）；本轮只清理配置开关，未删除该规则检测函数，避免扩大范围。
- `tasks/run_resume.py` 不再推进 `last_seq`：重启重放只多读帧、不重发正文，
  真正补发由交付台账幂等约束。

## 5. 已知边界

- 协议失败（单份 > 4000 字）整轮零发送，`parse_failed_reason=text_body_overflow`；不发半截、不猜补发。
- 旧 JSON 全文按普通正文一条原样发出，控制语义完全失效（`resolve_control` 只认 `submit_control` 登记值）。
- 无 20 份上限、无事后续跑合并、无 kill switch；回滚走版本。
- 未做真机/真 QQ 验证（本卡禁止部署）。
