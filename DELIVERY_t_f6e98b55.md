# 交付：表情引用、独立 § 信封、群聊 poke（t_f6e98b55）

基线 `21a0954`（dev），分支 `fix/next-bug-batch-20261116`，三项独立提交，未部署／未重启／未推送。

| 提交 | 主题 |
| --- | --- |
| `6a0798e` | fix(runs): § 信封取消共享分界，每份正文独立开闭 |
| `e153517` | fix(media): 引用表情接入引用呈递与入库 |
| `53ffc36` | fix(outbound): 群聊戳一戳改走 send_poke 公开 API |

## 1）纯表情引用接线（`e153517`）

- `_RepliedMedia` 补 `sticker_urls`，`_extract_replied_media` 补 `_extract_sticker_urls`；
  纯表情引用不再在 `_compose_quote_prefix` 返回空前缀。
- 呈递 `(引用表情#k)`，号仍 = 本条 `message_images` 全局 idx；枚举序
  own → sticker → quote → quote_sticker → forward（`iter_message_media` 唯一真源）。
- 入库 `kind='quote_sticker'`：`BufferedMessage.quote_sticker_urls`、
  `MessageStore.get_recent` 独立桶、`MessageBuffer.get_media_urls` 桶、perception 与
  主 handler 同源填、`forward_base` 后移。只注册发号不内联，取字节走 `get_message_images`。
- 覆盖：纯表情、文本+表情、普通图片、混合图片+表情（base 偏移）、历史入库 round-trip、
  fetcher start_index、主路径与 perception 同一枚举序（`tests/test_quote_sticker.py`，11 项）。

## 2）§ 协议根治：取消共享分界（`6a0798e`）

- 单条 `§\nA\n§`；两条 `§\nA\n§\n§\nB\n§`。整行 `§` 按「开/闭」两两配对，闭合标记随
  当前正文消费；`take_closed_units` 的 remainder 从已消费闭合之后起算，**不再留共享尾巴**。
- 旧共享形态 `§\nA\n§\nB\n§` → 只出 A，B 缺开头丢弃 + `reason=missing_open` 可见日志；
  不留兼容分支（负对照：`test_legacy_shared_boundary_is_no_longer_two_units`、
  `test_legacy_shared_form_yields_one_unit_and_a_missing_open_tail`、
  `test_legacy_shared_output_is_not_split_in_two_on_the_terminal_path`）。
- 删除：`_marker_tail`、`_terminate_marker_line`、`_ends_with_marker`、
  `_covers_a_closed_unit`、`rebase_after_interim` 整条路线（含 hermes_client 调用点）。
  替代为同一协议的纯函数：`unsealed_tail` + `pending_after_interim`（按独立信封重新推导，
  不是第二套状态机）：碎片比 interim 长 → 保「interim 未消费尾巴 + 多出字节」；碎片被
  interim 覆盖 → 用 interim 尾巴；interim 已闭合且碎片是最后一份已交付正文的旧切帧 →
  丢弃；其余分叉碎片原样留着按缺开头/缺末尾拒绝。CRLF 原样切片、段内去重、队列、
  台账、恢复/对账职责全部不动。
- 事故 run_a355…（`IndependentEnvelopeIncidentTests`）：commentary A 已发、final 发 B →
  只出 `[A, B]`，绝不生成 `A\n§§\nB` 复合单元，B 不重发；只出现既有的
  `segment_duplicate`（interim 与 delta 同文）与 `terminal_same_segment`（终态同文）。
  另覆盖单条、多条、静默、连续相同正文（段内去重不变 / 跨段照发）、delta 每个切点、
  interim 两态、工具前后、终态与恢复/对账一致（`decisions_from_text`、
  `units_from_text`、`runs_terminal.units_from_output` 同源）。
- 提示词同步（`core/prompt_protocol.py`、`core/prompt_builder.py`、`skill/SKILL.md`）：
  多条示例改成独立闭合；「上一份的结尾 § 不兼作下一份的开头」「每份都要自己写开头和
  结尾」；正文内独立 § 的说明改成「会提前闭合、后面发不出去」。
- fixture 按协议**故意变更**同步（不是回归）：`tests/test_runs_envelope.py`、
  `test_runs_stream.py`、`test_runs_terminal.py`、`test_completions_regression.py`、
  `test_control_regressions.py`、`test_control_mcp.py`、`test_queued_lane_interim_client.py`、
  `gateway_plugins/queued-lane-interim-thin/tests/test_plugin.py`（共享标记批量正文 → 独立
  信封；围栏内 § → 闭合当前正文，剩下的缺开头丢弃）、`verify.py`（两条独立信封 + 旧形态
  只出第一份 + 提示词同口径）。

## 3）群聊 poke（`53ffc36`）

- 出站不再拼原生 poke 消息段（NapCat 群聊 retcode=1400）：`_send_poke` 走公开 API
  `send_poke(group_id, user_id)`（群聊）/ `send_poke(user_id)`（私聊）；`type` 不进 API。
- 含 poke 的正文按原序拆成动作位（`_message_steps`）：文本/@ 分批、poke 各占一步，
  「消息 / 戳 / 消息」顺序不变；@ 与媒体落在对应消息位上。
- `_send_steps`：整批只 claim 一次交付组件，回执 id 汇总一次 `mark_component_sent`；
  中途失败整体 `mark_component_unknown`（部分失败不记成功）；已 sent 组件直接返回既有
  回执、不重发。
- 长文合并转发：poke 不进节点（`_pack_forward_nodes` 跳过），整包之后按原序补发。
- 非法参数 / `id=0` / 数字身份自戳（含前导零）仍按原样文本；@ 回归不变。
  `tests/test_outbound_poke.py`（13 项）。

## 验证（原始输出见 `EVIDENCE/`）

- 定向：`Ran 190 tests ... OK (skipped=1)` → `EVIDENCE/r_final_targeted.txt`
- 全量：`Ran 812 tests ... OK (skipped=1)` → `EVIDENCE/r_final_full.txt`
- turn 门禁：`Ran 6 tests ... OK` → `EVIDENCE/r_final_turn.txt`
- 网关插件独立：`queued-lane-interim-thin Ran 48 tests ... OK` →
  `EVIDENCE/r_final_gateway_plugin.txt`
- `verify.py`：`ALL PASS 22/22`（干净树）→ `EVIDENCE/r_final_verify.txt`
- RED：信封协议 `EVIDENCE/r2_envelope_RED.txt`（基线解析器，14 failures + 1 error）；
  引用表情 + poke 用基线 `21a0954` 临时 worktree 只换测试文件复跑 →
  `EVIDENCE/r13_baseline_RED.txt`（1 failure + 27 errors）。GREEN：
  `EVIDENCE/r2_envelope_GREEN.txt`、`EVIDENCE/r3_poke_GREEN.txt`。

## diff numstat（21a0954..HEAD，代码）

```
9	9	core/hermes_client.py
21	7	core/message_buffer.py
236	22	core/outbound.py
2	2	core/prompt_builder.py
5	3	core/prompt_protocol.py
150	124	core/runs_text.py
4	0	core/storage/message_store.py
5	1	handlers/message.py
28	10	handlers/message_media.py
6	2	handlers/message_perception.py
1	0	handlers/message_turns.py
7	2	skill/SKILL.md
2	2	tests/test_completions_regression.py
1	1	tests/test_control_mcp.py
4	4	tests/test_control_regressions.py
165	91	tests/test_outbound_poke.py
11	11	tests/test_queued_lane_interim_client.py
178	0	tests/test_quote_sticker.py
183	32	tests/test_runs_envelope.py
29	29	tests/test_runs_stream.py
6	5	tests/test_runs_terminal.py
11	7	verify.py
9	9	gateway_plugins/queued-lane-interim-thin/tests/test_plugin.py
```

## 未做／留给复核

- 未部署、未重启、未推送（按卡面要求），线上解析器仍是旧语义：**提示词与解析器必须同批部署**，
  否则模型按独立信封输出、旧解析器只认共享形态。
- 审计提到的问号表情 hook 按主人口径本批不动。
- 群聊 poke 实测依据来自主人给的 `send_poke` retcode=0 结论；本卡没有再次打真实平台调用。
