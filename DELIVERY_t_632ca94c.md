# DELIVERY t_632ca94c — 922e45a 返修：interim 复合误发 / @ 简化 / poke 工具化 / 排版

状态：**候选已提交，未部署、未重启、未推送、未发真实 QQ。** 基线 `922e45a`（父卡复核候选），
分支 `fix/next-bug-batch-20261116`，提交见文末。

## 1. 施工范围（主人 2026-11-18 最终评论）

| 项 | 内容 | 处置 |
| --- | --- | --- |
| A | 独立 § 信封在 interim 之后复合误发 | 修根因：`runs_text.pending_after_interim` 按原行边界消费 interim 已确认的完整末份信封 |
| B | 正文内联 @ 全面简化 | 只认 `@QQ号`（ASCII 数字）；撤销 `[CQ:at,qq=…]` 识别，不留第二套协议 |
| C | poke 全面简化 | 迁到 MCP 工具 `poke_user` 直调 OneBot `send_poke`；删除 `_InlinePoke`/`_message_steps`/`_send_with_pokes`/`_send_steps`/`_send_msg` 混合正文动作路径 |
| D | 文档与示例同步 | skill/SKILL.md、MCP 工具说明、决策协议与提示词、测试注释 |
| E | 同卡排版三项 | 普通单条空行、连续 steer 呈递重排、补投转录分层 |

## 2. A：interim 之后第二份信封闭合在 EOF 的复合误发

复现帧（ reviewer #311，真 `RunsEventParser` / 真队列 / 真客户端链路）：
`delta=§\n甲\n§\n§\n乙\n§` → `interim=同文` → `delta=§\n丙\n§\n` → `completed=§\n丙\n§\n`。
修前发送出口拿到 `[甲, 乙, "乙\n§§\n丙", 丙]`；修后严格 `[甲, 乙, 丙]`。

根因：`delta` 拆单位时帧尾未换行终结的 `§` 不算标记，`_seg_text` 留下 `§\n乙\n§`；interim 交付
`乙` 时把闭合行一起消费掉了，但 `pending_after_interim` 没有把碎片里「已由 interim 确认的完整
末份信封」消费掉，下一帧的 `§\n丙\n§` 接上去就把两份信封粘成 `乙\n§§\n丙`。

最小修复（`core/runs_text.py`）：
* 新增 `_consume_confirmed_envelopes()`：只剥「独立整行 `§` 开、独立整行 `§` 闭」的完整信封，
  且它必须逐字等于 interim 的结尾；半个标记/半个正文（更早切帧）一律不动，下一帧还能补全。
  剥完后剩下的碎片原样保留（真正的下一份信封），一个字符都不补。
* `split_text_units()`：闭合行之后的游离正文补 `reason=missing_open`（以前只静默丢弃；
  已有拒发行为不变）。

## 3. B：@ 简化

`core/outbound.py`：`_AT_MENTION_RE = (?<![0-9A-Za-z_])@([1-9][0-9]{4,11})(?![0-9A-Za-z_])`
* 文首/文中/文末、多个 `@` 都在原位置转成 At 段，顺序与前后文字不变；
* 昵称（`@小光`）、`@all`、`@全体成员`、`@0`、过短/过长数字串、邮箱样式（`a@10001.example`）
  一律不转换，原样当普通文本；
* `[CQ:at,qq=…]` 与 `[CQ:poke,…]` 不再是受控标记：留在正文里按普通文本发送（残留 CQ 形态
  仍有节流观测日志）。

## 4. C：poke 工具化

* 新工具 `mcp/tools/poke_user.py` + `mcp/server.py` 注册 `poke_user(adapter, user_id, group_id?)`：
  - `user_id` 是被戳的 QQ；给 `group_id` 走群聊 `send_poke(group_id, user_id)`，省略走私聊
    `send_poke(user_id)`；
  - 范围与目标判定复用 `validate_push_context`（群：活跃 reactive 会话 + 路由；私聊：主动下行
    白名单 + 调用方身份可识别），与 `push_message`/`recall_message` 同一套；
  - 自戳拒绝（数字身份比较，`000999` 与 `999` 同一个人）；结果带平台 `retcode`，平台拒绝
    记 `platform_rejected`、网络不明记 `unconfirmed`，都不自动重试。
* `core/outbound.py` 删除 `_InlinePoke`、`_message_steps`、`_send_steps`、`_send_with_pokes`、
  `_send_msg`、`_send_poke`、`_poke_supported`、onebotv11 adapter 探测 import；`send_text_with_media`
  回到单条消息出口。
* 原复核项 2/3（部分成功 repair 整批重放、转发失败不回退）随混合路径删除而消失；原项 4
  （poke+图片丢引用）属非法组合，不做功能。

## 5. D：文档与示例

* `skill/SKILL.md`：Modes 改成「每条消息自带开闭 §」，单条示例是真实多行 § 信封，静默写成
  两行 §；@ 教 `@12345`；新增 `poke_user` 工具说明；删除 CQ:at / CQ:poke 教学。
* `core/prompt_builder.py`、`core/prompt_protocol.py`、`core/decision_contract.py`、`mcp/server.py`、
  `mcp/tools/push_message.py`：模型可见文案与工具说明同步为 `@QQ号` + `poke_user`。

## 6. E：排版三项

1. **普通单条**：`render_chat_region` 的区域框架（开标签、固定说明、消息行、闭标签）之间一律
   空一行；`steer_region_decorations` 同步；表情包提示改空行分隔。区域起止结构与 m:id 文法不变。
2. **连续 steer**：发送侧不变（缓冲/批量出队/单批文本原样），网关薄插件呈递层新增
   `compose_batches()`：一次 drain 的多批由 `"\n"` 拼接时重排为「批间空行 + 每批自己的区域块 +
   批尾系统提示整段最后一条」。提示段由 NB 在呈递标记里显式声明（`presentation.hint`），
   插件不猜文案；没有声明就只加批间空行。单批投递逐字节等于登记文本。
3. **补投转录**：`steer_reconcile._build_case_a_text` 分层为「系统核对抬头 → 当前需要补投的插话
   （★ 1./2.…）→ 历史定位证据（触发消息/中途回应/中途推送/已送达插话/最终回复逐项标注
   '不是新指令'）→ 处理要求」，段落之间空行。取值与判定未变。

## 7. 真实验证

| 项目 | 命令 | 结果 |
| --- | --- | --- |
| 完整 unittest | `python -B -m unittest discover -s tests -t .` | **826 OK（1 skip）** |
| 定向集合 | 18 个相关模块（信封/流式/出站/steer/排版/工具注册） | **376 OK（1 skip）** |
| turn 门禁 | `python -B -m unittest discover -s turn_tests -p 'test_*.py'` | **6 OK** |
| 网关薄插件 | hermes venv `unittest discover …/steer-presentation-thin/tests` | **23 OK** |
| 验收 | `python -B verify.py` | **ALL PASS: 22/22**（提交后复跑，树干净） |
| 父卡原复现 | 真 `QueuedLaneInterimClientTest._drive` 回放 AB→interim→C(→completed) | `[甲,乙,丙]`；两封无尾换行帧 `[甲,乙,丁]` |

定向回归：`tests/test_runs_stream.py::InterimClosedEnvelopeAtEofTests`（真实帧）、
`tests/test_runs_envelope.py`（missing_open 警告）、`tests/test_outbound_mention.py`（@ 全矩阵 +
旧 CQ 不再识别）、`tests/test_poke_tool.py`（群/私参数、自戳、回执）、
`tests/test_poke_mcp_registration.py`（工具面）、`tests/test_layout_snapshots.py`（四个排版快照）。

## 8. 改动文件

生产：`core/runs_text.py`、`core/outbound.py`、`core/hermes_client.py`、`core/steer_presentation.py`、
`core/steer_reconcile.py`、`core/prompt_builder.py`、`core/prompt_protocol.py`、`core/decision_contract.py`、
`handlers/message_relay.py`、`mcp/server.py`、`mcp/tools/push_message.py`、`mcp/tools/poke_user.py`(新)、
`skill/SKILL.md`、`gateway_plugins/steer-presentation-thin/{patches.py,README.md}`。

测试：新增 `tests/test_outbound_mention.py`、`tests/test_poke_tool.py`、`tests/test_poke_mcp_registration.py`、
`tests/test_layout_snapshots.py`；删除 `tests/test_outbound_poke.py`（混合正文动作路径已移除）；
更新 `tests/_regions.py`、`tests/test_runs_stream.py`、`tests/test_runs_envelope.py`、
`tests/test_steer_region_presentation.py`、`tests/test_steer_presentation.py`、`tests/test_steer_overlap.py`、
`tests/test_steer_identity_at.py`、`tests/test_steer_pre_debounce.py`、`tests/test_steer_reply_convention.py`、
`tests/test_prompt_final.py`、`gateway_plugins/steer-presentation-thin/tests/test_plugin.py`。

## 9. 未做 / 边界

* 未部署、未重启 gateway、未推送远端、未发真实 QQ poke；插件目录未安装到任何 `$HERMES_HOME/plugins/`。
* 长群消息的合并转发路径本身未改（本次只在删除混合 poke 路径后回到单条出口）；原复核项 3 的
  截断回退分支随混合路径删除，不再单独存在。
* 附件路径与引用身份改造未在本卡范围内（planned-mods 条目 98 只取排版三项）。

## 10. 复核返修（run 304 → 本卡第二轮）

复核意见 1：`mcp/tools/poke_user.py` 丢弃了适配器已保留的拒绝码。

* `ActionFailed.info` 由 OneBot v11 适配器以 `ActionFailed(**线形回执)` 保留，拒绝时
  含真实数值 `retcode`；新增 `_rejected_retcode()` 只取这一个字段，`platform_rejected`
  结果如实带回，不暴露原始异常、不重试。
* 成功路径不伪造 `0`：适配器 `handle_api_result` 成功时只返回 `data`（`send_poke` 为
  `null`），因此删除原先对 `response["retcode"]` 的嗅探，成功结果 `retcode=null`。
* 测试改为真实 `Bot.call_api` → `Adapter._call_api`（只替换 WebSocket I/O：`_LineSocket`
  记录帧并把线形回执交回真实 `ResultStore`，`_StubDriver` 只提供配置与 shutdown 钩子）。
  成功线形 `{status:ok,retcode:0,data:null}` → `ok=True, retcode=None` 且帧参数为
  `{"group_id":54321,"user_id":10001}`；拒绝线形 `{status:failed,retcode:1400,...}` →
  `platform_rejected, retcode=1400`，只发一帧。原来用 fake bot 回 `{retcode:7}` 断言成功的
  证据已删除。

复核意见 2：`skill/SKILL.md` 同节两套可见性口径。

* 183 行无条件「tool call 以外写的文字都会即时发」改为「只有成对 `§` 内的正文才发，且要
  闭合后才发；裸正文与闭合后的残留一律丢弃」，与 Modes 的「不在自己那对 `§` 里就不发」
  统一。解析器未改。

验证：`tests/test_poke_tool.py` 13 OK（含两条线形回执用例）；完整 unittest 827 OK（1 skip）；
turn 门禁 6 OK；`verify.py` 22/22 ALL PASS（提交后复跑）。未部署、未重启、未推送、未发真实 QQ。
