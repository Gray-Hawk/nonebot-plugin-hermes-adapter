# 交付：小更新三件（Runs not_final / 视频 m:id / 出站戳一戳）

基线：`f4b3224`（dev 新基线）。只本地提交，**未部署、未重启、未推送、未合入 ACP/agy 代码**。

## 返修轮（首轮审查 R1/R2/R3，均 P2）

首轮候选 `835b08a` 被 reviewer_gpt 退回三条 P2（评论 301），本轮只做局部修复：

| 提交 | 内容 |
|------|------|
| `81bbaba` | fix(media): 视频 ref 逐段保位并共用文件总预算（R1+R2） |
| `565359f` | fix(outbound): 自戳判定按数字身份，前导零目标不再绕过（R3） |
| 本文件 | test(evidence): 返修轮 RED/GREEN 原始输出与说明 |

- **R1 逐段保位**：`_node_video_entries` → `_node_video_segments`，每个原 video 段给一个候选
  （无可下载 url → `None`）。块内标签严格按段位置生成，只有**登记成功**的段递增文件 idx；
  `[视频][视频 m:12#f0]`（无 url 段在前）与反向、交错三种排布都有用例，并核验块内 `#f0`
  指向的正是入库行 `#f0` 的同一段 url（真实 `handle_perception` + 主路径 `_extract_forward_full`）。
- **R2 共用总预算**：新增 `_file_registration_cap` / `_remaining_file_slots`；文件先占
  `max_files_per_message`，普通视频只用剩余额度，转发视频再扣一次（`_extract_forward_full`
  新增 `file_slots`，进缓存键）。占位改按「确实登记成功」判定（`_collect_video_placeholders`），
  不再按「有 url」抑制：cap=1 时 File+Video → 入库 1 行、content `[文件 m:1#f0:a.pdf] [视频]`；
  两个普通视频 → 入库 1 行、`[视频 m:1#f0] [视频]`；转发两个视频 → 入库 1 行、尾部只有 `#f0`。
  未登记视频一律留普通 `[视频]`，不产生指向未入库项的 ref；无 url 视频单条消息仍正常入库。
- **R3 数字身份自戳**：`_is_self_poke_target` 用 `int` 比较，`[CQ:poke,id=000999]` 与
  `self_id=999` 视为同一目标 → 保持原样文本；他人前导零目标与 @ 行为不变。
- 未扩框架、未改已通过的 Runs `not_final` 链；`git diff --check` 通过，树干净。
- 返修轮 numstat（`835b08a..565359f`，6 文件 +330/−45）：`core/outbound.py` +11/−1、
  `handlers/message.py` +2/−0、`handlers/message_media.py` +88/−34、
  `handlers/message_perception.py` +19/−8、`tests/test_outbound_poke.py` +13/−0、
  `tests/test_video_file_ref.py` +197/−2。
- 验收（代码提交 `565359f`；`verify.py` 在代码提交与证据提交上各跑一次，均 ALL PASS 22/22）：
  定向 42 OK（video 22 / poke 9 / runs 11）、全量 **786 OK (skipped=1)**、
  turn **6 OK**、`verify.py` **ALL PASS 22/22**；reviewer 探针（attachment 59，未改一行）
  `FINDINGS []`，三条缺陷全部复现为期望形态，`automatic_downloads == 0`。
  证据：`EVIDENCE/three_small_updates_20261116_FIX_{RED,GREEN}.txt` + `..._FIX_NOTES.md`。

## 提交（首轮）

| # | 提交 | 内容 |
|---|------|------|
| 1 | `316ed04` | feat(runs): submit_control 恢复 not_final（仅 Runs，复用既有 relay 续跑链） |
| 2 | `a81832e` | feat(media): 视频段入站登记文件引用并按 `m:<mid>#f<idx>` 出可寻址占位 |
| 3 | `3ccaa37` | feat(outbound): 正文内联 `[CQ:poke,type=1,id=<QQ>]` 转 OneBot poke 段 |
| 4 | 本文件 | test(evidence): RED/GREEN 原始输出 + 交付说明 |

## 验收（干净树，生产 venv `/home/neonsky/NeonBot/.venv/bin/python -B`）

- 定向：`tests.test_runs_not_final` 11 OK；`tests.test_video_file_ref` 11 OK；`tests.test_outbound_poke` 7 OK
- 全量：`unittest discover -s tests -t .` → **Ran 773 tests, OK (skipped=1)**
- turn 门禁：`unittest discover -s turn_tests -p 'test_*.py'` → **Ran 6 tests, OK**
- `verify.py` → **ALL PASS: 22/22**（含干净树／无 pyc／网关插件独立测试 48+21）
- RED：基线 `f4b3224` + 仅拷入三个新测试 = 5 fail（not_final）／5 fail+5 error（video）／3 fail+1 error（poke），原文见 `EVIDENCE/three_small_updates_20261116_RED.txt`
- GREEN 原文见 `EVIDENCE/three_small_updates_20261116_GREEN.txt`

## diff numstat（f4b3224..3ccaa37，18 文件 +1049/−106）

```
13	3	core/control_intent.py
107	37	core/outbound.py
3	2	core/prompt_builder.py
2	0	core/prompt_protocol.py
18	3	handlers/message.py
172	51	handlers/message_media.py
7	2	handlers/message_perception.py
10	1	mcp/server.py
14	0	mcp/tools/control.py
1	1	mcp/tools/push_message.py
6	2	skill/SKILL.md
4	1	tests/test_completions_regression.py
136	0	tests/test_outbound_poke.py
3	1	tests/test_prompt_final.py
1	1	tests/test_prompt_round2.py
240	0	tests/test_runs_not_final.py
2	1	tests/test_sticker_feature_switch.py
310	0	tests/test_video_file_ref.py
```

生产代码新增约 340 行（含注释/文档串），测试 686 行；三项各自都是几十到百行级，没有新框架。

## 各项口径

### 1）Runs not_final 恢复

- `submit_control` 增加布尔 `not_final`；`core/control_intent.build_control` 只在 `true` 时登记，
  `bind_control` 把它复制到**这一份**正文的 `decision["not_final"]`，随后复用
  `decision.get("not_final")` → `_post_send_feedback` → `message_relay._fire_followup_turn`
  的既有续跑链（`_RELAY_CHAIN_MAX`、普通正文归零语义都不动）。没有第二套信封/状态机。
- 仅 Runs：`submit_control_impl` 在 `interface != "runs"` 且传了 `not_final` 时直接 `ok=False`，
  completions 仍只走 `request_continuation`。任务锚开关关闭时 `not_final` 作为非锚字段保留。
- 提示词：`prompt_protocol._PROTOCOL` 的 Runs 段（`%…%`）补一句字段语义；completions 段落不变。
- 覆盖：正文发送后续跑恰好一次、第二份正文不继承、链上限（2 次后 WARN 不再点火、普通正文重置），
  普通正文不受影响；测试走真实工具入口 + 真实 `_make_runs_live_decisions` 即时发送器 +
  真实 `_post_send_feedback`。

### 2）视频 m:id 接线

- `_extract_file_entries` 把**有可下载 url** 的 `alconna.Video` 段登记成 `message_files` 行
  （`kind="video"`，name 缺省 `video.mp4`），与 File 共用同一 `m:<mid>#f<idx>` 编号空间；
  无 url 的视频不登记。
- `_collect_file_placeholders` 按 kind 出 `[视频 m:<mid>#f<idx>]`；拿不到 `m:id` 时退回 `[视频]`
  （普通文件占位形态一字未改）。
- 合并转发：`_extract_forward_full` 收集节点内 video 引用行（返回三元组）。主路径知道本条 `m:id`
  时块内直接渲染可寻址占位；perception 落库前块内保留 `[视频]`，引用行随消息落库后由同一占位
  函数补齐（block 内/尾部标签与 `message_files.idx` 严格同序：文件→普通视频→转发视频）。
- 未新增视频专用下载工具、不自动下载、不自动抽帧；`get_message_files` 按 ref 直接下 url。
- 提示词补一条 `[视频 m:X#fN]` 走 `get_message_files` 的说明。

### 3）出站戳一戳

- 只认 `[CQ:poke,type=<数字>,id=<数字>]` 与省略 type 的 `[CQ:poke,id=<数字>]`（type 缺省 1，
  id/type 只接受 ASCII 数字，参数顺序固定）；转成原生 OneBot poke 段
  （`alconna.Other(MessageSegment("poke", {...}))`）走同一条出站，标记不留进正文。
- 非法参数（非数字/缺 id/顺序不符/`type=x`）与原样文本一致（现有 inline 解析失败语义）；
  `id=0` 与自戳（`id == bot.self_id`）同样保留原样文本；未做真实自戳验收。
- 顺序：文本/@/poke 按原位置保持；长文合并转发分支同样带 poke 段（不占字符预算）。
- @ 回归不变；入站 `notices.py` 未改。

## 已知边界

- 视频引用只在拿得到 http(s) url 时登记；Telegram 等只给 file_id 的 adapter 仍按 `[视频]` 兜底
  （与文件路径一致，刷新链路只认 `file` 段，本次不扩）。
- 合并转发块内占位只有在主路径（已知 `m:id`）时才是 `[视频 m:…]`；历史行读到的块内仍是 `[视频]`，
  但同条消息 content 尾部有可寻址占位（与既有转发图 `[转发图#k]` 的呈现差异同源）。
- `verify.py` 当前为 22 项（技能文档里写的 25 项是旧口径），本轮结论按实际 22/22。
