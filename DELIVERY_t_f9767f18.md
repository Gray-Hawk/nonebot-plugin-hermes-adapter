# DELIVERY t_f9767f18 — steer 呈递区域化收口 + 去掉上游 OUT-OF-BAND 外壳（候选）

状态：**候选已提交，未部署、未重启、未上传。** 基线 `b3b1fac`（= t_0ecd8f77 的信封候选），
本次提交 `25d5ef1` + 证据提交（本文件）。

## 1. 问题（截图 m:68490）与本次逐条闭环

| 生产现象 | 根因（施工前实测） | 本次处置 |
| --- | --- | --- |
| 转录里 steer 带完整 `[OUT-OF-BAND USER MESSAGE — …]` 外壳 | 上游 `agent.prompt_builder.steer_user_row → format_steer_marker` 在**投递时**加壳 | 网关薄插件对 NB 结构化声明过的这一批跳过套壳（三处包装，上游一行未改） |
| 没有 `<聊天消息区域起始>`／`<聊天消息区域结束>` | NB `render_steer_rows` 只拼裸消息行 | NB 整批改走聊天区唯一构造点 `render_chat_region`（与普通消息同一外观） |
| 正文与末尾系统提示之间只换行 | `_with_steer_reply_convention` 用 `f"{text}\n{约定行}"` | 约定行改挂区域块之后、空行分隔（`\n\n`），落在闭标签之外 |

## 2. 设计决策（含"先评估"结论）

1. **复用 prompt_builder 的区域块构造（约束 1）**：抽出 `render_chat_region(lines)` 作为
   唯一构造点，`_render_chat_region`（reactive 聊天区）改为调用它；新增
   `render_steer_batch(rows)` = `render_chat_region([...rendered_line])`。全仓 grep 确认
   没有第二处手写区域标签（`steer_region_decorations()` 也只从这里取字面量）。
2. **请求体结构化标记，不做文本前缀猜测（约束 2）**：NB 在
   `POST /v1/runs/{id}/steer` body 加 `presentation = {"schema":
   "hermes.steer.presentation/1", "format": "chat_region"}`（`core/steer_presentation.py`，
   与插件侧双向字面量比对测试）。
3. **"上游请求体会丢额外字段"—已核实，窄补丁点结论**：`api_server_runs._handle_steer_run`
   只读 `input`/`message`/`text`（第 1457 行），归一后 `agent.steer(text)`；`presentation`
   字段确实被丢弃。因此窄补丁点定为：
   * 包装 `_handle_steer_run`：先 `await request.read()`（aiohttp 缓存字节，下游
     `request.json()` 仍读同一份）取 body，命中标记就把**上游将要投递的那份归一文本**
     （同一 or 链 + `_normalize_chat_content(...).strip()`）登记进本进程 `PresentedBatches`；
     上游非 200（409/400）时撤销登记；
   * 包装 `steer_user_row`：文本命中登记表 → 原样落行；未命中 → 上游原样套壳。
     `agent.agent_runtime_helpers` 在模块顶层按名导入过该函数，两处名字绑定一起换指。
   判据只有「精确相等」与「由若干登记批按上游 `"\n"` 拼接而成」两种，代码里没有任何对
   `[OUT-OF-BAND` 形状的识别 —— 正文里出现同样字样不会被误剥（有测试与负对照）。
4. **薄插件边界（约束 3）**：只做本次 Runs steer 呈递；不碰 `_pending_steer` 队列、
   `steer()`、drain、投递时机，不建表/不落盘、无恢复与身份逻辑。登记表进程内、上限 32 批、
   一次投递即消费。
5. **提示面同一处收口（必须做，否则功能回退）**：系统提示 `STEER_CHANNEL_NOTE` 只说
   「只信这个标记」。区域块成为本部署 steer 外观后，模型面对"没有标记的中途 user 行"
   会按注入拒收（上游 #40240 的历史故障）。插件**只在原 note 之后追加一段**说明区域块
   外观同为可信真实用户消息；原标记语义一字不改，CLI/桌面/Kanban 注入通道不受影响。
   回滚 = 卸载插件，追加段随之消失（不写死在 NB 侧提示词里）。
6. **与 t_0ecd8f77 信封提示词兼容（约束 5）**：本批在其后提交，未触碰
   `core/runs_text.py`、`RunsEventParser` 段边界/批号/3 秒节奏/终态口径、`prompt_protocol`
   与 `prompt_builder` 的分节符格式段。`verify.py` 的信封语义/提示词口径 7 项全 PASS。

## 3. 改动清单

生产逻辑（NB）：
* `core/prompt_builder.py`：+`render_chat_region`、`render_steer_batch`、
  `steer_region_decorations`；`_render_chat_region` 改调用同一构造点；`render_steer_rows`
  的 `text` 改为区域块（`SteerPresentation.rows` 与逐行 `rendered_line` 语义不变）。
* `handlers/message_relay.py`：发送文本 = `render_steer_batch(send_rows)`；约定行空行分隔；
  `steer_run(..., presented=True)`。
* `core/hermes_client.py`：`steer_run(presented=False)`，仅该请求体加
  `presentation`；默认请求体与上游原样一致（有测试）。
* `core/steer_presentation.py`（新）：契约常量 + `declares_chat_region` 判据。
* `handlers/message_turns.py`：`_match_pending_steer_rows(decorations=…)`——native 回收
  逐行还原时，区域开标签行/固定说明行/闭标签只在**当前位置没有任何账项命中**时跳过
  （与既有约定行同一位置、同一原则）；旧裸正文 pending_steer 仍兼容。

网关薄插件（新目录 `gateway_plugins/steer-presentation-thin/`）：`plugin.yaml`、
`__init__.py`（register + api_server connect 复验）、`patches.py`（三处包装 + `PresentedBatches`）、
`README.md`、`tests/test_plugin.py`。

测试与脚本：
* 新增 `tests/test_steer_region_presentation.py`（25 例）、`tests/_regions.py`（区域块断言助手）；
* 既有 `test_chat_line_unified` / `test_steer_presentation` / `test_steer_identity_at` /
  `test_steer_reply_convention` / `test_steer_overlap` 的判据改为「区域内消息行逐字节一致
  + 框架断言」，所有 `steer_run` 替身改为断言 `presented=True`；
* `scripts/smoke_chat_identity_at.py`、`scripts/sample_region_presentation.py`（新增样例 6：
  区域块 / 最终发送文本 / 未装插件时的负对照）。

## 4. 真实验证输出（原始输出见 EVIDENCE/）

| 项目 | 命令 | 结果 | 证据文件 |
| --- | --- | --- | --- |
| NB 新模块 | `python -m unittest -v tests.test_steer_region_presentation` | **25/25 OK** | `EVIDENCE/steer_region_presentation_nb.txt` |
| 相关既有模块 | 7 个 steer/聊天行模块 | **128/128 OK** | `EVIDENCE/steer_region_related.txt` |
| 网关薄插件 | hermes venv `unittest discover …/steer-presentation-thin/tests` | **20/20 OK** | `EVIDENCE/steer_presentation_thin.txt` |
| 全量 | `unittest discover -s tests` | **673 OK（1 skip）** | `EVIDENCE/full_tests_steer_region.txt` |
| 交付核对 | `verify.py` | **21/21 PASS** | `EVIDENCE/verify_steer_region.txt` |
| 渲染样例 | `scripts/sample_region_presentation.py` | 区域块/最终文本/负对照 | `EVIDENCE/steer_region_samples.txt` |

样例 6 的真实输出（最终发送文本）：

```
<聊天消息区域起始>
（聊天消息区域仅含尚未推送过的新消息，更早的对话见本会话历史记录）
[m:8001] [主人] [id=10001 user=主人]:
插一句

[m:8002] [普通用户] [id=10003 user=路人]:
我也说 [图 m:8002#0]（随条媒体未内联，需要看图/表情时用 get_message_images 按 m:8002 加对应 idx 取）
<聊天消息区域结束>

（系统提示：本条为对话中途插入的消息。若你之后回应它，当下直接输出正文即可，无需调用任何工具。）
```

网关侧证据（`test_plugin.py` 实际跑的是上游真代码）：真 `_handle_steer_run` 包装 + 真
`InterruptControlMixin.steer()` / `_drain_pending_steer()` +
真 `apply_pending_steer_to_tool_results` 投递路径；负对照「未声明文本逐字节等于上游
`format_steer_marker` 结果」；真 `gateway.platforms.api_server_runs._handle_steer_run`
安装后签名与 `_run_route_delegate` 调用点一致。

## 5. 覆盖矩阵（约束 4）

| 用例面 | 覆盖位置 |
| --- | --- |
| direct / followup | `DirectEntryTests`、`FollowupEntryTests`（真 `_maybe_steer_direct` / `_try_steer_followup`） |
| 单条 / 多条 | `RegionShapeTests`、`DirectEntryTests.test_multi_row_*`、`FollowupEntryTests.test_followup_multi_row` |
| 空行 | 消息间空行；约定行前一空行；`render_steer_batch(())==""` |
| 区域标记 | 开/固定说明/闭三行逐字节；`region_rows()` 严格解析；群/私两条链路 |
| 原文含 OUT-OF-BAND 字样不得误删 | NB 侧 `test_out_of_band_literal_in_body_survives_the_round_trip`、网关侧 `test_body_containing_the_shell_literal_is_not_stripped`、`test_unknown_text_is_not_consumed` |
| 媒体占位 | `[图 m:101#0]` / `[引用图 m:101#1]` + `get_message_images` 提示仍在行内 |
| m:id 回查与最终转录 | 逐行 `rendered_line` 仍是实发文本连续子串；台账逐行 `steer_text` 对齐；`/steer` 请求体含标记 |
| native 回收逐行还原 | `PendingSteerRestoreTests`：区域块单批 / 上游拼接双批 / 旧裸正文兼容 / 正文引用闭标签不被吞 |
| 请求契约双边一致 | NB `ContractTests`（含直接加载插件模块比对）+ 插件 `SchemaParityTests` |

## 6. 部署边界（本次未做）

* **不部署、不重启、不上传。** 插件目录未安装到任何 `$HERMES_HOME/plugins/`；
  `scripts/install_gateway_plugin.py steer-presentation-thin --check` 目前应为 NOT in sync。
* 上线顺序建议（两者互不为前提，但外观只在两件都到位时达成）：
  1. 装网关插件并重启该实例 gateway（`install_gateway_plugin.py steer-presentation-thin`）；
  2. 同批部署 NB 侧（区域块 + `presentation` 字段）。
  * 只装插件不升级 NB：没有标记 → 全部走上游原样，零变化。
  * 只升级 NB 不装插件：文本已是区域块但被外壳包住（模型看到「外壳里套区域块」），
    内容不丢、无回归风险。
* 回滚：删除 `$HERMES_HOME/plugins/steer-presentation-thin/` 并重启（提示词追加段随之消失）；
  NB 侧可单独回滚到旧呈递（裸消息行），插件对无标记请求一律不介入。

## 7. 已知限制 / 需要主人知情的差异

1. **提示词追加段是全局的**（该 gateway 内所有会话的系统提示都会多一段区域块说明）。
   它只描述"区域块外观的 steer 同样可信、且只在紧跟最新工具结果的那条 user 行里"，不改变
   其它通道的标记语义。
2. **压缩口径微变**：steer 行不再以 `[OUT-OF-BAND` 前缀被归类为"合成脚手架"，上下文压缩的
   verbatim 用户消息段会把它当真实用户消息收录（内容本就该保留），摘要预算占用略增。
3. **一次 drain 投递多批**时上游用 `"\n"` 拼接，两段区域块之间是单换行（每批内部仍空行分隔）；
   组成关系有覆盖，但外观不如单批整齐。
4. **登记表跨 agent 共享（进程内、有界 32）**：理论上"同一段文本"被另一 run 先声明且未投递时，
   后到那次会被当作已声明。文本含真实 `m:id`/身份栏，实际不可碰撞。
5. **测试环境变化（非本次引入）**：`t_0ecd8f77` 完成后，NeonBot site-packages 的
   `core/runs_text.py` 于 14:34 被更新为信封解析器（`EVIDENCE/pre_existing_failure.txt`），
   使 `test_deployed_parser_still_seals_at_interim`（其前提是"部署副本 = 裸正文 stock"）
   由通过变红。本次没有改弱断言：该用例在前提不成立时**显式 skip 并说明原因**，全量因此是
   673 OK（1 skip）。若主人不希望动这条用例，可回退该单文件改动（不影响本任务其它提交）。
