# steer-presentation-thin（候选，未部署）

Runs 车道中途 steer 的呈递收口：**NB 渲染的零包装正文原样落成那条 user 行，不再套上游的
`[OUT-OF-BAND USER MESSAGE — …]` 外壳。** 发送者、m:id 与媒体身份继续由 NB 的
`SteerRowPresentation.source_row` 和既有 steer 台账保存，不写进模型可见文本。

## 上游加壳位置

| 位置（stock） | 行为 |
| --- | --- |
| `gateway/platforms/api_server_runs.py::_handle_steer_run` | `POST /v1/runs/{id}/steer` 只读 body 的 `input`／`message`／`text`，归一后 `agent.steer(text)`；多余字段在此丢弃 |
| `agent/agent_runtime_helpers.py::apply_pending_steer_to_tool_results` | 工具批次结束后 drain，`steer_user_row(text)` 追加独立 user 行 |
| `agent/turn_iteration_prep.py::_inject_steer_after_newest_tool_result` | 上一次 API 调用期间到达的 steer，使用同一行构造函数 |
| `agent/prompt_builder.py::steer_user_row → format_steer_marker` | 加壳点：`[OUT-OF-BAND USER MESSAGE — …]`＋正文＋`[/OUT-OF-BAND USER MESSAGE]` |

NB 侧在 `core/prompt_builder.render_steer_batch` 以单换行拼接各行正文，在
`handlers/message_relay._with_steer_reply_convention` 按需追加批次级系统提示。网关插件只负责
去掉上游外壳，不重建 NB 的消息身份或排版。

## 三处窄补丁（全部包装，不改上游一行）

1. **`api_server_runs._handle_steer_run` 前置读一次 body**：body 带 NB 的结构化呈递标记
   `{"presentation": {"schema": "hermes.steer.presentation/1", "format": "plain_body"}}`
   时，把上游将要投递的归一文本（同一 `input`／`message`／`text` or 链＋
   `_normalize_chat_content(...).strip()`）登记到本进程的 `PresentedBatches`。上游返回非
   200（409／400…）时撤销登记。aiohttp 的 `request.read()` 缓存字节，下游仍读同一请求体。
2. **`steer_user_row`**：文本命中登记表则落行（`{"role": "user", "content": 文本,
   "display_kind": "steer"}`）；未命中则走上游 `format_steer_marker` 原样。
   `agent.agent_runtime_helpers` 在模块顶层按名导入过 `steer_user_row`，所以两处名字绑定一起
   换指，而且只换仍指向原函数的那个。
   单批逐字节等于登记文本；一次 drain 的多批由上游用单个换行拼接，`compose_batches`
   保持批间单换行，并把标记中声明的相同 `hint` 收拢到整段末尾一条。各批提示不一致或尾部
   不吻合时保持原文，不猜文案。
3. **`agent.system_prompt.STEER_CHANNEL_NOTE`**：在其后追加一段说明，本部署里零包装 user 行
   也可以是可信的中途 steer。原标记说明一字不改，CLI／桌面／Kanban 工具注入仍走上游标记的
   通道语义不变。没有这段说明，模型可能把无标记的中途 user 行按注入拒收（上游 #40240 的
   历史故障）。

**不按文本前缀猜上游包裹。** 登记表的键是 NB 明确声明过的整段原文，判据只有精确相等，或
「由若干登记批按上游的 `"\n"` 拼接而成」。代码不识别 `[OUT-OF-BAND` 形状，正文含同名字样
不会被误剥，有测试钉住。

## 持久化身份根治（2026-11-27）

生产异常：带图长任务发生压缩后，后续 steer 在会话视图里重复显示最初自然轮的正文与旧时间。
QQ 投递台账和 Milky 发送时延正常，问题位于 Hermes 转录持久化。

根因链：多模态自然轮压缩成文本投影后，与运行时图片 parts 不再逐字相等；
`reanchor_current_turn_user_idx` 的精确匹配失败，fallback 把最近的 `display_kind=steer`
误认为当前自然轮。后续压缩／工具结果裁剪再次重锚时可继续选中新的 steer，自然轮的
干净正文、原始提交时间和平台身份于是覆盖到插话上。

修复仍全部在插件侧：

1. 重锚结果若指向 steer，剔除 steer 后按上游同一套规则重算，并把索引映射回原列表；
2. `_db_flush_row` 复核身份，steer 永不接受自然轮覆盖；
3. `_apply_persist_user_message_override` 在回合收尾原地改写前做同样复核。

压缩产生的“归档旧行＋现役新行”会共享 `message_uid`，但分别只有一个 `active=0`
和一个 `active=1`，属于正常版本链，不按重复写入修。历史坏行保留取证，不用界面
遮罩或自动数据清洗冒充根治。

## 不做

- 不改 `_pending_steer` 队列、`steer()`、drain 与投递时机。
- 不新增恢复、身份或第二套状态机，不建表、不落盘。
- 登记表只活在本进程，上限 32 批，一次投递即消费；声明后未投递的残留由上限淘汰。
- 同文 steer 共用登记键时，先消费的一方走零包装，其余回退上游外壳，内容不会丢失；不要依赖
  同文并发批的包装形态一致。

## 请求契约（与 NB 同批）

```http
POST /v1/runs/{run_id}/steer
{"input": "<零包装正文>",
 "presentation": {"schema": "hermes.steer.presentation/1", "format": "plain_body",
                  "hint": "<本批批尾的系统提示（可选）>"}}
```

`hint` 可选。NB 在批尾挂系统提示时一并声明，插件只收拢明确声明且逐字吻合的那一段。

- NB 侧字面量在 `nonebot_plugin_hermes_adapter/core/steer_presentation.py`，两侧靠
  `gateway_plugins/steer-presentation-thin/tests/test_plugin.py::SchemaParityTests` 与
  `tests/test_steer_region_presentation.py::ContractTests` 双向比对。
- 插件未装或版本不认识时，上游丢弃多余字段，文本照旧被外壳包住，回退安全。
- 插件先装、NB 未升级时没有标记，全部走上游原样。

## 安装与回滚

```bash
python scripts/install_gateway_plugin.py steer-presentation-thin --check   # 只比对
python scripts/install_gateway_plugin.py steer-presentation-thin           # 装到 $HERMES_HOME/plugins/
```

装完必须重启对应 gateway（脚本不重启服务）。回滚是删除
`$HERMES_HOME/plugins/steer-presentation-thin/` 并重启；NB 侧无需同批回滚，标记字段会被上游
忽略。提示词追加段随插件一起卸载。

## 部署边界（本次只交付候选）

本阶段不部署、不重启、不上传。steer 行不再被上游按 `[OUT-OF-BAND` 前缀归为合成脚手架，
上下文压缩的 verbatim 用户消息段会把它当真实用户消息收录，摘要预算占用可能略增。多批正文
保持单换行，系统提示只留末尾一条；单批逐字节不动。

## 测试

```bash
# 网关侧（真上游代码，只换 HTTP／平台边界；离开仓库根避免 utils.py 遮蔽）
cd /tmp && /home/neonsky/.hermes/hermes-agent/venv/bin/python -B -m unittest discover \
  -s /home/neonsky/nonebot-plugin-hermes/work/nph-20-work/npha-upload-20261106/.worktrees/body-zero-wrap-20261127/gateway_plugins/steer-presentation-thin/tests -v

# NB 侧（真 direct／followup 入口、真台账、真 matcher）
/home/neonsky/NeonBot/.venv/bin/python -B -m unittest tests.test_steer_region_presentation
```

覆盖 direct／followup、单条／多条、原消息换行与批间单换行、正文含 `OUT-OF-BAND` 字样不误删、
媒体语义占位、source_row 与台账身份、多批拼接、409 撤销登记、未声明文本保持上游外壳、重复
安装幂等、上游形状认不出即抛、两侧字面量一致，以及按真实插件布局 `register(ctx)` 加载。
