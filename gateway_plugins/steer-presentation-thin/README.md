# steer-presentation-thin（候选，未部署）

Runs 车道中途 steer 的呈递收口：**NB 渲染的聊天区区域块原样落成那条 user 行，不再套
上游的 `[OUT-OF-BAND USER MESSAGE — …]` 外壳。**

## 为什么需要它（上游在哪儿加壳）

| 位置（stock） | 行为 |
| --- | --- |
| `gateway/platforms/api_server_runs.py::_handle_steer_run` | `POST /v1/runs/{id}/steer` 只读 body 的 `input`/`message`/`text`，归一后 `agent.steer(text)`；**多余字段在此丢弃** |
| `agent/agent_runtime_helpers.py::apply_pending_steer_to_tool_results` | 工具批次结束后 drain，`steer_user_row(text)` 追加独立 user 行 |
| `agent/turn_iteration_prep.py::_inject_steer_after_newest_tool_result` | 上一次 API 调用期间到达的 steer，同一行构造函数 |
| `agent/prompt_builder.py::steer_user_row → format_steer_marker` | **加壳点**：`[OUT-OF-BAND USER MESSAGE — …]` + 正文 + `[/OUT-OF-BAND USER MESSAGE]` |

生产截图 m:68490 的三处问题（外壳、无区域标签、正文与尾部系统提示只隔一个换行）里，
后两处是 NB 侧（区域块 + 约定行空行，见 NB `core/prompt_builder.render_chat_region` /
`handlers/message_relay._with_steer_reply_convention`），外壳这一处必须由网关侧收口。

## 做法（三处窄补丁，全部是包装，不改上游一行）

1. **`api_server_runs._handle_steer_run` 前置读一次 body**：body 带 NB 的结构化呈递标记
   `{"presentation": {"schema": "hermes.steer.presentation/1", "format": "chat_region"}}`
   时，把**上游将要投递的那份归一文本**（同一 `input`/`message`/`text` or 链 +
   `_normalize_chat_content(...).strip()`）登记到本进程的 `PresentedBatches`；上游返回非
   200（409/400…）时撤销登记。aiohttp 的 `request.read()` 会缓存字节，下游
   `_read_json_body` 仍读到同一份请求体。
2. **`steer_user_row`**：文本命中登记表 → 原样落行（`{"role": "user", "content": 文本,
   "display_kind": "steer"}`）；未命中 → 走上游 `format_steer_marker` 原样。
   `agent.agent_runtime_helpers` 在模块顶层按名导入过 `steer_user_row`，所以两处名字
   绑定一起换指（只换指向原函数的那个）。
3. **`agent.system_prompt.STEER_CHANNEL_NOTE`**：只在其后**追加一段**说明——本部署里
   区域块外观的 steer 同样是可信的真实用户消息。原标记说明一字不改，所以 CLI/桌面/
   Kanban 工具注入这些仍走上游标记的通道语义不变。没有这一段，模型面对"没有标记的
   中途 user 行"会按注入拒收（上游 #40240 的历史故障）。

**不按文本前缀猜上游包裹。** 登记表的键是 NB 明确声明过的整段原文，判据只有两种：
精确相等，或「由若干登记批按上游的 `"\n"` 拼接而成」（一次 drain 投递多批时才出现）。
代码里没有对 `[OUT-OF-BAND` 形状的识别，所以正文里出现同名字样不会被误剥——有测试钉住。

## 不做

* 不改 `_pending_steer` 队列、`steer()`、drain 与投递时机；
* 不新增恢复、身份或第二套状态机，不建表、不落盘；
* 登记表只活在本进程，上限 32 批，一次投递即消费（声明了却没投递的残留靠上限淘汰）。

## 请求契约（与 NB 同批）

```http
POST /v1/runs/{run_id}/steer
{"input": "<聊天区区域块>",
 "presentation": {"schema": "hermes.steer.presentation/1", "format": "chat_region"}}
```

* NB 侧字面量在 `nonebot_plugin_hermes_adapter/core/steer_presentation.py`，两侧靠
  `gateway_plugins/steer-presentation-thin/tests/test_plugin.py::SchemaParityTests` 与
  `tests/test_steer_region_presentation.py::ContractTests` 双向逐字比对。
* 插件未装 / 版本不认识 → 上游丢弃多余字段，文本照旧被外壳包住：**回退安全，不丢内容**
  （模型看到的是「外壳里套一个区域块」）。
* 插件先装、NB 未升级 → 没有标记，全部走上游原样：零行为变化。

## 安装 / 回滚

```bash
python scripts/install_gateway_plugin.py steer-presentation-thin --check   # 只比对
python scripts/install_gateway_plugin.py steer-presentation-thin           # 装到 $HERMES_HOME/plugins/
```

装完必须重启该实例的 gateway（脚本不重启服务）。回滚 = 删除
`$HERMES_HOME/plugins/steer-presentation-thin/` 并重启；NB 侧无需同批回滚（标记字段会被
上游忽略）。**提示词追加段随插件一起卸载**，不会留在系统提示里。

## 部署边界（本次只交付候选）

不部署、不重启、不上传。已知差异与限制：

* steer 行不再被上游按 `[OUT-OF-BAND` 前缀归为「合成脚手架」：上下文压缩的 verbatim
  用户消息段会把它当真实用户消息收录（内容本来就该保留），摘要预算占用略增；
* 一次 drain 投递多批时，上游用 `"\n"` 拼接，两段区域块之间是单换行（NB 每批内部仍是
  空行分隔）；组成关系有测试覆盖；
* 登记表键跨 agent 共享（进程内）：「同一段文本」在此之前被另一个 run 声明过且未投递时，
  理论上后到那次会被当作已声明。文本含真实 `m:id`/身份栏，实际不可碰撞；上限 32 批。

## 测试

```bash
# 网关侧（真上游代码，只换 HTTP/平台边界）
cd /tmp && /home/neonsky/.hermes/hermes-agent/venv/bin/python -B -m unittest discover \
  -s /home/neonsky/nonebot-plugin-hermes/work/nph-20-work/control-mcp-20261109/gateway_plugins/steer-presentation-thin/tests -v

# NB 侧（真 direct/followup 入口、真台账、真 matcher）
/home/neonsky/NeonBot/.venv/bin/python -B -m unittest tests.test_steer_region_presentation
```

覆盖：direct/followup、单条/多条、空行与区域标记、正文含 `OUT-OF-BAND` 字样不误删、
媒体占位 `[图 m:id#idx]`、m:id 回查与最终转录文本、多批拼接、409 撤销登记、未声明文本
保持上游外壳（负对照）、重复安装幂等、上游形状认不出即抛、两侧字面量一致。
