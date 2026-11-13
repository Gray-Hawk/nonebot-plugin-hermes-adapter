# DELIVERY t_a04914e9 — 阶段1：Antigravity ACP 最小文本后端

分支 `feat/antigravity-acp`，基线 `18b93f6`。候选提交：见本卡完成回执（本文件随候选一起提交）。

## 1. 做了什么

只走官方 `agy acp --process-per-session` 的 stdio NDJSON JSON-RPC；未接 SDK、直接 Google 端点、
第三方 OAuth、hook、MCP、steer、实时流式队列、后台恢复或独立 ACP 桥。

### 新增

- `backends/antigravity/client.py`：插件实例级懒启动 ACP 客户端。
  - `initialize(protocolVersion=1)` → `session/new` → `session/prompt`；已有映射走 `session/resume`。
  - `session/resume` 只在系统上下文哈希变化时带 `_meta.antigravity.systemPrompt.append`
    （标准 ACP `systemPrompt` 字段不使用）。
  - 模型经真实 ACP `session/set_model` 下发（显式配置时；空串保持账号默认）。
  - 只消费 `session/update.update.sessionUpdate == "agent_message_chunk"` 且
    `content.type == "text"` 的正文；`agent_thought_chunk` / `plan` / 工具状态一律丢弃。
  - `session/request_permission` 在原 RPC id 回复，统一选 `reject_once`（无则任一 `reject_*`，
    再没有则 `cancelled`）；未知 agent 请求按 method-not-found 拒绝，不阻塞模型。
  - 错误、进程退出、超时统一抛 `ACPError` 家族；超时额外发 `session/cancel`，不自动重放 prompt。
  - 进程随插件 shutdown 清理（`close_client`）。
- `tests/fixtures/fake_agy_acp.py`：离线 fake ACP NDJSON server，覆盖 initialize/new/resume/
  prompt/set_model/permission/取消/进程退出/错误/挂起。
- `tests/test_antigravity_acp.py`：26 项离线验收（见 §3）。

### 修改

- `config.py`：`agent_api_interface` 增加 `antigravity`；新增极简配置
  `agent_antigravity_cli`（缺省 `agy`）与 `agent_antigravity_model`（缺省空串）。
  `hermes_control_plane_enabled()`、新增 `background_report_enabled()` 改为只在
  `completions`/`runs` 开启，`!= astrbot` 不再误开后端能力。
- `core/storage/session_key_store.py`：同一 `session_keys.db` 新增 `acp_sessions` 表，
  存 `session_key → (acp_session_id, system_hash, model)`；不新增数据库。
- `core/hermes_client.py`：`chat()` 分发接入 antigravity；复用 `ChatResult` 与
  `runs_text.decisions_from_text` 的最终 § 解析；Hermes 专属 403/溢出/持久化文案检测只对
  `completions`/`runs` 生效；`/ping` 对 antigravity 只做 ACP 握手探活，不发任何 Hermes HTTP。
- `core/prompt_builder.py` + `core/prompt_protocol.py`：复用现有系统上下文与 § 纯文本协议；
  按后端剥离 `submit_control`/`push_message`/`request_continuation`/`get_background_tasks`/
  `get_message_images`、任务锚、退出门槛与文件/表情/记忆路由等首片不存在的工具说明。
- `handlers/message_turns.py`：antigravity 历史媒体不再重复内联；群 passive 也走正文决策通道，
  避免 § 标记原样进群。
- `__init__.py`：antigravity 启动时跳过反向 MCP server 与后台完成轮询，并注册 ACP 进程清理。

### 配置

```dotenv
AGENT_API_INTERFACE=antigravity
AGENT_ANTIGRAVITY_CLI=agy
# 可留空用账号默认模型；显式指定时走 ACP session/set_model
AGENT_ANTIGRAVITY_MODEL=gemini-3.8-flash-high
```

工作目录固定为插件既有数据目录下的 `antigravity-workspace/`（`resolve_custom_dir()`）；
没有硬编码主实例/肥鱼路径、账号或 token。凭据/OAuth 全部由官方 `agy` 自己管理。

## 2. 行为边界（首片）

- 只支持纯文本 prompt；图片/文件轮在本地拒绝（`is_local_refusal`），不启动 ACP、不发半截。
- 一轮 ACP 输出完整汇总后才按 § 解析发送，不接流式中途投递。
- `/clear` 复用 `SessionManager` 的 generation 换代：新 session key 没有 ACP 映射，
  下一次自然 `session/new` 新 ACP 会话；旧代正文继续由既有交付栅栏拦截。
- 工具权限全部拒绝（文件/shell/消息均不开放）；没有 submit_control/MCP。
- 未自动重放任何有副作用的 prompt；resume 只在「会话不存在（-32602）」时重建，其余错误如实上抛。

## 3. RED → GREEN 证据

解释器：`/home/neonsky/NeonBot/.venv/bin/python`，`PYTHONDONTWRITEBYTECODE=1`，`-B`。

| 垂直片 | 先看到的失败（RED） | GREEN |
| --- | --- | --- |
| ACP 映射持久化 | `AttributeError: 'SessionKeyStore' object has no attribute 'load_acp_session'`（2 errors） | `test_antigravity_acp.ACPSessionMappingTests` 2/2 |
| 懒启动 + initialize/new/prompt | `ModuleNotFoundError: ...backends.antigravity`（2 errors） | `ACPHandshakeTests` 2/2 |
| resume / 哈希变化 / 会话失效重建 | `ACPError: ACP session/resume 尚未实现`（3 errors） | `ACPResumeTests` 3/3 |
| 真实 ACP 模型选择 | `AssertionError: 0 != 1`（没有 set_model 帧） | `ACPModelTests` 2/2 |
| 权限原 id 拒绝 | 回帧是 `error -32601`（`unsupported client method: session/request_permission`） | `ACPPermissionTests` 1/1 |
| HermesClient 接线 / § 多单位 / 零 HTTP | 5 failures：`⚠️ AI 服务异常: Hermes HTTP 不得被调用` | `HermesAntigravityChatTests` 6/6 |
| prompt 能力门 | `AssertionError: '§' not found`；收紧后 `'get_message_images' unexpectedly found` / `'SOUL' unexpectedly found` | `AntigravityPromptTests` 2/2 |
| 配置/控制面/媒体门/探活 | 4 failures + 2 errors（Literal 缺 antigravity、控制面 True、历史媒体未剥离、health False） | `AntigravityConfigGateTests` 4/4、`AntigravityMediaGateTests` 1/1、`HermesAntigravityHealthTests` 1/1 |
| 超时取消 | `AssertionError: 0 != 1`（没有 session/cancel） | `ACPTimeoutTests` 1/1 |
| `/clear` 新会话 | 该组合用例首跑即通过（前几片已把行为拼齐，作为特性锁定测试保留） | `ACPClearSessionTests` 1/1 |

最终 focused：

```text
python -B -m unittest tests.test_antigravity_acp -v
Ran 26 tests ... OK
```

全量：

```text
python -B -m unittest discover -s tests -t .
Ran 741 tests in 33.7s
OK (skipped=1)

python -B -m unittest discover -s turn_tests -p 'test_*.py'
Ran 6 tests ... OK
```

`verify.py`（提交 `d4ae6c2` 的干净树）：

```text
ALL PASS: 22/22 项通过
```

其中全量项原始行：`ran=741 Ran 741 tests in 33.720s` / `OK (skipped=1)`；
交付树干净、无 pyc、网关插件独立闸（48 / 21）均 PASS。

## 4. 明确未实施

- 图片/文件/语音等媒体进入 ACP；首片文本轮本地拒绝。
- 实时流式中途投递、interim/旁白、steer、run 恢复、后台轮询、MCP/submit_control/任务锚/退窗。
- Hermes 服务侧人格/技能/记忆写入路由；antigravity prompt 不教这些能力。
- 真实 `agy` 模型调用与 OAuth/凭据处理（按任务边界留给肥鱼环境验收）。
- 多行 systemPrompt 增量去重之外的 prompt 缓存优化、ACP 会话显式关闭。

## 5. 肥鱼真实 ACP 验收步骤（主代理执行）

1. 在肥鱼实例环境（`/home/feiyu/NeonBot`，数据目录
   `/home/feiyu/.local/share/nonebot2/nonebot_plugin_hermes_adapter`）部署候选，配置：
   `AGENT_API_INTERFACE=antigravity`、`AGENT_ANTIGRAVITY_CLI=agy`；模型先留空，
   确认账号默认可用后再试 `gemini-3.8-flash-high`。
2. 确认官方 CLI 已登录（`agy` 自己管理凭据；不要在插件里注入 token）；重启肥鱼 NB（主代理操作）。
3. 私聊肥鱼发中性 canary：`请记住代号 ACP-CANARY-7731，只回复收到`。
   - 期望 QQ 只看到正文（不含 §）；`session_keys.db` 的 `acp_sessions` 出现该会话行；
     `agy acp --process-per-session` 进程存活。
4. 续聊：`代号是什么？` → 应答包含 `ACP-CANARY-7731`（同一 ACP 会话 resume）。
5. 发 `/feiyu clear`（肥鱼命令前缀），再问 `代号是什么？` → 不应再记得；数据库出现新
   session key 的 ACP 行，旧行不被复用。
6. `/feiyu ping` → pong；同时确认 8642/8742 上没有新 Hermes HTTP 请求（ACP 只做握手）。
7. 让 bot 写文件（如「把 hello 写到 test.txt」）：应明确拒绝/说明无法执行且不挂死
   （permission `reject_once`）。
8. 停肥鱼 NB：`agy acp` 子进程应随插件 shutdown 退出。
9. 日志抽查：不得出现完整系统提示、用户正文或凭据；只允许会话/错误码/长度类信息。

若真实 `agy` 与假设不一致，优先核对三处：`session/resume` 的参数形状、
`session/set_model` 是否可用、`_meta.antigravity.systemPrompt.append` 在 resume 上是否生效。
