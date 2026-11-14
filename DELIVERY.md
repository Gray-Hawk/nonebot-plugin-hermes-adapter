# DELIVERY 阶段2C1 — 两个只读 MCP 工具与 `mcp_` 自动审批（t_9a23b145）

- 分支：`feat/antigravity-acp-2c1`；基线 `54cc1069ba609250c29442b2ec29afe509779262`
- 候选提交：**`10da5cde2bef03ac63ee705aecaf285b0ad2f546`**（父 `54cc106`）
- 定案范围：计划 120（`alive/120_计划_ACP阶段2C1只读MCP与自动审批_2026-11-14_施工中_.md`）
- 仅改候选工作区：**未部署、未重启、未上传、未跑真实 `agy`/模型/QQ、未动主实例与肥鱼**。
  本说明不是部署批准；真实机验收（肥鱼）由主代理在复核通过后执行。

## 1. 逐文件改动

候选提交净 +869/−26 行，其中产品改动 **+139/−22**（4 个产品文件），测试 +730（1 个新测试文件 + 夹具）。

### 1.1 `__init__.py`（启动分支，+8/−4）

- antigravity 分支不再跳过反向 MCP：改为 `await start_mcp_server(tool_names=ANTIGRAVITY_MCP_TOOL_NAMES)`，
  仍由既有 `AGENT_MCP_ENABLED` 控制（关闭时 `start_mcp_server` 自己早退并记 disabled）。
- shutdown 同时注册 `stop_mcp_server` 与 `close_client`；不新增端口、进程或 token 配置。
- 删掉「antigravity 首片不开放 MCP」的旧跳过日志，避免现场误读。

### 1.2 `mcp/server.py`（工具名过滤，+25/−16）

- `build_mcp_app(..., tool_names: Sequence[str] | None = None)`：**唯一新参数**。
  `None` = 注册全表（既有后端/原有 MCP 行为不变）；传名单就只注册名单内工具。
- 新增内部 `_tool(func)`：14 个工具定义里 `@mcp.tool()` 统一换成 `@_tool`，sticker 两个
  条件注册也走 `_tool`。过滤发生在**注册时**，未注册的工具既不在 `tools/list`，也不可调用。
- 不新增配置键、不做工具配置系统、不改 Bearer 中间件、不改工具实现或 schema
  （测试逐项比对过滤前后 description/parameters 相同）。

### 1.3 `mcp/__init__.py`（启动透传，+13/−3）

- `start_mcp_server(*, tool_names=None)` 关键字参数透传给 `build_mcp_app`，
  并各留一行工具表日志（全量 / N 个名单），日志只含工具名，不含任何凭据。

### 1.4 `backends/antigravity/client.py`（+96/−2）

- 常量 `ANTIGRAVITY_MCP_TOOL_NAMES = ("get_recent_messages", "get_message_images")`、
  `MCP_SERVER_NAME = "nonebot-bridge"`、`_MCP_TOOL_PREFIX = "mcp_"`。
- `caller_scope_for_mcp()`（第 37 行）：用真实 `resolve_caller_scope("Bearer <AGENT_API_KEY>")`
  构造本轮反向 MCP 调用方范围（无 key → 既有 `CallerScope.dev()`；认不出 → `None`）。
  **单上游部署里 `CallerScope` 只有 dev/uniform 两态**，没有 per-group token；因此范围为
  uniform，本轮消息上下文只用其 `allows()` 与私聊可识别闸校验目标，不另造 token 表。
- `mcp_servers_for_turn(turn_meta)`（第 53 行）：`AGENT_MCP_ENABLED` 关闭 → `[]`；
  scope 认不出 → `[]`；群聊缺 `group_id` 或 `scope.allows()` 不成立 → `[]`；
  私聊缺 `user_id` → `[]`。成立时返回单个 HTTP server 定义
  `{type:"http", name:"nonebot-bridge", url:"http://<host>:<port>/mcp", headers:[{Authorization: "Bearer <key>"}]}`
  （无 key 时不发 headers，走 dev 语义），URL 复用既有 `hermes_mcp_host/port`。
- `prompt()`（第 1008 行）：每轮在会话装配前算一次 `mcp_servers`，并打一行
  `MCP tools=... server=... url=... bearer=yes|no`（**不含 token**）。
- `_create_session(..., mcp_servers=)`（第 974 行）：`session/new` 参数不再是写死 `[]`；
  正常 resuming 路径的 `session/resume` 参数、以及 resume 失败后重建会话的
  `_create_session` 调用，全部传**同一个 `mcp_servers` 对象**。
- `_answer_agent_request()`（第 357 行）：`session/request_permission` 里取真实
  `params.toolCall.name`，以 `mcp_` 前缀判定 MCP 工具，**在普通 `approval_callback` 之前**
  转交 `_auto_allow_mcp_permission()`；其余请求原样进入 2B 登记链。
- `_auto_allow_mcp_permission()`（第 431 行）：调既有 `_reply_permission(allow=True)`，
  按真实 options 的 `kind=allow_once` 取 optionId；不登记 `ACPPendingPermission`、
  不调 approval_callback、不发管理员通知。没有 `allow_once` 时既有逻辑退到
  `{"outcome":"cancelled"}`（安全拒绝，不是放行）；写帧失败只留一条 warning，
  **不转成人工审批单**。
- 未触碰：图片/媒体/文件内容块、sticker 配置、其余 12 个 MCP 工具、Runs 链、阶段4恢复。

### 1.5 测试与夹具

- 新增 `tests/test_antigravity_mcp_tools.py`（707 行，22 个用例）。
- `tests/fixtures/fake_agy_acp.py` +16/−7：新增 `--permission-name=<n>`，让假 ACP 能如实带出
  真实 `toolCall.name`（缺省不发该字段，2B 帧形态不变）。仍是 NDJSON 假 `agy acp` 子进程，
  不 mock 被测函数。

## 2. 与卡面的差异（已知、未擅自扩大）

1. **`session/load` 无调用点**。卡面要求 new/load/resume 传同一份定义；本客户端只有
   `session/new` 与 `session/resume`（`_load_session` 是本地 `session_keys.db` 读取，不是
   ACP `session/load`）；ACP `session/load` 属阶段4恢复能力，本卡明确不做。
   实现上把定义收敛为 `mcp_servers_for_turn()` 单一来源，new 与 resume（含 resume 失败重建）
   都用同一对象；测试断言 wire 里没有 `session/load` 帧。若后续新增 load 路径，必须复用该函数。
2. **「群/私 CallerScope 与跨 scope 拒绝」按现有单上游语义落地**：`CallerScope.allows()`
   恒真、没有 per-group token，因此「跨 scope」在本仓表现为
   `resolve_caller_scope` 认不出的 Bearer → HTTP 层 401、工具层 `scope_refusal` /
   `private_scope_refusal` 拒绝；不新建隔离模型。测试覆盖：外部 Bearer 401、群/私 target
   缺失不挂 MCP、scope 认不出不挂 MCP。
3. **启动分支无离线上线测试**：`__init__.py` 的 4 行分支（调用 `start_mcp_server(tool_names=...)`
   + shutdown 注册）依赖完整 NB 启动序列，未做 patch 满天飞的伪启动测试；工具表过滤与
   参数透传本身由 `build_mcp_app` / `start_mcp_server` 的真实函数测试覆盖。
   「`AGENT_MCP_ENABLED=true` 时 antigravity 真实起 8743」留待肥鱼真机验证（见第 6 节）。

## 3. RED → GREEN（先写对立测试）

在候选提交之前的基线（产品代码未改）上只放新测试，原始输出见
`EVIDENCE_T_9a23b145/RED-before-fix.txt`：

```
Ran 22 tests … FAILED (failures=6, errors=10)
ERROR … build_mcp_app() got an unexpected keyword argument 'tool_names'   （工具表/HTTP 401）
ERROR … cannot import name 'mcp_servers_for_turn'                        （定义/会话/脱敏）
FAIL  … mcp_ 权限请求被登记成人工审批 / 等待管理员                              （自动放行）
FAIL  … session/new 的 mcpServers 仍是 []                                   （new/resume 定义）
```

修复后同一文件 `Ran 22 tests … OK`（`EVIDENCE_T_9a23b145/GREEN-focused.txt`）。
RED 证据里的 Bearer 已替换为 `[REDACTED]`。

## 4. 实际执行结果

解释器 `/home/neonsky/NeonBot/.venv/bin/python`，全部 `-B`，cwd=候选目录。

| 验证 | 实际结果 | 原始输出 |
|---|---|---|
| 新增 2C1 套件 `tests.test_antigravity_mcp_tools` | **22 OK** | `EVIDENCE_T_9a23b145/GREEN-focused.txt` |
| focused（acp/stream_steer/clear_fence/permission/cancel/mcp_tools） | **115 OK** | `EVIDENCE_T_9a23b145/focused.txt` |
| 相关批（上述 + control_regressions/steer_pre_debounce/control_mcp_registration/recent_messages_private/get_message_images_contract） | **159 OK** | `EVIDENCE_T_9a23b145/related.txt` |
| `unittest discover -s tests` 全量 | **845 OK（skip=1）** | `EVIDENCE_T_9a23b145/full.txt` |
| `tests._bootstrap` + `turn_tests.test_envelope_entry` | **6 OK** | `EVIDENCE_T_9a23b145/turn.txt` |
| `verify.py`（候选提交、工作树干净时） | **22/22 ALL PASS** | `EVIDENCE_T_9a23b145/verify.txt` |

2A/2B 回归由 focused/相关/全量批覆盖（流式、steer 接管、clear 栅栏、权限 once/deny、
取消、FIFO 精确结单均未改动，测试全绿）。

## 5. 卡面验收项对照

| 卡面要求 | 落地 | 测试 |
|---|---|---|
| 复用既有 MCP 启动/`build_mcp_app`/CallerScope/Bearer，无新端口/服务/token | `__init__.py` 调同一 `start_mcp_server`；URL 取既有 host/port；Bearer 取 `AGENT_API_KEY` | 定义测试逐字比对 url/headers；无新增配置键（diff 可查） |
| 固定只暴露 2 个只读工具，其他后端/原 MCP 完整表不变 | `tool_names` 白名单 + `_tool` | `McpToolTableTests` 3 例（恰 2 项 / 默认表含 12 个既有工具 / 定义未被改写） |
| `mcp_` 前缀自动 allow_once，不发通知、不登记 pending | `_auto_allow_mcp_permission` 在 callback 之前 | 客户端 2 例 + handler 2 例；`pending_count()==0`、`admin_notices()==[]`、wire 帧 allow-once |
| 缺 allow_once 安全拒绝 | `_reply_permission(allow=True)` 退 `cancelled` | `test_mcp_permission_without_allow_once_is_safely_refused` |
| 自动应答写帧失败不登记人工单 | 只 warning，无登记 | `test_auto_reply_write_failure_opens_no_manual_ticket`（子进程不可用，现实路径） |
| `run_command` 等非 MCP 仍走 2B once/deny | 分支前置判定只认 `mcp_` | 客户端 1 例 + handler 1 例（登记/通知/`/approve once` 应答到原 RPC id） |
| 群/私 scope 与跨 scope 拒绝 | `mcp_servers_for_turn` 校验 target；外部 Bearer 401 | `McpServerDefinitionTests` 8 例 |
| Bearer 脱敏 | 日志只打 `bearer=yes/no` | `test_token_never_reaches_client_logs`（录制客户端 logger，断言不含占位 token） |
| 首片不接图片/文件/其余工具/阶段4 | 未改 hermes_client、media、文件工具与恢复链 | 相关/全量批 |

## 6. 真实 ACP 未测（明确单列）

1. 未启动真实 `agy`：`session/new`/`resume` 实际挂载、`mcp_<server>_<tool>` 真实工具名、
   真实 `request_permission` 帧的 `toolCall.name` 与自动放行后的工具执行**均未在本卡实测**
   （预研 t_11358697 已在真实 1.5.8 上验证过形态，本卡未复跑）。
2. 未验证 `AGENT_MCP_ENABLED=true` 时 antigravity 启动序列真实起 8743、shutdown 顺序。
3. 未验证 Bearer 错误时 agy「静默少工具」的现场表现（预研已知），本卡只保证不泄漏、不误放行。
4. 未验证多轮/长会话中 resume 重复携带 mcpServers 的稳定性（预研 16/17 仅同进程内）。
5. 未做肥鱼真实群/私验收（读取最近消息、无 approve、`pwd` 仍弹审批）。

## 7. 边界声明

- 测试使用假 `agy acp` NDJSON 子进程 + 真实客户端/HermesClient/handler/审批/出站；
  只替换平台发送边界、管理员白名单、隔离配置与等待节奏。
- `tests/test_antigravity_mcp_tools.py` 里的 `DUMMY_TOKEN` 是自造的离线占位串
  （形如 `offline-dummy-token-not-a-real-credential`），不是任何环境凭据；真实 token 未出现在
  代码、日志、证据或本说明中。
- 未部署、未重启、未上传；复核通过后再合回 ACP 分支并部署肥鱼。
