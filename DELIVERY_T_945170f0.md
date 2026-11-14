# DELIVERY 外部 Agent 后端 Hermes 暴露面脱敏（t_945170f0）

- 分支：`feat/external-backend-sanitize`；基线 `5989e14`
- 候选：**`45a5498`**（clientInfo／`/ping`／`/help`／三工具说明 + 4 例回归）
  ＋ **`3f55b60`**（HEAD，ACP 默认 cwd 改中性同级目录 + 4 例回归）；父提交 `5989e14`
- 依据：计划 124 + 主代理中途补的**真实泄漏证据**（肥鱼 thinking 按 cwd
  `…/nonebot_plugin_hermes_adapter/antigravity-workspace` 认出 hermes_adapter 并去
  `find / -name "*hermes_adapter*"`）——只改 clientInfo 不够，cwd 一并处理
- 边界：**未部署、未重启 NB、未上传、未改写生产配置、未跑真实模型 prompt、未发真实 QQ**。
  真实 `agy` 只做了不含 `session/prompt` 的协议探针（见 §4）。本说明不是部署批准。

## 1. 逐文件改动（`EVIDENCE_T_945170f0/candidate.diff`，`git diff 5989e14 3f55b60`）

8 文件，+217 / −13；产品代码 5 文件共 **+23 / −13**（含 `_acp_workspace()` 新 docstring 与 3 处注释改词），其余是回归（`tests/test_antigravity_acp.py` +106、新模块 +79、`test_antigravity_mcp_files.py` +9）。

### 1.1 `backends/antigravity/client.py`（4 处）

| 位置 | 改动 |
|---|---|
| `_CLIENT_INFO` | `name`: `nonebot-plugin-hermes-adapter` → **`nonebot-acp-client`**；`version` 仍 `"0.1.0"`，`protocolVersion` 仍 `1`，`clientCapabilities` 原样 |
| `_acp_workspace()` | 默认 cwd `resolve_custom_dir()/antigravity-workspace` → **`Path(base).parent / "acp-workspace"`**（数据目录的**同级**目录）；无数据目录时回退 `<tmp>/nonebot_acp/acp-workspace`（不占公共 `/tmp/acp-workspace`）。**不新增配置项、插件数据目录本身不动**，路径由数据目录位置唯一确定 → 跨重启稳定 |
| 模块 docstring | `HermesClient 只调用 prompt()` → `上层客户端只调用 prompt()` |
| `ACPCancelledError` docstring | `让 HermesClient 能映射 …` → `让上层客户端能映射 …` |

cwd 是单一来源：`_ensure_started()` 的 `create_subprocess_exec(cwd=…)`、
`session/new.params.cwd`、`session/resume.params.cwd` 全部取自 `self._workspace`，
所以只改工厂一处即可，wire 上不会出现两套路径（用例 §2-b 锁死）。

### 1.2 `handlers/commands.py`（2 处）

| 函数 | 改动 |
|---|---|
| `build_ping_message` | `🏓 pong! Hermes Agent 连接正常。` → `🏓 pong! AI 助手连接正常。`；`⚠️ 无法连接到 Hermes Agent，请检查 Gateway 是否正在运行。` → `⚠️ 无法连接到 AI 助手，…`（Gateway 排障指向保留） |
| `build_help_text` 内置头（私聊／群聊） | `🤖 Hermes Agent 帮助` → `🤖 AI 助手帮助`；其余文案、命令清单生成逻辑（管理员行、T1 `/delay` `/resume`、`cmd_prefix`）与自定义头尾路径一字未动 |

未新增接口参数、未加后端名称配置、未加后端分支。

### 1.3 三个白名单 MCP 工具的模块说明（各 1 处）

| 文件 | 改动 |
|---|---|
| `mcp/tools/get_recent_messages.py` | `供 Hermes 端后续调 …` / `让 Hermes 不要直接读到 CDN 链接` → `供调用方…` / `让调用方…` |
| `mcp/tools/get_message_images.py` | `Hermes Agent 端 tools/mcp_tool.py:2231` → `调用方 tools/mcp_tool.py:2231` |
| `mcp/tools/get_message_files.py` | `让 Hermes 端拿本地路径 …` → `让调用方拿本地路径 …` |

**只改模块 docstring。** 实际注册进 MCP 的说明来自 `mcp/server.py` 三个函数自身的
docstring，实测本来就干净（`registered-surface.txt`：三条 description 全英文、无 hermes），
所以**工具行为、注册说明、schema、`_meta` 参数都没动**，`mcp/server.py` 在本候选里 diff 为 0。

### 1.4 审批通知与命令展示

**没有引入任何字符串替换／过滤器。** 审批通知里的命令（`toolCall.title`／`rawInput`
命令行）仍原样展示，`core/approvals.py`、`handlers/commands.py` 的审批段零改动——
本卡只换「cwd 目录名」本身，不去改命令文本里的字面量。

## 2. 新增回归（8 例，全部复用既有入口）

| # | 用例 | 位置 | 断言 |
|---|---|---|---|
| a | `test_initialize_client_info_is_neutral_and_keeps_the_version` | `tests/test_antigravity_acp.py::ACPHandshakeTests` | 真实 `initialize` 帧（NDJSON 夹具记录）：`version == "0.1.0"`、`protocolVersion == 1`、`name` 非空、不含 `hermes`、以 `nonebot` 开头 |
| b | `test_new_and_resume_send_the_neutral_workspace_as_cwd` | 同上 | 真 `session/new` 与 `session/resume` 的 `cwd` 都等于 `数据目录同级/acp-workspace`、都是数据目录的同级、名不含 `nonebot_plugin_hermes_adapter`、目录真的被创建（子进程确在该 cwd 启动） |
| c | `test_workspace_is_a_neutral_sibling_of_the_plugin_data_dir` | `ACPWorkspacePathTests` | 完全受控的合成 base（`/srv/nb-data/nonebot_plugin_hermes_adapter`）→ cwd 恰为 `…/nb-data/acp-workspace`；两次调用一致（跨重启稳定）；不在数据目录内；**整串不含 `hermes`** |
| d | `test_workspace_falls_back_to_a_neutral_temp_dir` | 同上 | 无数据目录时名为 `acp-workspace`，不含 `nonebot_plugin_hermes_adapter` |
| e | `test_session_created_under_the_old_cwd_resumes_under_the_new_one` | `ACPWorkspaceMigrationTests`（`--persist-sessions` 跨进程） | 旧 `…/antigravity-workspace` 下建的会话，换进程 + 新 `…/acp-workspace` 后仍 `session/resume`（`session/new` 仍为 1、`sessionId` 不变、cwd 为新值） |
| f | `test_registered_tool_descriptions_do_not_name_the_upstream_backend` | `tests/test_antigravity_mcp_files.py::AntigravityToolWhitelistTests` | 真 `build_mcp_app(tool_names=ANTIGRAVITY_MCP_TOOL_NAMES)` 实际注册的恰三工具，每条 `description` 非空且不含 `hermes` |
| g | `test_ping_texts_are_backend_neutral` | `tests/test_user_facing_neutral_text.py` | `/ping` 两串不含 `hermes`、都含「AI 助手」，`pong` 与 `Gateway` 形态不变 |
| h | `test_help_texts_are_backend_neutral` | 同上 | 私聊／群聊内置 `/help` 不含 `hermes`、含「AI 助手帮助」，`clear/ping/help` 按 `_display_cmd` 在场，T1 关闭时无 `/delay`，非管理员看不到运行时命令 |

`tests/test_user_facing_neutral_text.py` 是新文件（79 行），但用既有 `tests/_bootstrap` +
`unittest` 与同一套临时目录隔离约定，没有新框架。

关于「cwd 不含 hermes」的断言口径：cwd 由 `resolve_custom_dir()` 决定，**本机 TMPDIR 在
`~/.hermes/…` 下**，对临时目录夹具做整串 blanket `hermes` 断言只会打到家目录本身（首轮
实测就是这样误红的），所以：用例 c 在完全受控的合成路径上做整串断言；用例 b/d/e 断言
**受控成分**（父目录、目录名、不含 `nonebot_plugin_hermes_adapter`、不含
`antigravity-workspace`）。这不是放宽——泄漏源就是插件目录名，它被逐项钉住了。

### 变异核验（证明不是空断言）

| 变异 | 变红用例 | 原始输出 |
|---|---|---|
| `_CLIENT_INFO.name` 改回 `nonebot-plugin-hermes-adapter` | a | `RED-mutations.txt`（4 失败／10 例） |
| `/ping` 文案改回 `Hermes Agent` | g | 同上 |
| 内置 `/help` 私聊头改回 `Hermes Agent 帮助` | h | 同上 |
| `server.py` 的 `get_message_files` docstring 插 `for the Hermes agent` | f | 同上 |
| `_acp_workspace()` 改回 `base/antigravity-workspace` | b、c、d | `RED-workspace-cwd.txt`（3 失败／4 例；用例 e 不依赖工厂，故不红，属预期） |

## 3. 旧会话换 cwd 的可行性

- **夹具（跨进程）**：用例 e 用 `--persist-sessions` 跨进程库证明「旧 cwd 建的会话 + 新 cwd
  的 resume」在客户端侧走的是同一条 `session/resume`，不重建、不丢 `sessionId`。
- **真实 agy 1.5.8 探针**（`probe-acp-cwd-resume.txt`，**不发 `session/prompt`，不耗模型**）：
  在旧 cwd `…/nonebot_plugin_hermes_adapter/antigravity-workspace` 下 `initialize` +
  `session/new` 拿到 `sessionId`，结束进程；换到新 cwd `…/acp-workspace` 起新进程 `initialize` +
  `session/resume {sessionId, cwd: <新>}` → **真实返回同一 `sessionId` 与模型表，无报错**。
  即：升级后首次续聊不会因 cwd 变化被拒；即便将来某个版本改判，现有 `-32602` 重建兜底
  （`test_stale_resume_falls_back_to_a_new_session`）仍会兜住。
- 升级后旧的 `…/nonebot_plugin_hermes_adapter/antigravity-workspace/` **不删**（不在本卡
  删除范围，也不影响新 cwd）；它已不再出现在任何 ACP 帧里。
- 探针在 `~/.gemini/antigravity-cli/` 下留了两条无 prompt 的会话记录（真实 agy 的会话库
  行为），属探针副产物，不碰生产 bot 数据。

## 4. 保留的结构性 Hermes（及理由）

| 位置 | 保留内容 | 理由 |
|---|---|---|
| 包名／模块名 | `nonebot_plugin_hermes_adapter`、发布名 `nonebot-plugin-hermes-adapter` | 卡面禁止改包名；改名牵动数据目录、dist-info、测试 shim、脱敏规则 |
| 类与模块 | `HermesClient`、`core/hermes_client.py` | 卡面禁止改 `HermesClient` 类；它是本插件自己的客户端，不是被替换的后端 |
| 配置键 | `hermes_*`（`hermes_api_key`、`hermes_mcp_*`、`hermes_antigravity_*`…） | 卡面禁止改配置键（`client.py` 里现在只剩这些键名，无注释指称） |
| 日志前缀 | `[HERMES]`、`[HERMES antigravity]`、`[HERMES MCP]` | 卡面禁止改日志前缀（运维检索面） |
| 协议头 | `X-Hermes-Session-Id` | 真实上游 header 名，改了 Runs 通道即断 |
| Hermes 主后端 | completions／runs 后端与 `hermes_client` 全链 | 卡面禁止改主后端 |
| `handlers/commands.py` L399 | `/hermes-status` 回执 `🔍 Hermes Plugin M1-mem 状态` | 该命令本身就是「看本插件运行时状态」的管理员命令名与语义 |
| `handlers/commands.py` L442 | `_HERMES_CONTROL_UNAVAILABLE = "当前 AstrBot 后端未提供此 Hermes 专用命令"` | 描述「AstrBot 后端没有 Hermes 专属控制面」这一既有事实，去掉反而失真 |
| `handlers/commands.py` L204 | `handle_ping` docstring「检查 Hermes 连接状态」 | 内部接线说明（探测走 `hermes_client.health_check`）；卡面只点名 `build_ping_message`／`build_help_text` |
| 其余源码注释 | `handlers/message*.py`、`mcp/server.py:101`、`mcp/tools/{list_active_sessions,push_message}.py`、`core/*`、`tasks/*`、`tests/*` | 卡面限定「仅 ACP 与三个白名单 MCP 工具」；测试里的 `HermesClient`／昵称夹具是回归资产 |

外部 Agent 实际可见的五条面（ACP `initialize.clientInfo`、ACP 会话 `cwd`、三个工具注册
description、系统提示词、`/ping`／`/help`）本轮收口四条；系统提示词肥鱼真实转录实测无
Hermes，未动。

## 5. 实测结果（解释器 `/home/neonsky/NeonBot/.venv/bin/python`，全部 `-B` +
`PYTHONDONTWRITEBYTECODE=1`，cwd＝交付目录；原始输出全部留档）

| 验证 | 实际结果 | 原始输出 |
|---|---|---|
| focused（新模块 + acp/mcp_files/mcp_tools/images/stream_steer/restart/cancel/clear_fence/permission） | **164 OK**（15.031s；阶段4 的 156 + 本次 8） | `focused.txt` |
| 相关批（focused + continuation/control_regressions/steer_pre_debounce/control_mcp_registration/recent_messages_private/get_message_images_contract/inline_image_limit_runs/gif_preview_frames/sticker_mcp_registration） | **232 OK**（15.347s） | `related.txt` |
| `unittest discover -s tests -t .` 全量 | **894 OK（skipped=1）**（47.895s；基线 886 + 本次 8，既有用例无增删） | `full.txt` |
| `unittest tests._bootstrap turn_tests.test_envelope_entry` | **6 OK** | `turn.txt` |
| `verify.py`（候选 `3f55b60` 的干净检出 `git worktree add --detach`，树干净） | **22/22 ALL PASS**（内含全量 894 OK/skip 1、网关插件 48+21 OK） | `verify.txt` |
| 变异核验 | 4 失败／10 例；3 失败／4 例 | `RED-mutations.txt`、`RED-workspace-cwd.txt` |
| 真实 agy 协议探针 | 旧 cwd 建会话 → 新 cwd `session/resume` 返回同一 `sessionId` | `probe-acp-cwd-resume.txt` |
| 注册面实拍 | 三条 description 全文 + `CLIENT_INFO={'name': 'nonebot-acp-client', 'version': '0.1.0'}` | `registered-surface.txt` |
| 改动前后 grep | 五文件 Hermes 命中行 + ACP cwd 取值前后对照 | `before-after-grep.txt` |

verify 在干净检出上跑：工作区还留着未入库的 `EVIDENCE_T_945170f0/` 与本说明，直接跑会命中
「交付树干净」。检出在 scratch：`/home/neonsky/.hermes/profiles/coder_deepseek/cache/scratch/sanitize-verify`
（删目录后 `git worktree prune` 清理登记）。

## 6. 未做／复核时请重点看

- **未部署、未重启**：肥鱼真实 `initialize`（`clientInfo.name` 与 `cwd`）、`/ping`、`/help`
  要在部署后核对；本卡只保证代码、回归与协议可行性。
- 未改包名／配置键／日志前缀／`HermesClient`／Hermes 主后端；未做全仓库替换；未上传。
- 复核关注点：
  1. `clientInfo` 的 `version`／`protocolVersion` 是否真的一字未动（用例 a 已锁）；
  2. `session/new`／`session/resume` 的 cwd 是否只来自 `_acp_workspace()`（唯一来源，用例 b）；
  3. 旧会话换 cwd 的真实可行性（§3 探针 + `-32602` 重建兜底）；
  4. 保留清单里有没有你认为仍暴露、但卡面允许改的项（尤其 `/hermes-status` 回执与
     `handle_ping` docstring 这两个我按「管理员命令语义／内部接线」留下的）。
