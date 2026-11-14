# DELIVERY 阶段2C3 — `get_message_files` 第三个 MCP 工具与 `read_file` 边界（t_1a01e0bb）

- 分支：`feat/antigravity-acp-2c3`；基线 `3ad5783f1633a0574103b60d206c6abbbd3d6c15`
- 候选提交：**`8c33227`**（父 `3ad5783`）
- 定案范围：计划 122（`alive/122_计划_ACP阶段2C3消息文件与read_file_2026-11-14_施工中_.md`）
- 仅改候选工作区：**未部署、未重启、未上传、未跑真实 `agy`／模型／QQ、未动主实例与肥鱼、
  未改任何生产配置**（肥鱼 `agent.md` 的 `read_file` 补充与真实 QQ 验收由主代理部署阶段执行）。
  本说明不是部署批准。

## 1. 结论与体量

本片是「加一个工具名」的垂直片：ACP 的 `mcp_` 自动放行在 2C1 就已经是**按前缀**判定的，
所以第三个只读工具接进来不需要新的审批分支、目录权限或配置键。

| 类别 | 行数 |
|---|---|
| 产品（3 个文件） | **+8 / −4**，其中真正的行为改动 **+1 行**（白名单加 `"get_message_files"`），其余是注释／docstring 从「两个」改为「三个」 |
| 测试（1 个新文件 + 1 个既有文件） | **+539 / −6**：`tests/test_antigravity_mcp_files.py` 532 行 / **16 用例**；`tests/test_antigravity_mcp_tools.py` +7／−6（白名单常量与命名同步为三工具） |

## 2. 逐文件改动

### 2.1 `backends/antigravity/client.py`（+4/−1）

- `ANTIGRAVITY_MCP_TOOL_NAMES` 增为三元素，**原两个顺序不动**，`get_message_files` 排第三。
  该常量只在 `__init__.py` 启动路径作为 `start_mcp_server(tool_names=...)` 传入，
  因此 `tools/list` 与真实调用同时受限（2C1 的注册期过滤）；默认 `build_mcp_app()` 全表与
  其他后端零影响。
- 注释补一句边界：模型拿 `refs` 换 FileCache 的 `local_path`，再用原生 `read_file` 读正文，
  `read_file` 仍走人工审批。
- **未改**：`_MCP_TOOL_PREFIX`／`_auto_allow_mcp_permission`／`_register_permission`／
  `mcp_servers_for_turn`（Bearer、群私 scope、new+resume 同源）全部原样。

### 2.2 `__init__.py`（+3/−2）、`mcp/__init__.py`（+1/−1）

启动分支与 `start_mcp_server` docstring 的「两个只读工具」改为「三个」。无逻辑改动；
启动日志的 `工具表: N 个 (...)` 本来就按 `tool_names` 动态打印。

### 2.3 测试

**`tests/test_antigravity_mcp_files.py`（新增，16 用例）**

1. 工具表（4）
   - 产品常量与**独立字面量**三元组逐项相等（也即顺序：原两个在前）；
   - 用**产品常量**（不是测试自造白名单）注册时，注册表恰三个且顺序一致；
   - 默认（未过滤）`build_mcp_app()` 仍含 12 个既有工具，且多于三个；
   - 第三工具在过滤表与默认表里 description／input schema／output schema **同一份**。
2. 第三工具 schema 与真实 wire（4）
   - `refs` 必填、items 为 `{message_id:int, idx:int(min 0)}` 且两者均必填；
   - 描述告诉模型返回 `local paths`、最多 4 refs、ref 来自 `[文件 m:<id>#f<idx>:...]`；
   - `get_recent_messages` 的 **output schema**（`file_count` 字段说明）带 `get_message_files`，
     即模型能从消息元数据发现该工具；
   - **真 FastMCP 客户端**（`fastmcp.Client`，进程内线上 wire）：`tools/list` 顺序恰为三个，
     `call_tool("get_message_files", {refs:[…]})` 返回结构化结果；本进程无 HTTP 调用方
     → `not_found` 且 `local_path=null`（fail-closed）；5 个 refs 在服务端被拒。
3. 权限对立（3，**真 handler → 真 `HermesClient.chat` → 真 `AntigravityACPClient` → 假 `agy acp` 子进程**）
   - `mcp_nonebot-bridge_get_message_files` 的真实权限帧（`kind=other`、真实
     `optionId: allow/deny`）→ 自动 allow_once：回帧恰 1 条 `optionId="allow"`、
     人工 pending 0、无管理员通知、客户端 `_permissions` 为空；
   - 原生 `read_file`（真实帧形态：`name=read_file`、`kind=read`、`rawInput.Path`、同一组
     `allow/deny`）→ 仍登记人工单（choices `once/deny`）、发 1 条通知、应答前**不回帧**；
     `once` 后回 `optionId="allow"` 并清账；
   - 同上 `deny` → 回 `optionId="deny"`，pending 清 0。

   `run_command` 的人工对立面由 2C1 的 `test_run_command_still_goes_through_admin_approval`
   与 `test_non_mcp_permission_still_waits_for_the_admin` 覆盖（同批全绿、未改动）。
4. 群／私 scope 与越权（5，直调 `get_message_files_impl`）
   - 认不出调用方（`scope=None`）→ 群与私两条 ref 都 `not_found`，**不发起下载、不解析 bot**、
     不写库；
   - `uniform` 调用方的群 ref 与私 ref 都走到下载步（`download_failed`，`local_path=null`）；
   - 不存在的 message_id 与越权同为 `not_found`（不可区分）；
   - 缓存命中直接返回 `local_path`／`sha256`／`actual_size` 并回填下载态，不重复下载；
   - `GetMessageFilesInput` 上限 4 refs（0 与 5 都被拒）。

**`tests/test_antigravity_mcp_tools.py`（+7/−6）**：`READ_ONLY_TOOLS` 常量与两个用例名同步为
「三个」，断言仍是「过滤表恰等于该集合」「默认表含全部 12 个既有工具」。2C1 的 mcpServers、
Bearer、`mcp_` 自动放行、`run_command` 人工用例一行未改。

## 3. RED → GREEN

先把新套件写在基线上（产品代码未动），原始输出 `EVIDENCE_T_1a01e0bb/RED-before-fix.txt`
（基线白名单只有两个工具时的真实失败）：

```
FAIL test_product_whitelist_is_exactly_the_three_read_only_tools_in_order
     Tuples differ: ('get_recent_messages', 'get_message_images')
                  != ('get_recent_messages', 'get_message_images', 'get_message_files')
FAIL test_registration_with_the_product_whitelist_exposes_exactly_three
ERROR test_schema_requires_refs_of_message_id_and_idx          KeyError: 'get_message_files'
ERROR test_description_tells_the_model_it_returns_local_paths  KeyError: 'get_message_files'
ERROR test_third_tool_definition_is_identical_in_filtered_and_default_tables  KeyError
ERROR test_wire_serves_the_third_tool_and_rejects_five_refs    KeyError
Ran 17 tests → FAILED (failures=2, errors=4)
```

RED 里的 17 条包含当时还存在的 `run_command` 用例；GREEN 阶段判断它与 2C1 的
`test_run_command_still_goes_through_admin_approval` 重复，已删除，所以最终 16 条。

加一行白名单后：新增 16 + 2C1 既有 22 = **38 OK**（`EVIDENCE_T_1a01e0bb/GREEN-focused.txt`）。

## 4. 实际执行结果

解释器 `/home/neonsky/NeonBot/.venv/bin/python`，全部 `-B`，cwd＝候选目录，全部在候选
`8c33227` 上跑。

| 验证 | 实际结果 | 原始输出 |
|---|---|---|
| 新增 2C3 套件 + 2C1 既有套件 | **38 OK** | `EVIDENCE_T_1a01e0bb/GREEN-focused.txt` |
| focused（acp/stream_steer/clear_fence/permission/cancel/mcp_tools/images/mcp_files） | **149 OK** | `EVIDENCE_T_1a01e0bb/focused.txt` |
| 相关批（上述 + control_regressions/steer_pre_debounce/control_mcp_registration/recent_messages_private/get_message_images_contract/inline_image_limit_runs/gif_preview_frames） | **202 OK** | `EVIDENCE_T_1a01e0bb/related.txt` |
| `unittest discover -s tests -t .` 全量 | **879 OK（skip=1）** | `EVIDENCE_T_1a01e0bb/full.txt` |
| `tests._bootstrap` + `turn_tests.test_envelope_entry` | **6 OK** | `EVIDENCE_T_1a01e0bb/turn.txt` |
| `verify.py`（产品与测试代码与候选 `8c33227` 相同；运行提交与工作树状态见首行） | **VERIFY_RESULT** | `EVIDENCE_T_1a01e0bb/verify.txt` |

全量从基线 863 → **879**（只新增 16 个用例，既有用例无增删），skip 仍是 1。
2A（流式／steer 接管／clear 栅栏）、2B（once／deny／取消／双超时）、2C1（工具表、Bearer、
`mcp_` 自动放行、`run_command` 人工）、2C2（image 块与媒体收窄）均由 focused／相关／全量批覆盖，
产品代码未触碰这些链路。

## 5. 卡面验收项对照

| 卡面要求 | 落地 | 测试 |
|---|---|---|
| 白名单只加 `get_message_files`，保留原两个顺序；默认全表与其他后端不变 | `ANTIGRAVITY_MCP_TOOL_NAMES` 加一项；`build_mcp_app`／`mcp_servers_for_turn` 未改 | 工具表 4 例（含默认全表、定义同一份） |
| 复用现有 QQ File 元数据、`m:id#fN`、get_message_files refs、FileCache／local_path | 零新增下载／提取／存储代码；工具实现与契约原样 | wire 例（refs 契约 + fail-closed）+ scope 5 例（缓存命中返回 local_path） |
| get_message_files 的 `mcp_` 权限自动 allow_once、无人工 pending；read_file／run_command 仍人工 | 沿用 2C1 的 `mcp_` 前缀分支；不开 always-proceed、不扩目录权限 | 自动放行 1 例（真实 optionId）+ read_file 人工 2 例 + 2C1 run_command 2 例 |
| 测试覆盖：工具表恰 3、默认全表不变、第三工具 schema/wire、自动权限、人工对立、群私 scope/越权 | 见 §2.3 | 16 例 |
| 至少一组真客户端／fake ACP，不能只改常量后自报收尾 | 权限 3 例为真 handler → 真客户端 → 假 `agy acp` 子进程；wire 4 例走真 FastMCP 客户端 | 同上 |
| 全量回归 2A/2B/2C1/2C2，focused／相关／全量／turn／verify 留原始输出 | 见 §4 | 5 份原始输出 |
| 不改生产配置、不部署／重启／上传；肥鱼 agent.md 与真实 QQ 验收交主代理 | 本卡未触碰任何生产路径 | — |

## 6. 未实测项（明确单列）

1. **真实 ACP 下的第三工具调用**：本卡不跑 `agy`／模型／QQ。自动放行是用 2026-11-13 真实抓包
   形态（`mcp_nonebot-bridge_<tool>`、`kind=other`、`optionId allow/deny`）离线复刻的；
   真实 1.5.8 上「模型决定调用文件工具 → 自动放行 → 拿到 local_path」未实测。
2. **原生 `read_file` 的真实权限提示**：预研已实测「cwd 外路径会弹 `kind=read` 的 once/deny」
   （`15-acp-read-outside-cwd.ndjson`），但「本插件产出的 FileCache 路径」未实测；缓存目录与
   ACP workspace 的父子关系由部署决定，本卡未改。
3. **多文件与 4-refs 上限的真实成本／时延**：工具表增量按预研 per-tool 统计约 1587 字符，
   一次真实工具调用的第二轮重发成本未同口径实测。
4. **大文件／二进制读**：`read_file` 对超大文件、非文本文件的截断与报错表现未测；本片不做文本提取器。
5. **肥鱼 Nano `agent.md` 的 `read_file` 声明**：未改（属主代理部署阶段）；缺该声明时模型只会
   声称没有权限、不会产生权限请求（见 `antigravity-cli-acp` 技能）。
6. **主实例**：未触碰，继续走 Hermes 后端。

## 7. 边界声明与回滚

- 测试只替换：官方 `agy acp` 进程（NDJSON 夹具）、平台发送边界、管理员白名单、隔离配置与存储。
  不跑真实模型、不发真实 QQ、不写生产数据目录。
- 测试里的 canary（`ACP2C3-FILE-CANARY`、`offline-download`、`DUMMY_TOKEN`）都是自造占位，
  不是任何真实凭据或真实消息。
- 候选是独立分支上的单个提交 `8c33227`（父 `3ad5783`）；回滚＝把该提交从部署树撤下，
  无数据迁移、无配置改动、无新端口／凭据／库表。产品侧删掉白名单里的 `"get_message_files"`
  即可回到 2C2 行为（第三工具从 `tools/list` 消失，模型看不到）。
