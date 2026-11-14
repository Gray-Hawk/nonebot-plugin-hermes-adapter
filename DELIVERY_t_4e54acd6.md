# DELIVERY Agent Adapter 更名：仓库、测试与发布物（t_4e54acd6）

- 分支：`feat/agent-adapter-rename`；基线 `944cc50`（已含脱敏与中性 cwd）
- 候选：**`2032bfc`**（发布元数据 + 运行代码）＋ **`d612dc4`**（测试/脚本/网关/verify）；父提交 `944cc50`；
  交付说明与证据随本文件的 docs 提交（`docs(rename): 交付说明与 …`，HEAD）
- 命名：发布名 `nonebot-plugin-agent-adapter`／导入名 `nonebot_plugin_agent_adapter`／显示名 `Agent Adapter`／版本 `0.2.0`
- 边界：**未改生产目录与配置、未部署、未重启、未上传、未改远端仓库名、未动 `~/.hermes/skills` 的 95 文件**。
  本说明不是部署批准，也不是上传批准。
- 依据：计划 125 ＋ `~/.hermes/cache/scratch/agent-adapter-rename/REPORT.md`（§2/§4/§7）＋
  `~/.hermes/skills/nonebot-hermes-plugin-modding/references/packaging-and-module-rename.md`
- 体量：`git diff --shortstat 944cc50 d612dc4` → **91 文件，+923 / −635**；其中产品代码
  （`__init__.py`/`config.py`/`mcp/__init__.py`/`pyproject.toml`/`setup.py`）**20 行新增**，其余是机械改名与新增闸。

## 1. 逐类别改动

### 1.1 发布门面（8 文件，+27 / −24，`2032bfc`）

| 文件 | 改动 |
|---|---|
| `pyproject.toml` | `name` → `nonebot-plugin-agent-adapter`；`version` → `0.2.0`；description 的「Hermes Agent API Server」→「Agent 后端 API」（上游分支说明保留）；`[tool.nonebot].plugins` / `packages.find.include` / `package-data` 键 → 新模块名。**四处字面量同切**，漏一处即空 wheel 或声明失效 |
| `setup.py` | 守卫 `_PACKAGE` 与报错文案 → 新包名（扁平树构建仍被拒） |
| `__init__.py` | 模块 docstring → `NoneBot Plugin Agent Adapter`；`__version__` → `0.2.0`；`__plugin_meta__.name` → `Agent Adapter`；description → 「通过 Agent 后端 API…」；usage 里「与 Hermes Agent 对话」→「与 AI 助手对话」（与已脱敏的 `/ping`、`/help` 同口径）；加载行 `Hermes Plugin loaded` → **`Agent Adapter loaded`** |
| `README.md` | `<h2>`、分发名、模块名 → 新名；badge 的 raw URL **留旧仓库**并记 `TODO(D3)` |
| `LICENSE` | 首行名与第二处版权行 → 新名；`fork of nonebot-plugin-hermes` 与上游作者版权行**不动** |
| `skill/SKILL.md` | description 里的 `nonebot-plugin-hermes-adapter bridge` → 新名（技能 `name: nonebot-bridge` 不变） |

### 1.2 运行代码（3 文件，10 行）

| 位置 | 改动 |
|---|---|
| `config.py` `PLUGIN_MODULE_NAME` | → `nonebot_plugin_agent_adapter`。这是 localstore 调用栈缺失时**唯一的按名兜底读点**（`core/storage/file_cache.py` 延迟导入它） |
| `mcp/__init__.py` `_default_db_path` docstring | 示例路径 → 新模块名目录 |
| `__init__.py` | 见 §1.1（docstring／版本／元数据／加载行） |

未新增接口、未加后端分支、未动 `hermes_*` 字段与 `AGENT_*` ENV。

### 1.3 测试（65 文件，含 1 个新文件）

- `tests/` 62 个既有文件 + `turn_tests/test_envelope_entry.py`：硬 import、`tests._bootstrap` shim、
  `AGENT_PLUGIN_CUSTOM_DIR` 夹具、路径断言一次切新名，**不留双链**。
- `tests/_bootstrap.py`：shim 换新名，并新增 meta path **旧名阻断器**（`_OLD_PACKAGE_NAMES` 是旧名唯一字面量真源）：
  生产 venv 的 site-packages 里仍装着改名前的包，只换 shim 会让漏改的套件解析到旧包并假绿。阻断器还把
  已进入 `sys.modules` 的旧名清掉。
- `tests/test_module_rename.py`（新，7 例）：
  七处名字一致、版本/显示名同源、旧名不可导入、旧运行兜底不在场（按名解析出的目录名是新名）、
  新名解析到本工作树、ACP `clientInfo` 不带插件名、`[HERMES]` 前缀保留而加载行已改名。
  旧名**不在本文件里另写字面量**，直接读 `_bootstrap._OLD_PACKAGE_NAMES`。
- 跨树对照的部署路径（`tests/test_queued_lane_interim_client.py`、
  `gateway_plugins/queued-lane-interim-thin/tests/test_plugin.py`）：改成**新目录优先、旧名兜底**。
  生产冷迁移未做，旧目录仍是"线上装的那份"；写成单一旧名会让迁移动手当天这套对照静默失效，
  写成单一新名会在迁移前整段 skip。兜底条目在生产迁移完成后删掉。
- `tests/test_evidence_probe_readonly.py`：历史取证探针**文件一字不改**（它自带旧名 import），
  只在加载时把源码里的旧名重写成新名——避免"为了跑历史探针而引入双导入兼容层"。

### 1.4 脚本与网关插件（16 文件）

- `scripts/`：11 个文件（`smoke_*` 8、`migrate_module_rename.py`、`sample_region_presentation.py`、
  `install_skill.py`/`kanban_notify_handover.py`）切新名；`smoke_*` 的「跑法」注释里的 shim 路径同步。
  `migrate_module_rename.py` 的 docstring 示例与 `NEXT:` 提示改为本次迁移（旧名 → 新名），工具逻辑未改。
- `gateway_plugins/`：`queued-lane-interim-thin/tests/test_plugin.py`、两份 README、`steer-presentation-thin`
  的 `patches.py` 注释与 README —— 都是注释/README/测试 shim，运行时确认 gateway 侧不按模块名 import，
  **不需要重启 gateway**。

### 1.5 `verify.py`（+109 / −4，22 → 25 项）

新增三道闸：

| 闸 | 判据 |
|---|---|
| 新名七处一致（含版本 0.2.0） | `pyproject.name` / `version` / `[tool.nonebot].plugins` / `packages.find.include` / `package-data` 键 / `setup.py` 守卫 / `config.PLUGIN_MODULE_NAME` / `tests/_bootstrap.py` / `__init__.__version__` |
| 旧名仅在白名单处保留 | 活文件逐文件计数，**多出、数量不符、白名单未命中**都判 FAIL（历史 `EVIDENCE*/evidence_*/DELIVERY*/review_t_*/SAMPLES/` 不参与） |
| 旧名 import 被阻断且新名解析到本工作树 | 引导器装上后旧名必须 `ModuleNotFoundError`；新名 `__path__` 必须含本仓库根 |

## 2. 保留项（本卡刻意不动）

| 类别 | 现状 | 理由 |
|---|---|---|
| 远端仓库 URL | `pyproject` Homepage、`__init__` homepage、README badge 三处仍是旧仓库路径，各带 `TODO(D3)` | 远端改名未批准；改名后三处一起切 |
| 上游归因 | `Upstream` URL、LICENSE 上游版权行、README「来源与归属」 | 上游 gsskk 的署名不能改写成 fork 自己 |
| `hermes_*` Python 字段 / `AGENT_*` ENV | 514 / 91 处 | 已是脱敏后的对外契约（`AGENT_*` 收口，字段名维持内部 API 形态） |
| `[HERMES]` 日志前缀 | 全部保留；只改插件加载行 `Agent Adapter loaded` | 运维检索锚，单独立项（D5） |
| `HermesClient` / `core/hermes_client.py` | 未改 | 三后端共用的上游 HTTP 客户端，改名会牵动 handlers/tasks/网关对照（报告 §8） |
| 历史证据 | `EVIDENCE*/evidence_*/DELIVERY*/review_t_*/SAMPLES/`、`*.log` 一字未改 | 当时的真实输出 |
| 本机 skills | `~/.hermes/skills/**` 95 文件未动 | 卡边界：另卡处理（需要同步的常量见 §7.3） |

**脱敏后的中性标识**保持中性，且**没有回退 hermes、也没有改写成新插件名**：ACP `clientInfo.name`
仍是 `nonebot-acp-client`，ACP cwd 仍是数据目录同级的 `acp-workspace/`（无数据目录时 `<tmp>/nonebot_acp/acp-workspace`）。
改名没有理由把插件身份重新写进对外协议；`tests/test_module_rename.py` 与 `tests/test_antigravity_acp.py`
双向断言：既不含旧名、也不含新名、也不含 `hermes`。

## 3. 测试矩阵（全部重定向 `HOME`/`XDG_*`/`TMPDIR` 到 scratch 沙箱，`-B` + `PYTHONDONTWRITEBYTECODE=1`）

| 套件 | 命令 | 结果 | 原始输出 |
|---|---|---|---|
| focused | `-m unittest tests.test_module_rename -v` | **Ran 7, OK** | `EVIDENCE_T_4e54acd6/01_focused.log` |
| related | `test_module_rename` + `test_antigravity_acp` + `test_evidence_probe_readonly` + `test_queued_lane_interim_client` + `test_user_facing_neutral_text` + `test_antigravity_mcp_files` + `test_antigravity_stream_steer` | **Ran 105, OK (skipped=1)** | `02_related.log` |
| 全量 | `-B -m unittest discover -s tests -t . -q` | **Ran 901, OK (skipped=1)**，48.1s | `03_full.log` |
| turn | `-m unittest tests._bootstrap turn_tests.test_envelope_entry -q` | **Ran 6, OK** | `04_turn.log` |
| 网关 `queued-lane-interim-thin` | Hermes venv、中立 cwd | **Ran 48, OK** | `05_gateway_queued.log` |
| 网关 `steer-presentation-thin` | 同上 | **Ran 21, OK** | `06_gateway_steer.log` |
| `verify.py` | 干净树 | **ALL PASS: 25/25** | `07_verify.log` |

- 基线（`944cc50`，改名前提）实测 **894 全绿**；改名后 **901 全绿**（+7 即新回归），失败集不增、跳过数不变（1）。
- 旧包遮罩探针（`08_old_name_mask_probe.log`）：不装引导器时 `find_spec('nonebot_plugin_hermes_adapter')`
  **命中** `~/.local/NeonBot/.venv/.../site-packages/nonebot_plugin_hermes_adapter/__init__.py`（遮罩风险真实存在）；
  装 `tests._bootstrap` 后同一 `find_spec` 抛 `ModuleNotFoundError`，而新名 `__path__` 指向本工作树。
- 生产数据/缓存目录未被写入：`~/.local/share/nonebot2/` 与 `~/.cache/nonebot2/` 下**只有旧名目录**，
  没有出现 `nonebot_plugin_agent_adapter` 目录；沙箱里则如预期生成了新名目录（重定向生效）。
- smoke 脚本（`scripts/smoke_*.py`，无 CI 覆盖）逐个跑：`smoke_chat_identity_at` 22 项、
  `smoke_task_anchor_switch` 44、`smoke_chat_config` 38、`smoke_misc_media` 86、`smoke_prompt_dump` 34、
  `smoke_group_profile` 20、`smoke_message_identity_mapping` 56 全过。
  **`smoke_config_templates` 1 项既有红**（`新 Config 仅 8 个启动键 | got 11`）：在基线 `944cc50` 上复跑同样红，与改名无关。

## 4. 发布物（scratch，未上传）

复用既有 `release_sync.py` → 隔离 `build` → 审计 → 隔离安装的流程。脚本与常量在 scratch 副本上改（`~/.hermes/skills` 未动，见 §7.3）：

| 步骤 | 结果 | 证据 |
|---|---|---|
| 暂存区 | `root_files=11  package_files=223  hash_verified=234  skipped_denied=17  skipped_symlinks=0`（逐文件 sha256 双向核对） | `11_release_sync.log` |
| 构建 | 隔离 venv（`python -m build`，1.6.1）→ `Successfully built nonebot_plugin_agent_adapter-0.2.0-py3-none-any.whl` | `12_build.log` |
| 审计 | 218 条目 / 212 `.py` / **1,019,442 B**；`METADATA Name=nonebot-plugin-agent-adapter`、`Version=0.2.0`；`top_level.txt=[nonebot_plugin_agent_adapter]`；`RECORD` 覆盖全部文件且大小与 sha256 全部一致；`skill/SKILL.md` 随包；包内无 EVIDENCE/DELIVERY/SAMPLES/`__pycache__`/`*.pyc`/`*.db`/日志；无 console script；载荷旧名仅在 §6 白名单 6 处 | `09_wheel_audit.log` |
| 空 wheel 防呆 | 在开发树根直接 `python -m build --wheel` 被 `setup.py` 守卫拒绝（exit 1，`找不到标准包目录 nonebot_plugin_agent_adapter`），未产出任何 wheel | `13_empty_wheel_guard.log` |
| 隔离安装 | 独立 venv 装该 wheel → `load_plugin("nonebot_plugin_agent_adapter")` 返回插件、`id_`/显示名 `Agent Adapter`/版本 `0.2.0` 正确；`importlib.metadata` 认新分发名；四个子模块可导入；包路径命中该 venv 而非仓库 | `10_wheel_install_check.log` |
| 校验和 | `sha256 c00372237e234c8dc175c9ad234b7062cc47040c0f3c41cb3fea0f4156242f0f`（1,019,442 B） | `16_wheel_sha256.txt` |

**未上传**：没有 PyPI、没有实例 `incoming/`、没有远端推送。

## 5. 静态闸与旧名白名单（`verify.py` 逐处计数，多一处/少一处都 FAIL）

| 文件 | 模块名 | 分发名 | 理由 |
|---|---|---|---|
| `README.md` | 0 | 1 | D3：badge 仍指旧仓库路径（远端未改名） |
| `__init__.py` | 0 | 1 | D3：`homepage` 仍指旧仓库 URL |
| `pyproject.toml` | 0 | 1 | D3：`Homepage` 仍指旧仓库 URL |
| `tests/_bootstrap.py` | 1 | 0 | 旧名阻断器的唯一字面量真源 |
| `tests/test_queued_lane_interim_client.py` | 1 | 0 | 生产冷迁移未做的部署路径兜底 |
| `tests/test_evidence_probe_readonly.py` | 1 | 0 | 历史证据探针的加载时改名桥 |
| `gateway_plugins/queued-lane-interim-thin/tests/test_plugin.py` | 1 | 0 | 生产冷迁移未做的部署路径兜底 |
| `verify.py` | 1 | 1 | 旧名闸自己的字面量真源 |

## 6. 与评估报告的差异（报告基线早于脱敏合回，按当前 `944cc50` 源码为准）

1. **报告 §2.3 的 `backends/antigravity/client.py` 两条改名已由脱敏卡完成且方向不同**：`clientInfo` 改成中性
   `nonebot-acp-client`、cwd 改成中性 `acp-workspace`（不再是从旧名改新名）。本卡遵循「保持中性、不回退」，
   没有把它改成新插件名，并用回归锁住三个方向都不许出现。
2. **报告 §2.2 的「Hermes Agent API Server」文案处理**：实际已由脱敏卡把 `/ping`、`/help` 改成「AI 助手」；
   本卡把 `__plugin_meta__` 描述与 usage 对齐同一口径（保留「Hermes 后端」在 README 与配置说明里的指称）。
3. **报告 §7.4 的「wheel 无 tests/scripts 内部路径」并非当前打包事实**：`packages.find` 会收包目录下所有
   子包，**实测 wheel 含 `tests/`、`scripts/`、`gateway_plugins/`、`turn_tests/`、`verify.py`**（212 个 `.py`）。
   既有 0.1.0 wheel（`wheel-883a473`）同样如此，属既有打包形态；本卡**不擅自改打包边界**（那是"顺手改发布面"），
   只在轮子里挡掉历史证据目录并把这个事实写进复核项（§9 Q2）。
4. **暂存区原本会带历史证据目录**：`release_sync.py` 的 `DENY_DIR_NAMES` 不含 `EVIDENCE*/DELIVERY*/review_t_*/SAMPLES`，
   而这些目录里有 `.py`（取证探针），会被 `packages.find` 收进 wheel，把旧名与内部路径一起发出去。
   本卡在 scratch 副本里补了 `DENY_DIR_PREFIXES`（目录 + 文件两种形态），暂存区 `skipped_denied=17`。
5. **`misc_config.example.yaml` 与 `core/config_templates.py` 在基线就漂移**（注释头「10 键」vs「9 键」、一段注释换行）。
   `release_sync.py --require-clean` 前的 example 校验因此**在基线同样失败**。本卡在 scratch worktree 里用
   `--emit-examples` 重生成后再组暂存区（**没有把该重生成写回仓库**；example 不进 wheel，发布物不受影响）。
   是否把重生成提交进仓库，留给发布/上传卡决定。
6. **脱敏残留扫描不是零**，且与改名无关：源树 4405 命中（绝大多数在历史 `DELIVERY*/EVIDENCE*` 里），
   活文件 66；暂存区 66，与基线活文件 66 **逐文件一致**（`14/15/17` 三份 digest）。说明更名既没引入也没消除残留；
   `主人`/主机路径这些既有内容属于脱敏 apply 的门，不在本卡范围。

## 7. 未做项（明确交接）

### 7.1 生产冷迁移（另一窗口，需停服批准）

两个实例各四件事：包目录 `site-packages/nonebot_plugin_hermes_adapter` → 新名、`dist-info`
（`Name`/`top_level.txt`/`RECORD` 重建）、数据目录、缓存目录；外加实例 `pyproject.toml` 依赖名与插件声明两行。
`scripts/migrate_module_rename.py` 准入检查已就绪（WAL/SHM/journal 存在即拒、目标存在即拒）。
迁移后务必：
- 删掉 §1.3 两处部署路径兜底里的**旧名条目**（否则对照永远走旧目录）；
- 启动验收的日志行改成 **`Agent Adapter loaded`**（不再是 `Hermes Plugin loaded`）；
- `pip check`、`pip list` 无旧名、site-packages 无旧目录/旧 dist-info。

### 7.2 其它

上传/PyPI 首发、远端仓库改名（含 §5 三处 URL 与 `TODO(D3)`）、NoneBot 商店提交、`[HERMES]` 前缀与
`HermesClient`/`hermes_client.py` 改名 —— 都不在本卡，也未做。

### 7.3 本机 skills 95 文件（另卡）需要跟着改的点

本卡**没有写** `~/.hermes/skills/**`。为跑通发布流程，在 scratch 副本（`~/.hermes/profiles/coder_deepseek/cache/scratch/aa-rename/base/release-tools/`）上做了等价修改，正式副本需要同样处理：

| 文件 | 需要的改动 |
|---|---|
| `scripts/release_sync.py` | `DIST_NAME`/`PKG_NAME` → 新名；`DENY_DIR_PREFIXES` 挡 `EVIDENCE/evidence_/review_t_/DELIVERY/SAMPLES`（目录与文件两种形态）；布局 docstring |
| `scripts/scan_changed_residual.py` | `PUBLIC_NAMES` **留旧＋加新**；包前缀归一化改成按多个公开模块名匹配（否则暂存区布局认不出豁免文件） |
| `scripts/sanitize_plugin_snapshots.py` | `RULES` 加 R10c（`nonebot_plugin_agent_adapter_backup` 两形态，旧 R10/R10b 保留）；`RESIDUAL` 加新备份目录名 |
| `scripts/nph_test_bootstrap.py`、`smoke_release_tree.py`、`smoke_module_rename.py`、`dev_sync.sh`、`SKILL.md`＋references、其余 smoke 脚本 | 按报告 §2.7 一次切新名（86 个脚本仍含旧名） |

## 8. 复核要点（给 GPT）

1. **七处一致**是否漏点：`verify.py` 的三道新闸只读字面量，`tests/test_module_rename.py` 用 `tomllib` 复核；
   两者都过。若认为还该把 `README` 分发名/模块名纳入闸，请指出——目前由 `01_focused`/`07_verify` 之外的人工 diff 保证。
2. **打包边界**：wheel 里含 `tests/`、`scripts/`、`gateway_plugins/`、`turn_tests/`、`verify.py`。这与既有 0.1.0
   发行物同形，但报告 §7.4 期望"不含"。要不要在更名这一版顺手把测试/脚本排出发行物？**这会改变发行面，需主人拍板**，
   且会让"生产按各实例现有代码纯改名、不重铺 wheel"的策略失去对照基线。
3. **旧名兜底两处**（生产部署路径）：现在新目录优先、旧名兜底。请确认这个写法在"生产迁移当天"是可接受的过渡形态，
   而不是"双导入兼容层"（它只影响跨树对照测试，不影响产品 import 面）。
4. **历史探针加载桥**：`tests/test_evidence_probe_readonly.py` 在加载时把探针源码的旧名重写成新名。另一种选择是
   让该用例 skip。请确认"改写加载"优于"丢覆盖"。
5. **`misc_config.example.yaml` 漂移**（§6.5）：本卡没提交重生成结果，发布物不受影响；是否现在补一次 example 重生成提交。
