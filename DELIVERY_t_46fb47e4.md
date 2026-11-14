# DELIVERY t_46fb47e4 — 发布前收口（打包边界 + 干净构建前置，不部署／不上传）

本卡范围：确认 Agent Adapter 0.2.0 的公开发行边界、修复干净构建前置、重跑发布物与回归。
**未部署、未上传、未改生产、未装进运行 venv、未写 `~/.hermes/skills/**`。**
`rename_approved=true` 不等于 `upload_approved=true`：本卡结论 `release_ready=false`。

- 基线：`0ad6c43`（更名候选）
- 分支：`feat/agent-adapter-release`
- 本文件提交前的代码 HEAD：`424bd55`

## 1. 结论

| 项 | 状态 |
|---|---|
| 公开 wheel 只含运行包（tests/scripts/gateway_plugins/turn_tests/verify.py 排除） | 完成，三道闸 + 反向断言实测 |
| `misc_config.example.yaml` 模板漂移 | 完成，按 `core/config_templates.py` 重生成并提交（仅注释差异） |
| `migrate_module_rename.py` 示例来源路径 | 完成，改为本次实际来源 `nonebot_plugin_hermes_adapter`，工具逻辑未动 |
| README 精确名称行纳入 verify 名称闸 | 完成，D3 旧 URL 例外保留，检查项仍 25 |
| `release_sync.py --require-clean` | 现在 exit 0（基线因 example 漂移 exit 2） |
| 隔离 build／安装／`load_plugin`／RECORD／hash | 全过 |
| 回归 901(skip1)／turn 6／网关 48+21／verify 25 | 全过 |
| 全树脱敏扫描：wheel 绝对闸 | **未绿：15 命中，全部是产品角色词「主人」**（详见 §6） |
| 上传 | 未授权、未执行 |

## 2. 发行边界：三道闸

主人 2026-11-14 定案：公开 wheel 只保留插件运行需要的安装文件；开发文件留在源码仓库；
两个生产实例按各自现状纯改名，不重铺这个 wheel。

1. **发布暂存**：`release_sync.py` 的 `DEV_ONLY_PATHS = {tests, scripts, gateway_plugins, turn_tests, verify.py}`
   在复制前拒收（按相对路径首段匹配）。该脚本住在 `~/.hermes/skills/**`，本卡**没有写**，
   以补丁形式交付：`scripts/release_tooling/release_sync_public_surface.patch`
   （基线 sha256 `5db9f6f0…`、行数 368 → 补丁后 `276b7e27…`、375 行）。本卡用 scratch 副本
   （同 sha256）跑构建。
2. **pyproject**：`[tool.setuptools.packages.find] exclude` 排除四个子包，暂存区即使被手工污染
   也不会把 dev 子包收进 wheel。
3. **成品 wheel 反向断言**：`scripts/audit_public_wheel.py`（新增，开发文件，不进 wheel）核
   无 dev 目录／无历史证据／无缓存字节码数据库日志／RECORD 全覆盖 sha256+大小／元数据／
   top_level，同时正向断言运行载荷齐全（防排除过头产出空壳）。

数据对比（同一构建流程）：

| | 基线 0ad6c43 | 本卡 424bd55 |
|---|---|---|
| 暂存区文件 | 234（root 11 + pkg 223） | **126（root 11 + pkg 115）**，denied 129 |
| wheel 条目 | 218 | **118** |
| wheel `.py` | 212 | **112** |
| wheel 字节 | 1,019,442 | **589,840** |
| 少掉的 100 条 | — | tests 67 + scripts 16 + gateway_plugins 15 + turn_tests 1 + verify.py 1 |

## 3. 干净构建链（隔离环境实测）

环境：`HOME`／`XDG_DATA_HOME`／`XDG_CACHE_HOME`／`TMPDIR` 全部指向本卡 scratch；
`-B` + `PYTHONDONTWRITEBYTECODE=1`；构建在暂存区目录、`--no-isolation` 的隔离 build venv
（`build 1.6.1` + `setuptools 84.2.1` + `wheel 0.48.0`，离线安装）；网关测试中立 cwd + 隔离 `HERMES_HOME`。

1. 干净提交 → `release_sync.py --require-clean`：`exit 0`，
   `root_files=11 package_files=115 hash_verified=126 skipped_denied=129 skipped_symlinks=0`。
2. 暂存区构建 wheel：`Successfully built nonebot_plugin_agent_adapter-0.2.0-py3-none-any.whl`。
3. 产物：589,840 B，`sha256 d98a18b0ea232a8ccbb42b0ff9b7aac05fffc531f26f4d98b511e9eb489f438a`，
   118 条目 / 112 `.py` / 1 个 dist-info。
4. `audit_public_wheel.py`：11 项全 PASS（重复成员/路径穿越、顶层只有运行包与 dist-info、
   运行载荷齐全、无 dev 文件、无历史证据/交付/样例、无缓存/venv/.git、无字节码/数据库/日志/
   临时文件、dist-info 唯一、METADATA Name=nonebot-plugin-agent-adapter Version=0.2.0、
   top_level 唯一、RECORD sha256/大小全对）。
5. 隔离安装：`pip install --no-index --no-deps --target <scratch>/site_v2 <wheel>` 成功。
6. 真 `load_plugin`（不走 `tests/_bootstrap`、cwd 中立、隔离 HOME）：
   `plugin.name=nonebot_plugin_agent_adapter`、显示名 `Agent Adapter`、
   `importlib.metadata` 认 `nonebot-plugin-agent-adapter 0.2.0`、`module.__version__=0.2.0`、
   `core/handlers/mcp/tasks/backends` 可导入、`skill/SKILL.md` 随包、模块路径落在隔离安装目录。

## 4. 全树脱敏扫描（源／暂存区／wheel 分别统计）

工具：`scan_changed_residual.py --tree`（既有规则，含公开名掩码与 prompt_builder「主人」豁免）。

| 目标树 | 豁免外命中 | 豁免 | 命中文件 | 分类 |
|---|---|---|---|---|
| 源码树 HEAD | 4739 | 3 | 176 | 历史证据/交付 4665、开发文件 56、运行面 18 |
| 发布暂存区 | 18 | 3 | 8 | 全部 `主人`（包内 11、README 4、user_lists.example 3） |
| 最终 wheel 解包 | 15 | 3 | 7 | 全部 `主人`（包内 11、METADATA 4） |

源码树命中的 token 面（说明敏感内容的实际位置）：`/home/neonsky` 3478、`nph-20-work` 628、
`主人` 182、`1581124503` 144、`NeonBot` 142、`.venv` 130、`2968545011` 14、`462540549` 6、
`kimi` 6、`253384307` 4 —— **除 `主人` 外全部落在历史证据/交付（4665）与开发文件（56），
这两类都不进暂存区、不进 wheel**。wheel 里已无个人主机路径、无真号、无内部文档名、无署名。

**没有把「相对基线没有新增」当上传闸通过**：wheel 扫描 exit 1，绝对闸未绿，原因见 §6。
**也没有删任何历史 EVIDENCE 去追零**：历史目录逐字未动（源树扫描仍如实计入）。

## 5. 未决项：wheel 里只剩产品角色词「主人」

现象：wheel 15 处命中全是 `主人`，分布在
`core/user_lists.py`(5)、`core/runs_text.py`(2)、`core/control_intent.py`(1)、
`core/inline_media_cap.py`(1)、`core/message_origin.py`(1)、`handlers/message_turns.py`(1)、
`dist-info/METADATA`(4，来自 README 正文)；外加 3 处 `core/prompt_builder.py` 的既有豁免。

关键事实：`core/prompt_builder.py:126` `_IDENTITY_MASTER = "主人"` 是**运行时身份栏标签**，
README 里 `[有人@我] [m:123] [主人] [id=…]` 正是产品实际渲染形态。所以这些 `主人` 是产品
词汇，不是个人身份信息。

两条路，代价不同，需要主人拍板：

- **A. 判为可发布词汇**（推荐）：把「主人」并入既有角色词豁免（与 prompt_builder 豁免同源）。
  代价：`scan_changed_residual.py` / `sanitize_plugin_snapshots.py` 的 `RESIDUAL`+`EXEMPT_HITS`
  与 `smoke_release_tree.py` 的 `INTERNAL_TOKENS` 要改，属发布工具同步面（需单独授权）。
  仓库与 wheel 不动，`wheel 载荷 == 源树逐字节` 的不变式保持。
- **B. 按 R8 改写**：注释/docstring/README 里 `主人`→`维护者`。已实测可行：
  10 个文件 21 行，改写后暂存区扫描 0 命中、wheel 扫描 0 命中（见附件 `r8_variant.diff`）。
  代价：README 的渲染样例会变成 `[维护者]`，而运行时仍输出 `[主人]`；docstring 与紧邻的
  `_IDENTITY_MASTER` 常量不一致（文档失真）。

本卡没有擅自选 B，也没有拿 A 当已批准；两种都留了可复现证据。

## 6. 发布工具依赖（正式同步属另一授权面）

- `release_sync_public_surface.patch`：应用后 sha256 应为 `276b7e27…`；基线不符时按补丁三处
  语义重落，不要硬打。
- **发布套件未同步**：`smoke_release_tree.py` 的 oracle 还认为 `scripts/` 属于发布面
  （`REAL_PKG_TOP`、`is_publishable`），会把本次边界判为失败。需在同一次工具同步里加
  `DEV_ONLY_PATHS` 并去掉 `scripts`。本卡未改该文件，故实测它报
  `真实树包目录含全部源码面 | missing=['scripts']`。
- 该套件在干净暂存区上共 4 项失败：`user_lists.example.yaml 六键齐全`（模板现为 7 键、
  套件硬编码 6，**先于本卡存在**，`core/config_templates.py` 本卡未动）、
  `README 不含内部主机/路径/代号 | leaked=['主人']`（**先于本卡**，README 本卡未动）、
  上述 `missing=['scripts']`（本次边界导致，需套件同步）、`目标树 --tree 零残留`（即 §5）。
- 即：正式发布门禁要绿，需要 (1) §5 的主人拍板、(2) 发布套件 oracle 同步、(3) 六键漂移修复。

## 7. 回归（HEAD 424bd55，隔离 HOME/XDG/TMPDIR，`-B`）

| 入口 | 结果 |
|---|---|
| `unittest discover -s tests -t .` | 901 OK（skip 1），48.0s |
| `unittest discover -s turn_tests -p 'test_*.py'` | 6 OK |
| 网关 `queued-lane-interim-thin` | 48 OK |
| 网关 `steer-presentation-thin` | 21 OK |
| `verify.py` | 25/25 ALL PASS（含新增 README 精确声明项） |

## 8. 提交清单

| 提交 | 内容 |
|---|---|
| `9c26937` | 按 `core/config_templates.py` 重生成 `misc_config.example.yaml`（仅注释：10→9 键、一行注释合并） |
| `45eb0b2` | `migrate_module_rename.py` 示例来源改 `nonebot_plugin_hermes_adapter`（7/9/11 行）；verify 旧名白名单登记该文件 3 处 |
| `f7d3019` | verify 名称闸加 README 分发名/模块名精确行（D3 URL 例外保留，仍 25 项） |
| `424bd55` | 打包边界：`pyproject` exclude + `audit_public_wheel.py` + `scripts/release_tooling/`（补丁与说明）+ migrate extra 注释 |

## 9. 明确没做的事

- 未部署、未重启、未停服；未对两个生产实例做任何迁移。
- 未上传 PyPI/GitHub；未改远端仓库名；未推分支。
- 未把 wheel 装进任何运行 venv（只装进 scratch 的 `--target` 目录）。
- 未写 `~/.hermes/skills/**`（发布工具以补丁交付）。
- 未改历史 `EVIDENCE*/DELIVERY*/SAMPLES/review_t_*`，未删证据追零。

## 10. 复现命令（关键三步）

```bash
WS=<scratch>; R=<repo>
# 1) 干净提交 → 暂存区（补丁后的 release_sync 副本）
HOME=$WS/env/home XDG_DATA_HOME=$WS/env/xdg_data XDG_CACHE_HOME=$WS/env/xdg_cache \
TMPDIR=$WS/env/tmp PYTHONDONTWRITEBYTECODE=1 \
python -B <release_sync_patched>.py --require-clean --source $R --out $WS/staging
# 2) 隔离构建 + 审计
(cd $WS/staging && python -B -m build --wheel --no-isolation --outdir $WS/dist)
python -B $R/scripts/audit_public_wheel.py $WS/dist/*.whl --expect-version 0.2.0
# 3) 脱敏扫描（源／暂存区／wheel 解包各一次）
NPH_WORK=<目标树> python -B <skill>/scripts/scan_changed_residual.py --tree
```
