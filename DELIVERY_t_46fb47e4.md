# DELIVERY t_46fb47e4 — 发布前收口（打包边界 + 干净构建前置，不部署／不上传）

本卡范围：确认 Agent Adapter 0.2.0 的公开发行边界、修复干净构建前置、重跑发布物与回归。
**未部署、未上传、未改生产、未装进运行 venv。** 本机发布工具在主人单独授权后已同步（§6）。
`rename_approved=true` 不等于 `upload_approved=true`：本卡结论 `release_ready=false`。

- 基线：`0ad6c43`（更名候选）
- 分支：`feat/agent-adapter-release`
- 第一轮代码 HEAD：`424bd55`；打包边界与交付说明提交：`6ba179d`

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
| 全树脱敏扫描：源／暂存区／wheel 分别统计 | 通过：暂存区与 wheel 均 0 命中，源树命中全在历史证据／交付与开发文件（§4、§5） |
| 本机发布工具同步（`~/.hermes/skills/**`） | 主人授权后完成，备份与 sha256 见 §6 |
| 上传 | 未授权、未执行 |

## 2. 发行边界：三道闸

主人 2026-11-14 定案：公开 wheel 只保留插件运行需要的安装文件；开发文件留在源码仓库；
两个生产实例按各自现状纯改名，不重铺这个 wheel。

1. **发布暂存**：`release_sync.py` 的 `DEV_ONLY_PATHS = {tests, scripts, gateway_plugins, turn_tests, verify.py}`
   在复制前拒收（按相对路径首段匹配）。补丁先作为交付物落在
   `scripts/release_tooling/release_sync_public_surface.patch`
   （基线 sha256 `5db9f6f0…`、行数 368 → 补丁后 `276b7e27…`、375 行）；
   主人授权后已同步进 `~/.hermes/skills/**`，本轮构建直接用同步后的正式副本。
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
   118 条目 / 112 `.py` / 1 个 dist-info。本轮重建同为 589,840 B / 118 条目 / 112 `.py`，
   解包内容与第一轮逐文件一致（`diff -r` 为空）；**容器 sha256 会随 zip 时间戳变化**
   （实测 `25a1b58c…`、`f686d84e…` 两个容器哈希对应同一解包内容），载荷指纹以解包内容与
   RECORD 为准，不要拿容器哈希当可复现指纹。
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

工具：同步后的 `scan_changed_residual.py --tree`（角色词「主人」2026-11-14 起整体放行；
本机路径、真号、内部文档名、NeonBot／.venv 等照扫）。

| 目标树 | 命中 | 豁免 | 命中文件 | 分类 |
|---|---|---|---|---|
| 源码树 HEAD（含本交付文件） | 4570 | 0 | 158 | 历史证据/交付 4531、开发文件 39、运行面 0 |
| 发布暂存区 | 0 | 0 | 0 | 126 文件 |
| 最终 wheel 解包 | 0 | 0 | 0 | 118 文件 |

源码树命中的 token 面：`/home/neonsky` 3479、`nph-20-work` 629、`NeonBot` 145、
`1581124503` 145、`.venv` 132、`2968545011` 15、`462540549` 7、`kimi` 7、`253384307` 5 ——
**全部落在历史证据/交付与开发文件，这两类都不进暂存区、不进 wheel**。wheel 里已无个人
主机路径、无真号、无内部文档名、无署名。

绝对闸按实际发布物判定：暂存区与 wheel 均 0 命中。没有把「相对基线没有新增」当上传闸通过；
也没有删任何历史 EVIDENCE 去追零——历史目录逐字未动，源树扫描如实计入。

## 5. 角色词「主人」：主人选 A，已落地

第一轮 wheel 剩 15 处命中，全是 `主人`，分布在 `core/user_lists.py`(5)、
`core/runs_text.py`(2)、`core/control_intent.py`(1)、`core/inline_media_cap.py`(1)、
`core/message_origin.py`(1)、`handlers/message_turns.py`(1)、`dist-info/METADATA`(4，来自 README 正文)；
外加 3 处 `core/prompt_builder.py` 的既有豁免。

关键事实：`core/prompt_builder.py:126` `_IDENTITY_MASTER = "主人"` 是**运行时身份栏标签**，
README 里 `[有人@我] [m:123] [主人] [id=…]` 正是产品实际渲染形态。这些 `主人` 是产品
词汇，不是个人身份信息。

主人 2026-11-14 拍板**选 A**：把「主人」作为可发布角色词放行，保持运行时 `[主人]`、
README 示例和 docstring 一致，不改写成「维护者」。落地：

- 仓库与 wheel 不动，`wheel 载荷 == 源树逐字节` 的不变式保持。
- 发布工具按授权同步：`RESIDUAL` 去掉「主人」、sanitize 不再替换、扫描器加
  `PUBLISHABLE_ROLE_WORDS`、`EXEMPT_HITS` 清空、`INTERNAL_TOKENS` 去掉「主人」（§6）。
- 其他本机路径、账号、内部文档名、NeonBot 等敏感项继续扫描；奇怪注释所在的开发文件
  按定案排除出 wheel，不靠豁免把它们带进安装包。
- B 方案（改写「维护者」）留作对照，未采用：会让 README 样例与运行时输出不一致。

## 6. 本机发布工具同步（2026-11-14，主人单独授权）

同步目标 `~/.hermes/skills/nonebot-hermes-plugin-modding/`；改动前备份在任务工作区
`skills_backup_20261114T2020/`（含 `MANIFEST.sha256`），明细同时写入该技能的
`references/release-tooling-sync.md`。

| 文件 | 前 sha256 → 后 sha256 | 变化 |
|---|---|---|
| `scripts/release_sync.py` | `5db9f6f0…` → `276b7e27…` | `DEV_ONLY_PATHS`，与仓库补丁逐字一致 |
| `scripts/scan_changed_residual.py` | `b80bbd90…` → `fa88b2df…` | `RESIDUAL` 去「主人」；`PUBLISHABLE_ROLE_WORDS`；`EXEMPT_HITS` 清空 |
| `scripts/sanitize_plugin_snapshots.py` | `c2acb96d…` → `74fc4d03…` | 不再改写「主人」；`RESIDUAL` 去「主人」 |
| `scripts/smoke_release_tree.py` | `1863a1b4…` → `fd291f67…` | 去 `scripts` oracle／七键／`INTERNAL_TOKENS` 去「主人」／开发文件反向断言 |
| `scripts/smoke_sanitize_r8.py` | `d2cce35e…` → `5e0e0d62…` | 按角色词放行口径重写 |
| `references/support-files.md`、`sanitized-history-upload.md` | `8969b9f0…`／`69c1f88e…` → `63fb1692…`／`86bc5bf5…` | 同步口径与索引 |

同步后回归：

- `smoke_sanitize_r8.py`：15 PASS／0 FAIL。
- `smoke_release_tree.py`（`NPH_WORK=<干净暂存区>`）：95 PASS／0 FAIL，含新增
  「开发文件（tests/scripts/gateway_plugins/turn_tests/verify.py）不进入暂存区」
  「真实树暂存区不含开发文件」两项反向断言。
- `release_sync.py --require-clean`：exit 0，`skipped_denied=130`（比第一轮多 1，是本卡
  自己的 `DELIVERY_t_46fb47e4.md` 按历史交付前缀被拒收，属预期）。

`release_sync_public_surface.patch` 仍留在仓库作为交付证据；应用后 sha256 应为
`276b7e27…`，本机同步件与此逐字一致。

## 7. 回归（本轮 HEAD `6ba179d` 之后重跑，隔离 HOME/XDG/TMPDIR，`-B`）

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
| `6ba179d` | 交付说明与第一轮实测数据 |
| 本轮 `docs(delivery)` 提交 | §4–§6 收口数据、角色词放行结论与本机 skills 同步记录 |

## 9. 明确没做的事

- 未部署、未重启、未停服；未对两个生产实例做任何迁移。
- 未上传 PyPI/GitHub；未改远端仓库名；未推分支。
- 未把 wheel 装进任何运行 venv（只装进 scratch 的 `--target` 目录）。
- 本机 skills 仅在主人单独授权后同步发布工具（§6）；未动其他 profile，未部署。
- 未改历史 `EVIDENCE*/DELIVERY*/SAMPLES/review_t_*`，未删证据追零。

## 10. 复现命令（关键三步）

```bash
WS=<scratch>; R=<repo>
# 1) 干净提交 → 暂存区（同步后的正式 release_sync）
HOME=$WS/env/home XDG_DATA_HOME=$WS/env/xdg_data XDG_CACHE_HOME=$WS/env/xdg_cache \
TMPDIR=$WS/env/tmp PYTHONDONTWRITEBYTECODE=1 \
python -B <skill>/scripts/release_sync.py --require-clean --source $R --out $WS/staging
# 2) 隔离构建 + 审计
(cd $WS/staging && python -B -m build --wheel --no-isolation --outdir $WS/dist)
python -B $R/scripts/audit_public_wheel.py $WS/dist/*.whl --expect-version 0.2.0
# 3) 脱敏扫描（源／暂存区／wheel 解包各一次）
NPH_WORK=<目标树> python -B <skill>/scripts/scan_changed_residual.py --tree
# 4) 发布骨架回归（干净暂存区；含开发文件不进暂存区的反向断言）
NPH_WORK=$WS/staging python -B <skill>/scripts/smoke_release_tree.py
```
