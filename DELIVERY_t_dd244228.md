# DELIVERY t_dd244228 — ACP 沙箱返修：统一启用并跟随 agy 持久设置

分支 `wt/t_dd244228`，基于候选分支 `wt/t_57f77786`（HEAD `3a9369f`，基线 `dev=28b95c3`）。
本卡按主人 2026-11-16 最新定案返修：**沙箱在插件 ACP 侧常开、没有关闭分支**，
覆盖旧口径「缺省零变化／仅 argv 含 `--sandbox` 时启用」。只碰 antigravity ACP 后端与其测试；
未合并、未部署、未上传，未跑真实 agy 模型（真实 agy 验收由主会话负责）。

## 1. 行为契约（现行）

| agy 持久设置 `enableTerminalSandbox` | `AGENT_ANTIGRAVITY_ARGS` | 实际 argv | 沙箱来源 |
| --- | --- | --- | --- |
| `true` | 不含 `--sandbox` | 不追加 `--sandbox` | 复用 agy 自带沙箱 |
| `false`／缺键／文件缺失／坏 JSON | 不含 `--sandbox` | 追加一项 `--sandbox` | 插件补 flag |
| 任意 | 显式含 `--sandbox` | 原样保留（不重复追加） | 显式 argv |
| `true` | 含 `--sandbox=false` | 追加一项 `--sandbox`（后者生效） | 插件补 flag |

两种来源下**同样**：启动时创建 `~/.cache/agent-adapter/acp-workspace`、在原 ACP workspace 内
重写受控 `BASH_ENV`（内容只有一行 `cd <缓存工作区>`）、始终把 `BASH_ENV` 传给 agy 子进程、
系统提示尾部始终追加一句实际工作目录说明。

不变项：`session/new`／`session/resume` 的 `cwd` 仍是原 ACP workspace；缓存目录**不进**
`additionalDirectories`；**只读** agy settings（不改全局设置、不写 `enableTerminalSandbox`）；
不自动设置 `toolPermission`；不启用 `--dangerously-skip-permissions`；不新增沙箱专用配置开关
（`AGENT_ANTIGRAVITY_ARGS` 仍只是通用 argv 入口）。

持久设置路径按 agy 自己的目录口径解析：默认 `~/.gemini/antigravity-cli/settings.json`；
`--gemini-dir`／`--app-data-dir`（argv，含 `--name=value` 形态）与 `ANTIGRAVITY_GEMINI_DIR`／
`ANTIGRAVITY_APP_DATA_DIR` 会改目录，插件跟着走（读错文件会把沙箱判反）。相对目录落在 home 下。

## 2. 逐文件改动

### `backends/antigravity/client.py`
- 常量：`SANDBOX_FLAG` 保留，新增 `SANDBOX_SETTING_KEY = "enableTerminalSandbox"`；注释按新口径改写。
- 删除 `sandbox_enabled()`（按 argv 判开关的旧入口），新增四个纯函数：
  - `_option_value(args, *names)`：从 argv 取 `--name value`／`--name=value`。
  - `agy_settings_path(cli_args)`：按上面的目录口径给出 settings.json 实际路径。
  - `persisted_sandbox_enabled(path)`：只认 JSON 顶层 `enableTerminalSandbox === true`；
    文件缺失、非对象、坏 JSON、`false`、缺键都返回 `False`（→ 插件补 flag）。只读，不改文件。
  - `effective_sandbox_args(cli_args, persisted)`：返回 `(实际 argv, 是否复用持久沙箱)`；
    显式 `--sandbox` > 持久 true > 补 `--sandbox`，三条互斥且不重复。沙箱没有关闭分支：
    `--sandbox=false` 这类显式关闭不算数，插件照补一项（Go 布尔开关后者覆盖前者）。
- `__init__`：新增 `agy_settings_file`（仅测试注入隔离设置；生产现读）；按持久设置算出
  `self._persisted_sandbox`、`self._effective_cli_args`、`self._sandbox_reused`；
  `_bash_env` 变成常量路径（不再有 `None` 分支）。
- `_prepare_sandbox_env()`：不再有「非沙箱返回 `None`」分支——每次都 mkdir 缓存工作区、
  重写受控 BASH_ENV，返回 `{**os.environ, "BASH_ENV": <受控文件>}`；写失败仍按
  `ACPProcessError` 上抛，不静默降级。
- `_ensure_started()`：argv 改用 `self._effective_cli_args`；`env` 恒传入；INFO 日志加
  `来源=`（`agy 持久设置`／`AGENT_ANTIGRAVITY_ARGS 显式`／`插件补 --sandbox`）便于现场核对。
- `prompt()`：非控制轮**无条件**追加工作区说明（原来只在 sandbox 模式追加）；控制轮
  （`/compact`）仍不追加、不改 system hash。new／resume／resume 重建三处共用同一份 `prompt_text`。
- `get_client()`：不变地接 `AGENT_ANTIGRAVITY_ARGS`；沙箱解析全部下沉到客户端构造。

### `config.py`
- `AGENT_ANTIGRAVITY_ARGS` 的 docstring 按新口径改写：它是通用 argv 入口，不是沙箱开关，
  并写明沙箱由插件统一保证（持久 true 复用、否则补 `--sandbox`）。字段与缺省值未变（配置项仍 11 个）。

### 测试
- `tests/test_antigravity_sandbox.py`（重写，21 项）：纯函数覆盖持久 true／false／缺键／坏 JSON／
  文件缺失、settings 路径的 argv 与环境变量口径、显式 flag 不重复、`--sandbox=false` 也照样补；客户端覆盖持久 true 无
  `--sandbox`、持久 false／文件缺失补 `--sandbox`、两种情况都生成缓存工作区＋受控 BASH_ENV＋
  BASH_ENV 覆盖继承值＋提示词说明＋cwd 不变＋无 `additionalDirectories`，持久 true 不改 settings 文件，
  以及 new／resume 提示一致、控制轮不追加 `_meta`、stale BASH_ENV 启动重写。
- `tests/test_antigravity_acp.py`／`test_antigravity_compact.py`／`tests/acp_2b_harness.py`：
  客户端构造统一注入临时 `sandbox_shell_dir` 与 `agy_settings_file`（测试不再碰真实
  `~/.cache`／`~/.gemini`），把系统提示/hash 断言改成「原文 + 说明」的 `append_text()` 口径。
- `tests/test_antigravity_restart.py`：复用上面的 `append_text()` 修正 hash／append 断言。
- 既有 compact／steer／clear／cancel／permission／MCP 回归全部原样保留并通过。

## 3. 命令与结果（本机，生产 venv）

```
cd <worktree>; export PYTHONDONTWRITEBYTECODE=1
python -B -m unittest tests.test_antigravity_sandbox                      → Ran 21 tests, OK
python -B -m unittest tests.test_antigravity_{acp,compact,restart,sandbox,stream_steer,clear_fence,cancel,permission,mcp_tools}
                                                                          → Ran 171 tests, OK
python -B -m unittest discover -s tests -t .                              → Ran 953 tests, OK (skipped=1)
python -B -m unittest discover -s turn_tests -p 'test_*.py'               → Ran 6 tests, OK
python -B verify.py（干净树）                                              → 见第 4 节
```

原始输出见 `EVIDENCE_t_dd244228/`（`sandbox_tests.log`、`focused.log`、`full_tests.log`、
`turn_tests.log`、`verify.log`）。

## 4. 干净树 verify

在工作树干净（`git status --porcelain` 为空）的提交上运行 `python -B verify.py`：
**ALL PASS: 25/25 项通过**（含其内部复跑全量 953 项与两个网关插件独立测试 48／21）。
原始输出 `EVIDENCE_t_dd244228/verify.log`（首行 HEAD 即该提交）；证据随后 amend 进同一提交，
代码未再改动。

## 5. 非目标／边界

- 未改 Hermes／completions／runs／astrbot 后端，未改主实例配置或 `.env`。
- 未改全局 agy settings；本机 `~/.gemini/antigravity-cli/settings.json` 现无 `enableTerminalSandbox`
  键，所以本机默认走「插件补 `--sandbox`」。
- 未新增第二套配置或沙箱框架；沙箱是否生效不由插件开关控制（常开）。
- 旧交付说明 `DELIVERY_t_57f77786.md` 的「缺省零变化」口径已被本文件取代。
