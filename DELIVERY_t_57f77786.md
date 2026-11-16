# DELIVERY t_57f77786 — ACP 沙箱缓存工作区 BASH_ENV 启动补丁

> 口径已更新：本文件记录的「缺省零变化／仅 argv 含 `--sandbox` 时启用」已被主人 2026-11-16
> 定案覆盖，现行契约见 `DELIVERY_t_dd244228.md`（沙箱在插件 ACP 侧常开、跟随 agy 持久
> `enableTerminalSandbox`）。本文件仅作历史记录。

分支 `wt/t_57f77786`（基线 `dev=28b95c3`）。把 2026-11-16 早晨实测通过的 ACP 沙箱
PoC 转正：**只碰 antigravity ACP 后端**，主实例与 completions／runs／astrbot 后端一行未改。
未合并、未部署、未上传，未跑真实 `agy` 模型（真实 agy 验收由主会话负责）。

## 1. 前置核对结果

- 仓库里**没有**现成的「ACP CLI args」配置键：argv 唯一来源是
  `backends/antigravity/client.py::DEFAULT_ACP_ARGS`，`get_client()` 此前根本不传
  `cli_args`，所以 `--sandbox` 无处可放，也没有任何 sandbox 相关代码或配置（全仓 grep
  `sandbox` 命中 0）。
- 因此本次新增**唯一一个通用载体** `AGENT_ANTIGRAVITY_ARGS`（空白分隔的附加 argv），
  **不新增 sandbox 专用布尔开关**：是否进入沙箱模式只看真实 argv 里有没有精确的
  `--sandbox`（`sandbox_enabled()`）。缺省空串 → argv 与改造前逐字节一致。
- 肥鱼现场 `.env` 现在只有 `AGENT_ANTIGRAVITY_{CLI,MODEL,PERSONA}`，没有 args 键；
  部署时由主会话加 `AGENT_ANTIGRAVITY_ARGS=--sandbox`，并单独处理 proceed-in-sandbox。

## 2. 逐文件改动

### `config.py`
- 新增 `hermes_antigravity_args`（环境变量 `AGENT_ANTIGRAVITY_ARGS`），缺省 `""`。
- 类 docstring 的配置项计数 10 → 11。

### `backends/antigravity/client.py`
- 新增常量：`SANDBOX_FLAG = "--sandbox"`、`SANDBOX_BASH_ENV_NAME = ".acp-sandbox-bash-env"`。
- 新增纯函数（唯一判断点，便于断言）：
  - `sandbox_enabled(cli_args)`：`SANDBOX_FLAG in argv` 的精确成员判断（`--sandbox-mode`、
    `--no-sandbox` 都不算开启）。
  - `sandbox_shell_workspace()`：`Path.home() / ".cache" / "agent-adapter" / "acp-workspace"`，
    运行用户 `~/.cache` 下的稳定专用子目录；**不进** session cwd，**不进**
    additionalDirectories（声明成工作区根会重新变只读）。
  - `sandbox_workspace_notice()` / `append_workspace_notice()`：一句中性任务文本
    `本次任务的实际工作目录是 <绝对路径>，相对路径的读写都以此目录为准。`，
    追加到既有系统提示尾部。
  - `parse_extra_args(raw)`：`shlex.split` 成独立 argv 项；空串返回 `()`。
- `__init__`：新增 `sandbox_shell_dir`（仅测试注入隔离目录）；按 argv 计算
  `self._sandbox`、`self._sandbox_shell`、`self._bash_env`（非 sandbox 时为 `None`）。
- 新增 `_prepare_sandbox_env()`：**只在 sandbox 模式**创建缓存工作区并写受控 BASH_ENV，
  返回 `{**os.environ, "BASH_ENV": <文件>}`；非 sandbox 返回 `None`（= 子进程完全继承，
  与改造前一致）。写文件失败按 `ACPProcessError` 上抛，不静默降级。
- `_ensure_started()`：`create_subprocess_exec(..., cwd=str(self._workspace), env=env)`；
  `cwd` 仍是原 ACP workspace，argv 仍只由 `build_acp_args()` 生成；sandbox 模式多打一条
  INFO（shell 工作区与 BASH_ENV 路径）。
- `prompt()`：把「提示词文本 + hash」的重算收成一个 if/else——
  `system_prompt is None`（`/compact` 控制轮）保持空 hash、不追加；否则
  `prompt_text = 追加工作区说明后的文本`（sandbox 才追加），`system_hash` 按 `prompt_text`
  计算。`session/new`、`session/resume`、resume 失败重建三处都改用同一个 `prompt_text`，
  所以 new／resume 提示一致、哈希稳定、不变时不重复 append。
- `get_client()`：把 `AGENT_ANTIGRAVITY_ARGS` 解析后接到
  `cli_args=(*DEFAULT_ACP_ARGS, *extra_args)`；解析失败（引号不配对）打 WARN 并按空值处理。
- 未改动：模型下发、MCP 装配、权限／审批、§ 解析、steer、`/clear` 代际栅栏、
  `additionalDirectories`（本后端从不发送该字段）、全局 agy settings。

### `tests/fixtures/fake_agy_acp.py`
- `log_argv()` 额外写 `<log>.env`（子进程实际看到的 `BASH_ENV`）。只看 argv 证明不了
  env：继承与覆盖必须分开断言。原 `<log>.argv` 格式不变，既有用例不受影响。

### `tests/test_antigravity_sandbox.py`（新增 12 项）
- 纯函数：`--sandbox` 精确成员判断（近似名不算）、默认缓存路径为
  `~/.cache/agent-adapter/acp-workspace` 且不同于 ACP session cwd、工作区说明是一句
  含绝对路径的中性文本（不含身份词、不含换行）。
- 缺省零变化：真实子进程 argv 逐项等于改造前、`BASH_ENV` 与父进程一致、
  不生成 BASH_ENV 文件、不创建缓存目录、`session/new` 的 cwd 与 `_meta` 提示词原样、
  无 `additionalDirectories`。
- 缺省继承既有 `BASH_ENV`：父进程已设 `BASH_ENV` 时子进程拿到的就是那个值（未被覆盖）。
- sandbox 模式：`--sandbox` 落到真实 argv；缓存工作区被创建；BASH_ENV 文件在**原 ACP
  cwd 内**、内容精确等于 `cd <缓存工作区>\n`；子进程 env 的 `BASH_ENV` 指向该文件；
  `session/new` 的 cwd 未变、没有 `additionalDirectories`、缓存路径只出现在提示词那一句。
- new／resume 一致性：首轮 `session/new` 的 append = `SYS-A\n\n<说明>`；同上下文第二轮
  resume 不带 `_meta`（哈希未变不重复追加）且 cwd 不变；换上下文后 resume 的 append =
  `SYS-B\n\n<说明>`，保存的 hash 等于该文本的 sha256。
- `/compact` 控制轮：resume 不带 `_meta`、保存的 hash 不变、prompt 仍是 `/compact`。
- sandbox 覆盖继承值（父进程已有 `BASH_ENV` 时被子进程覆盖为受控文件）。
- 陈旧 BASH_ENV 文件会在启动时被重写。
- 配置面：新键缺省空串、别名 `agent_antigravity_args`；`get_client()` 空值 →
  `_cli_args == DEFAULT_ACP_ARGS`、`_sandbox=False`；`--sandbox` → argv 追加且
  `_sandbox=True`、`_sandbox_shell` 走默认缓存路径。

## 3. 测试证据（解释器 `/home/neonsky/NeonBot/.venv/bin/python`，`-B`）

```
python -B -m unittest tests.test_antigravity_sandbox         → Ran 12 ... OK
python -B -m unittest tests.test_antigravity_acp             → Ran 42 ... OK
python -B -m unittest tests.test_antigravity_compact         → Ran 21 ... OK
python -B -m unittest tests.test_antigravity_clear_fence     → Ran 4  ... OK
python -B -m unittest tests.test_antigravity_permission      → Ran 21 ... OK
python -B -m unittest tests.test_antigravity_restart         → Ran 7  ... OK
python -B -m unittest tests.test_antigravity_stream_steer    → Ran 20 ... OK
python -B -m unittest tests.test_antigravity_mcp_tools       → Ran 22 ... OK
python -B -m unittest tests.test_antigravity_cancel          → Ran 13 ... OK
python -B -m unittest discover -s tests -t .                 → Ran 944 ... OK (skipped=1)
python -B -m unittest discover -s turn_tests -p 'test_*.py'  → Ran 6  ... OK
HERMES_HOME=<隔离目录> python -B verify.py                    → ALL PASS: 25/25
```

`verify.py` 在干净 HEAD worktree（`git worktree add --detach <scratch> HEAD`，隔离
`HERMES_HOME`）里跑：25/25 全门，含网关插件独立套件（48＋21）、旧名白名单、
交付树干净、无 pyc／pytest 缓存、验收不依赖暂存区。同一 worktree 复查新套件 12 项与
全量 944 项、turn 6 项同样全绿，交付树仍为 0 未提交改动。

全量从基线 932 → 944（+12），既有计数（ACP 42／compact 21／clear_fence 4／permission 21／
restart 7／stream_steer 20）与基线逐项一致：原有 compact／steer／clear／restart 行为未被改动。

TDD 过程：新用例先跑出 12 ERROR（`sandbox_enabled`／`sandbox_shell_workspace` /
`sandbox_workspace_notice` 不存在），实现后转绿；两处测试自身的断言错误（fixture 的
`argv[0]` 是脚本路径而非解释器、缓存路径本应出现在提示词里）已按真实行为修正。

## 4. 边界与未验证项

- **真实 agy 未跑**：`~/.cache` 可写、`bash -c` 保留 `BASH_ENV`、proceed-in-sandbox 下
  权限请求为 0、原生 `write_file/read_file` 走原文件审批——这些都以 2026-11-16 早晨的
  官方 agy 实测为准，本次没有用主用户凭据或改生产去复测。本机想再跑一条 `bash -c`
  对照也被审批策略拦下，未换写法绕开。
- `BASH_ENV` 会继承给 agy 的全部 bash 子进程（MCP、hooks、后台命令）：转正后需按
  `references/sandbox.md` 回归这些链，确认预期 `cd` 没有污染别的启动路径。
- 沙箱给的是整个 `~/.cache` 的写权限，不止选定子目录——这是上游行为，插件没有扩大
  也没有收窄它；需要更强隔离应走外层 bwrap／容器，不在本补丁范围。
- 不改全局 agy settings、不设 `toolPermission`、不自动 approve `run_command`／
  `BypassSandbox`、不启用 `dangerously-skip-permissions`；`proceed-in-sandbox` 由主会话
  在部署阶段单独处理。
- 部署时要核对一条日志：`ACP 沙箱模式 shell 工作区=` 必须是运行用户的
  `~/.cache/agent-adapter/acp-workspace`（肥鱼即 `/home/feiyu/.cache/...`）。路径由
  `Path.home()` 解析（`$HOME`，缺失时回落 passwd 的 home）；`/home/feiyu/.cache` 实测
  属主 `feiyu:feiyu`、可写，但服务若把 `$HOME` 指到别处，这条日志会先暴露出来。
