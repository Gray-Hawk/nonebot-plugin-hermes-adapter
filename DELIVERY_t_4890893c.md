# DELIVERY t_4890893c — ACP 后端支持显式默认自定义 Agent

分支 `feat/antigravity-acp`，基线 `d171dcb`。只做最小补充：给 ACP 后端一个显式
`--agent <名>` 配置，不改其它任何后端行为。未部署、未重启、未上传、未跑真实 `agy`
模型（本机也没有在主实例启动 agy）。

## 1. 做了什么

实测结论决定了实现方式：肥鱼侧 `agy --agent nano` 会回答 Nano，不带参数仍回答
Antigravity；交互 `/agents` 的选择不跨进程生效，官方 settings 也没有默认 agent 键。
所以插件只能在**启动命令行**上传参，不能靠会话内选择或 settings。

### 修改

- `config.py`：新增可选配置 `hermes_antigravity_agent`（环境变量
  `AGENT_ANTIGRAVITY_AGENT`），缺省空串 = 保持官方默认 agent。
- `backends/antigravity/client.py`：
  - 新增纯函数 `build_acp_args(cli_args, agent)`：`agent` 去空白后为空则原样返回
    `cli_args`；非空则返回 `(*cli_args, "--agent", agent)`。
  - `AntigravityACPClient.__init__` 新增 `agent: str = ""`，存 `self._agent`（strip）。
  - `_ensure_started()` 的 argv 由 `[self._cli_path, *build_acp_args(self._cli_args, self._agent)]`
    生成，仍走 `asyncio.create_subprocess_exec(*argv)`：独立 argv 项，不拼 shell 字符串、
    不做包装脚本、不写全局 settings。
  - `get_client()` 把 `plugin_config.hermes_antigravity_agent` 接进客户端（与 cli/model
    同一处定型，启动/重载生效，不在每轮重读）。
- `tests/fixtures/fake_agy_acp.py`：假服务端启动时把真实 `sys.argv` 写到
  `<log>.argv`，供测试断言 `--agent` 在命令行里的确切落点（仅测试夹具，生产无关）。
- `tests/test_antigravity_acp.py`：新增 9 项用例（见 §3）。

### 没动的地方

- 模型（`session/set_model`）、OAuth/凭据、动态 systemPrompt 的 `_meta` 追加、
  § 协议解析、`/clear` 代际栅栏、超时/错误归一化、权限拒绝、媒体本地拒绝：一行未改。
- 没有扩成 agent 管理系统（无列表、无切换命令、无持久化 agent 选择）。
- 没有在 `session/*` 请求里塞 agent 字段，也没有写 `~/.antigravity` 之类全局设置。

### 配置

```dotenv
AGENT_API_INTERFACE=antigravity
# 可留空：保持官方默认 agent；非空则给 `agy acp --process-per-session` 追加 --agent <值>
AGENT_ANTIGRAVITY_AGENT=nano
```

实际生效命令行：

```text
agy acp --process-per-session                            # AGENT_ANTIGRAVITY_AGENT 为空
agy acp --process-per-session --agent nano               # AGENT_ANTIGRAVITY_AGENT=nano
```

## 2. 行为边界

- 只有非空（去空白后）才追加参数；只写空白等同未配置，避免 `--agent "   "`。
- 参数拼装只发生在 argv 列表：值里的空格、`;`、`$()`、反引号原样留在同一个 argv 项，
  不会被 shell 或进程层拆开（用例直接断言真实子进程 `sys.argv`）。
- 配置在 `get_client()` 首次构造时定型；未配置时命令行与改造前逐字节一致。

## 3. TDD 证据

先 RED 后 GREEN，三个切片依次推进（argv 纯函数 → 真实子进程命令行 → 配置接线）：

1. `ACPAgentArgvTests`（4 项）：缺省无 `--agent`；`nano` 变成独立
   `("--agent", "nano")` 两项；含空格/`;`/`$()`/反引号的值仍是单一 argv 项；
   纯空白配置等同未配置。
2. `ACPAgentSpawnTests`（3 项）：用假 `agy acp` 子进程记录的真实 `sys.argv` 断言
   —— 缺省时命令行与夹具参数完全相等且无 `--agent`；`agent="nano"` 时末尾恰好是
   `["--agent", "nano"]` 且前面参数不变；特殊字符值仍是最后一项、只多两项。
3. `AntigravityAgentConfigTests`（2 项）：`Config()` 里 `hermes_antigravity_agent == ""`、
   alias 是 `agent_antigravity_agent`、`model_validate({"agent_antigravity_agent": "nano"})`
   能读到；`get_client()` 把配置值接到客户端命令行。

RED 原始输出（`EVIDENCE/antigravity_agent_red.txt`）：把改动前的 HEAD 树导出到临时目录，
只覆盖测试文件与夹具后运行——9 项全部报错（`ImportError: build_acp_args`、
`TypeError: unexpected keyword argument 'agent'`、`AttributeError: hermes_antigravity_agent`），
即失败原因都是能力缺失，不是断言写错：

```text
Ran 9 tests in 0.014s
FAILED (errors=9)
```

GREEN 原始输出（`EVIDENCE/antigravity_agent_green.txt`）：

```text
# focused
Ran 9 tests in 0.056s
OK
# tests.test_antigravity_acp + tests.test_antigravity_clear_fence
Ran 39 tests in 0.842s
OK
# 全量 tests/
Ran 754 tests in 33.863s
OK (skipped=1)
# turn_tests
Ran 6 tests in 0.053s
OK
```

解释器 `/home/neonsky/NeonBot/.venv/bin/python`，`PYTHONDONTWRITEBYTECODE=1`、`-B`。

## 4. 改动文件

```text
backends/antigravity/client.py   +20 -1
config.py                        +3  -0
tests/fixtures/fake_agy_acp.py   +8  -0
tests/test_antigravity_acp.py    +135 -6（原有 make_client 抽出 fixture_args()，无行为变化）
EVIDENCE/antigravity_agent_red.txt, EVIDENCE/antigravity_agent_green.txt（新增证据）
```

## 5. 供复核关注点

- `build_acp_args` 是命令行唯一拼装点，`_ensure_started` 只用它；没有第二处拼接。
- 空串路径不新增任何 argv 项（`test_default_command_line_is_unchanged` 直接比对整条命令行）。
- 夹具只多写一个 `.argv` 文件，不影响任何既有断言（39/754 项全绿为证）。
- 若要真机验证：在肥鱼实例设 `AGENT_ANTIGRAVITY_AGENT=nano` 后重启插件，问一句看是否答
  Nano——本卡按任务要求没有执行这一步。
