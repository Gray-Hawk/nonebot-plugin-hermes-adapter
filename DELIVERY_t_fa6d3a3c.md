# t_fa6d3a3c 交付：反重力 ACP 手动会话压缩（复用 `/compress`）

## 结论

反重力后端接入人工会话压缩，只复用现有 `/compress` 命令面与管理员检查，**没有新增 QQ 命令、
配置项、RPC 方法或第二套压缩系统**。实测 agy 1.5.9 只有命令文本可用（`session/prompt` 发
`/compact`），因此实现是「已有会话 `session/resume` + `session/prompt('/compact')`」。

- 分支：`wt/t_fa6d3a3c`（看板分配的独立 worktree；卡里建议名 `feat/antigravity-acp-compact`
  未启用，避免与 dispatcher 的 worktree 分支名脱钩）
- 功能提交：`56e64675055f056aa3d055f03653a88ce85ff445`
- 复核返修：`3f1892f`（压缩控制轮 steer 隔离，见下）
- 交付树 `verify.py`：`ALL PASS: 25/25`

## 改动文件

| 文件 | 改动 |
| --- | --- |
| `backends/antigravity/client.py` | 新增 `compact(...)->bool`、`COMPACT_COMMAND_TEXT`、`ACPTurn.collect_text`/`control`；`prompt()` 支持控制命令形态（`system_prompt=None` + `collect_text=False`），`steer()` 拒绝控制轮 |
| `handlers/commands.py` | +101/−5：`/compress` 后端分支 `_compress_acp`、ACP 回执与错误归类、`build_help_text(backend=…)`、`_help_backend()` |
| `tests/test_antigravity_compact.py` | 新增 20 项：fake ACP wire 契约 + 命令面 + 帮助文案 + 压缩/steer 重叠回归 |
| `tests/test_manual_compress_control.py` | +12：`HandlerTests.setUp` 钉住 `hermes_api_interface="runs"`（见下） |

### 为什么改那条既有测试

`tests/test_antigravity_clear_fence.py` 在**导入期**就把全局 `plugin_config.hermes_api_interface`
设成 `antigravity`（该文件注释说明是为配置根隔离）。此前 `handle_compress` 不看后端，所以无影响；
现在它有后端分支，同进程跑全量时 Runs 侧命令用例会误走 ACP 分支。修法是在测 Runs HTTP 面的
`HandlerTests.setUp` 里钉住 `"runs"`，断言一条没削弱。

## 行为契约

- **裸 `/compress`**：`session/resume`（不带系统提示 `_meta`、不改保存的 system hash、不同步模型）
  → 同一 sessionId 上 `session/prompt` 文本精确为 `/compact`。成功回执只说明压缩完成且 session 不变。
- **缺会话**：`compact()` 在拉起进程前判 `_load_session`，返回 `False` 且**零 RPC**；resume 返回
  -32602（会话失效）同样按缺会话处理，绝不 `session/new` 补空会话。
- **忙碌**：命令侧 `active_turn(session_key)` 预检，复用既有 409/`session_busy` 回执；预检后的竞争
  由 ACP 客户端同一把 `_session_lock` 串行化，不新增状态机。
- **附加参数**：`--preview` 或任何焦点/附加文本只回能力边界，**零 RPC**。
- **错误**：取消/超时/其它 ACP 错误按真实类别出回执；不伪造 before/after 消息数、摘要、进度或
  `compaction_update`。
- **正文隔离**：控制轮 `stream_here=False` + `collect_text=False`，并在 `user_message_chunk` 分支
  额外拦截——agent 的 `/compact` 回显不会把控制轮变成 streaming 轮，agy 输出不进 QQ 正文流。
- **`/help`**：反重力管理员只看到「手动压缩当前会话上下文」，不写 `--preview`；Hermes/completions/runs
  的 HTTP 压缩、preview、轮换采纳行为保持原样。

## 复核返修：压缩控制轮不接受正常 steer（`3f1892f`）

复核 P1：`compact()` 复用 `prompt()` 注册的控制轮虽然 `collect_text=False`，但仍被
`active_turn()`/`steer()` 消费链当成可接管轮；压缩期间用户发正常私聊会被成功 steer 并跳过
正常 dispatch，控制轮又没有正文出口，最终只回「压缩完成」、回复被静默吞掉。

最小修复（不扩状态机、不动 preview/focus）：

- `ACPTurn` 增加 `control` 标记，`prompt(system_prompt=None)` 的控制轮登记该标记；
  `collect_text` 仍只管正文隔离。
- `steer()` 遇到控制命令轮返回 `False`；消息入口按既有 False 回退走正常 dispatch，
  同一把 `_session_lock` 把后续普通轮排在压缩之后，回复不丢、只处理一次。
- `active_turn()` 仍暴露控制轮，保留 `/compress` 的忙碌预检与 409 回执语义。

新增回归（`tests/test_antigravity_compact.py`，+4 项）：

- 真 `/compress` + 真 `handle_message` + fake ACP 重叠：压缩期间消息不被 steer、零第三条
  prompt；压缩收尾后正常 dispatch 恰好 1 次，`session/prompt` 第三条携带该消息，
  平台收到真实回复；结束无残留 active turn。
- 控制轮 `steer()` 返回 False 且不发 `session/prompt`、控制轮原样保留；
- 普通在跑轮 steer 行为不变；
- 压缩进行中再次 `/compress` 仍回「正忙」，不发第二条 `/compact`。

## 测试（真实命令与结果）

```
# 解释器：/home/neonsky/NeonBot/.venv/bin/python（PYTHONDONTWRITEBYTECODE=1, -B）
python -B -m unittest tests.test_antigravity_compact -v      → Ran 20 tests ... OK
python -B -m unittest discover -s tests -t .                 → Ran 921 tests ... OK (skipped=1)
python -B -m unittest discover -s turn_tests -p 'test_*.py'  → Ran 6 tests ... OK
python -B verify.py                                          → ALL PASS: 25/25（含网关插件独立测试 48/21 项）
python -B -m py_compile backends/antigravity/client.py handlers/commands.py \
        tests/test_antigravity_compact.py tests/test_manual_compress_control.py   → 通过
```

新测试逐项覆盖：已有会话 resume 无 `_meta`、MCP 与普通轮同源、`session/prompt` 精确 `/compact`、
无 `session/new`、保存的 system hash 不被改写、不重发模型；缺会话 `False` 且 wire 为空；失效会话不建
新会话；控制轮不接管正文流；压缩后普通 prompt 仍按哈希 append 并更新 hash；命令侧裸命令成功／
agy 输出不外泄／缺会话／忙碌／附加参数零 RPC／帮助文案；Hermes 帮助保留 `--preview`。

## 真实 ACP 验证（官方 `agy acp`，1.5.9，中性 canary）

肥鱼环境未能直接跑：`/home/neonsky` 与插件 venv 对 `feiyu` 不可读（0700），跨用户执行被本机审批
策略拦下，未做权限变通。改为本机同版本官方 `agy acp`（`/home/neonsky/.local/bin/agy`，1.5.9）
直连真客户端验证，只发中性 canary、不挂 MCP、不注入人格、不改配置、不触碰任何运行实例。

三段式探针（`AntigravityACPClient` 子类记录真实 wire 帧）：

| 观测 | 结果 |
| --- | --- |
| prompt#1 | `OK`（4.8s），`session/new` 1 次，sessionId `31badd0b-…` |
| `compact()` | 返回 `True`（7.8s），sessionId 不变，`session/resume` 参数键 = `[cwd, mcpServers, sessionId]`（无 `_meta`） |
| prompt#2 | `BLUE-FOX-27`（5.6s）——压缩后仍能复述 canary，同一会话续聊 |
| `session/prompt` 文本序列 | `记住代号 BLUE-FOX-27。只回复 OK。` / `/compact` / `代号是什么？只回复代号本身。` |
| `session/compact` RPC | 0 次（客户端根本没发） |
| `session/set_model` | 0 次 |
| 收到的 update 类型 | `agent_message_chunk` / `available_commands_update` / `session_info_update` / `usage_update` / `user_message_chunk`（无 `compaction_update`） |

原始结果 JSON：`~/.hermes/profiles/coder_deepseek/cache/scratch/acp-probe/acp_compact_probe_result.json`。
探针在本机 agy 账号下留下 3 个中性 canary ACP 会话（纯文本，无实例人格）。

## 明确未接的边界

- `--preview`（占用/上限）→ 只回能力边界，不发 RPC。
- 焦点主题（focus）、`usage` 缓存、`available_commands` 能力缓存、压缩进度/摘要展示 → 均未接，
  也没有为它们预留配置键或模块。
- 真实压缩的 before/after 消息数、摘要内容：ACP 侧没有可用事件，不伪造。
