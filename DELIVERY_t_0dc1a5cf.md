# t_0dc1a5cf 交付：session key 按真实后端命名 + ACP 压缩回执脱敏

## 结论

内部 session key 不再统一挂 Hermes 名，按**真实后端**命名（主人口径）：

| 后端（`AGENT_API_INTERFACE`） | session key |
| --- | --- |
| `antigravity`（ACP） | `antigravity-{internal_id}[-gN]`，如 `antigravity-onebotv11+private+10001` |
| `completions` / `runs` / `astrbot` | `hermes-{internal_id}[-gN]`，**完全不变** |

同时修掉 QQ 可见面的泄漏：ACP `/compress` 成功回执改成固定文本，不再回显内部
`session key` / ACP sessionId。

- 分支：`wt/t_0dc1a5cf`（看板分配的独立 worktree，未动主工作树）
- 功能提交：`128c603`
- 回执文本钉死提交：`67d84db`
- 交付树 `verify.py`：`ALL PASS: 25/25`

## 改动文件

| 文件 | 改动 |
| --- | --- |
| `core/session.py` | +109/−6：`resolve_session_key_prefix()`（只读现有配置，不新增开关）；`_render_key()` 统一渲染；`_rename_derived_keys()` 把缓存的**本地派生** key `hermes-*[-gN]` 一次性改名到当前后端前缀；`session_key_prefixes()` 提供当前 + 仍需兼容的旧前缀 |
| `handlers/commands.py` | +3/−2：`_ACP_COMPRESS_DONE` 固定为 `🗜️ 会话压缩完成(ACP session 不变)`，删掉 `{session_key}` 占位 |
| `core/background_report_runtime.py` | +6/−2：`resolve_background_route()` 的 origin_session_id 前缀正则接受 `hermes-` 与 `antigravity-` |
| `mcp/tools/background_status.py` | +60/−8：`_session_prefixes()` 按后端给候选前缀；`_belongs()` 支持多前缀；`_collect_running()` 在多前缀时逐前缀查、按 `(source, task_id)` 去重合并 |
| `tests/test_session_key_backend_prefix.py` | 新增 9 项：两种后端的新建 / clear / T1、旧本地 key 改名、旧 ACP 映射不被读取、后台匹配兼容 |
| `tests/test_antigravity_acp.py` | 新增 1 项 e2e：换名下轮干净新建 ACP 会话、不 resume 旧 hermes 会话、不错读旧映射 |
| `tests/test_antigravity_compact.py` | 新增 1 项：成功回执精确文本 + 不含内部 key / ACP UUID / QQ 号 |

## key 命名与旧数据：什么保留、为什么

1. **Hermes 后端全部照旧（必要保留）。** 上游 session id（含响应头
   `X-Hermes-Session-Id` 采纳回来的轮换 id）不可本地强改，改名等于断上游历史。
   因此 Hermes 后端新派生 key 仍是 `hermes-{internal_id}[-gN]`，`session_keys`
   缓存里的现有 key 原样续用；只有 `agent-`/新前缀这种"为统一名字重置用户会话"的
   做法被明确禁止。`_rename_derived_keys()` 在前缀相同（`hermes` → `hermes`）时
   直接返回 0，一个 key 都不动。
2. **ACP 旧会话不保、不续用（主人口径）。** 反重力侧没有任何需要保留的东西：
   - `session_keys` 里旧版本缓存的 `hermes-{internal_id}[-gN]` 在 ACP 后端启动时
     **只改本地名字**为 `antigravity-{internal_id}[-gN]`（保留 `-gN` 代际）；
   - 旧 `acp_sessions` 行**不迁移、不双写、不别名**，成为无引用历史（可留待既有
     清理路径淘汰）；
   - 换名后的新 key 读不到旧映射，下一轮按真实行为 `session/new` 新建 ACP 会话。
     不会错误续读旧 `hermes-*` 映射，也没有任何旧 key 回退层。
3. **上游轮换 id 永不改名。** 只有本地派生形态 `{prefix}-{internal_id}[-gN]`
   （`-g` 后必须全数字）才参与改名；上游回传的任意 uuid 原样保留。

## 新 key 何时生效

- ACP 部署：`SessionManager.bind_store()`（插件启动装配）立即把已缓存的本地
  `hermes-*` 派生长 key 改名并落库；此后**新建会话、`/clear`、T1 空闲重置**都用
  `antigravity-*`。重启不会退回 `hermes-*`。
- Hermes 部署：无任何变化（新会话、clear/T1 仍 `hermes-*`，历史连续）。
- 配置切换后端时，下一次取 key 会按当前后端校准并做同一次改名（仍只动本地派生 key）。

## 回执脱敏证据

`handlers/commands.py`：

```python
_ACP_COMPRESS_DONE = "🗜️ 会话压缩完成(ACP session 不变)"
...
text = _ACP_COMPRESS_DONE          # 原: .format(session_key=session_key)
```

回归（`tests/test_antigravity_compact.py::ACPCompressCommandTests::
test_success_receipt_carries_no_internal_session_key`）在真 handler + 假 ACP 子进程
链路上断言：

- 回执精确等于 `🗜️ 会话压缩完成(ACP session 不变)`；
- 不含真实内部 key（`antigravity-onebotv11+private+123`）、`antigravity-`、`hermes-`、
  `onebotv11+`、管理员 QQ 号；
- 不含形如 ACP UUID 的片段（`[0-9a-f]{8}-[0-9a-f]{4}-`）。

## 后台状态匹配

- `resolve_background_route()`：`origin_session_id` 正则改为
  `(?:hermes|antigravity)-…`，两种真实后端都能还原 adapter / 私聊 / 群目标；
  `agent-…` 等非真实前缀仍返回 `None`。
- `get_background_tasks`：ACP 部署的候选前缀为
  `antigravity-{adapter}+{scope}+{id}` 与 `hermes-{adapter}+{scope}+{id}`（当前在前）。
  升级前已落地的看板 `session_id` / 子代理 `origin_session_id` 仍是 `hermes-*`，
  保留的是**只读匹配**，不构成 ACP 会话续用、迁移或别名层。Hermes 部署只认
  `hermes-*`，行为不变。
- 运行中聚合多前缀时逐前缀查 `(source, task_id)` 合并去重，来源错误也去重，
  `running_unknown` 语义不变。

## 测试（真实命令与数字，解释器 `/home/neonsky/NeonBot/.venv/bin/python`，`-B`）

```
python -B -m unittest tests.test_session_key_backend_prefix      → Ran 9  ... OK
python -B -m unittest tests.test_antigravity_compact              → Ran 21 ... OK
python -B -m unittest tests.test_antigravity_acp                  → Ran 42 ... OK
python -B -m unittest tests.test_antigravity_clear_fence          → Ran 4  ... OK
python -B -m unittest tests.test_antigravity_permission           → Ran 21 ... OK
python -B -m unittest tests.test_antigravity_restart              → Ran 7  ... OK
python -B -m unittest tests.test_antigravity_stream_steer         → Ran 20 ... OK
python -B -m unittest tests.test_background_report_chain          → Ran 25 ... OK
python -B -m unittest tests.test_background_report_lifecycle      → Ran 9  ... OK
python -B -m unittest tests.test_control_mcp                      → Ran 42 ... OK
python -B -m unittest discover -s tests -t .                      → Ran 932 ... OK (skipped=1)
python -B -m unittest discover -s turn_tests -p 'test_*.py'       → Ran 6  ... OK
HERMES_HOME=<隔离目录> python -B verify.py                        → ALL PASS: 25/25
```

`verify.py` 内含网关插件独立套件：`queued-lane-interim-thin` 48 项、
`steer-presentation-thin` 21 项，均 OK；旧名白名单 / 新名一致性 / 交付树干净
（提交后）全部 PASS。

TDD 过程：新增回归先跑出 5 FAIL + 1 ERROR（ACP 前缀、旧映射读取、后台匹配、
回执泄漏位点），实现后转绿；Hermes 侧既有用例自始未红，证明旧行为未被改动。

## 边界

未改 `HermesClient`、日志前缀、配置字段、包名，无全仓文本替换，无新增配置开关，
未重建状态体系，未部署 / 未重启生产 / 未推送远端。`astrbot` 后端不在主人口径的
两种后端命名内，保持 `hermes-` 不变。
