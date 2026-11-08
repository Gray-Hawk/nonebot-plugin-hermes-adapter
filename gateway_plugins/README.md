# gateway_plugins — Hermes 网关侧用户插件

本目录存放**跑在 Hermes gateway 进程里**的用户插件（Hermes `$HERMES_HOME/plugins/<name>/`
布局）。它们与 nonebot 适配器包（本仓库根目录的 `nonebot_plugin_hermes_adapter` 源码）分开
发布：适配器是聊天桥，这些插件补的是网关自身缺的控制面。**不改 Hermes 上游核心文件。**

安装用仓库脚本，从本目录取源（不要从 `.acceptance/` 手抄）：

```bash
python scripts/install_gateway_plugin.py                    # -> ~/.hermes/plugins/session-compress
python scripts/install_gateway_plugin.py --check             # 只比对
python scripts/install_gateway_plugin.py --hermes-home /home/feiyu/.hermes --force
```

装完必须重启该实例的 gateway 进程才会加载（脚本不重启服务）。

## session-compress

Runs 车道按 session id 触发**手动压缩**的控制端点。压缩语义完全复用 messaging `/compress`
的同一实现（`GatewaySessionCommandsMixin._run_manual_compression`：临时 agent + 共享
`compress_now` + 同一轮换持久化）；本插件只补“按 id 寻址 + 鉴权 + 会话级忙碌围栏”这一层
传输，不含第二套压缩器、不另存会话历史。

- 路由：`POST /api/plugins/session-compress/sessions/{session_id}/compress`
- 探活：`GET /api/plugins/session-compress/status`
- 鉴权：复用网关控制面凭据 `API_SERVER_KEY`（Bearer）。未配置/过短时 fail closed 503，
  缺失/错误 bearer 401；密钥不写日志、不回显、不从插件配置读取。
- 请求体（可选）：`{"preview": bool, "partial": bool, "keep_last": int, "focus": "..."}`
- 响应：`status`（`compressed` / `no_change` / `not_enough` / `preview`）、
  `session_id_before` / `session_id_after`、`rotated`、`in_place`、前后消息数与粗略 token，
  以及 `X-Hermes-Session-Id` 响应头（轮换后的新 id，客户端必须采纳）。

语义要点：

- **忙碌互斥是会话级**：压缩前取该会话自己的跨进程 turn lease（任何 agent turn 都取同一把）。
  正在跑 turn 时返回 `409 session_busy` + `Retry-After: 1`；其它会话不受影响，没有全局锁。
- 已有压缩在跑时返回 `409 session_compressing`（读该会话的 compression lock，诊断用）。
- `in_place` 缺省 true：session id 通常不变；若实例配置轮换（`compression.in_place: false`），
  返回新的 continuation id，旧 id 只作历史，不能写回。
- `preview` 是**只读**分支：复用 messaging 的 `_compress_preview_reply` 生成报告，
  不建 agent、不取 lease、不探测 compression lock、不写历史；turn 在跑时也可用。
- 未知/非法 session id：`404 session_not_found` / `400 invalid_session_id`。

测试（需要 Hermes checkout；**必须在一个不是本仓库根的目录下运行**，否则本仓库根目录的
`utils.py` 会遮蔽 Hermes 的 `utils`）：

```bash
cd /tmp && PYTHONPATH=/path/to/hermes-agent \
  /path/to/hermes-agent/venv/bin/python -m unittest discover \
  -s /path/to/this-repo/gateway_plugins/session-compress/tests -v
```
