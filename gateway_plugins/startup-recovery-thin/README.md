# startup-recovery-thin

Hermes 网关**用户侧薄插件**：把两处启动恢复的同步阻塞挪出事件循环。不改 Hermes 上游文件，
不换内部适配器，不动 Runs，不新增图片回收器／TTL／索引／状态机／持久化表。

## 它做两件事

1. **启动期图片扫描让路** — 包装 `gateway.session_ingress_media.collect_unheld_api_images`：
   - 在事件循环线程上调用（唯一站点是 `gateway.session_api_turn.recover_authority_api_turns`
     的启动恢复，函数内 `from gateway.session_ingress_media import ...` 属调用时导入）→
     **不执行原函数**，返回 0，并打一条明确的 WARNING（整个进程只打一次）。
   - 在 worker 线程调用（`gateway/platforms/api_server.py` 删除会话路径的
     `await asyncio.to_thread(collect_unheld_api_images, db)`）→ **执行原函数**，删除会话后的
     图片回收行为不变。
2. **native 启动恢复的绑定阶段进 worker 线程** — 以 PR #148062 head
   `4d4b4e93e63fbc55a1b1422395d8b4c3e1b28b5e` 为蓝本，包装
   `gateway.run_runtime.recover_profile_native_sessions`：pending 读取、路由遍历、压缩血缘
   查询（`db._read_all` / `session_store.list_sessions` / `logical_owner`）在
   `asyncio.to_thread` 中执行；真实的 `SessionAuthority.recover_native_sessions` 与
   `recover_detached_sessions` 仍回主事件循环。
   - 若内核**已经**带该 PR（存在 `_native_recovery_bindings` 且函数源码里已有 `to_thread`）→
     `deferred-core-patched`，不再叠一层 hop。
   - 若内核把绑定阶段抽成 `_native_recovery_bindings` 但仍同步调用 → 复用内核那个函数，只补线程跳。

## 刻意不做的

- **不碰 PR #148062 的 shutdown 等待差异**（`settle_gateway_runtime` 的 `join_authority_work`
  超时）。整文件恢复与那段行为去留另立清理卡评估。
- 不做三插件合并、不调 watchdog、不搬图片、不引入缓存状态；没有卸载钩子（重载是幂等的）。
- 安装失败／函数形状不认识 → **抛错**（`RuntimeError`），不静默降级。

## 安装

```bash
python scripts/install_gateway_plugin.py startup-recovery-thin              # -> $HERMES_HOME/plugins/
python scripts/install_gateway_plugin.py startup-recovery-thin --check       # 只比对
```

装完必须重启该实例的 gateway 才会加载（脚本不重启任何服务）。插件本身**幂等**：重复加载、
adapter 重连、`discover_and_load(force=True)` 都只会再校验一次，不会叠包（测试
`test_reinstall_stacks_nothing` 固定这一点）。

### 安装时序

- 插件加载时 `register(ctx)` 立刻 `install()`（最早）。
- `ctx.register_platform_handler("api_server", wire)`：`APIServerAdapter.connect()` 在
  `AppRunner.setup()` 之前调用 `_wire_plugin_handlers`，**然后**才 `return True`；
  `run_adapters.GatewayAdapterLifecycleMixin._publish_primary_adapter` 在 connect 之后发布
  adapter 并调用 `recover_api_turns`。所以 `wire` 天然早于 `recover_api_turns`。
  隔离 PoC 用真实 `connect()`＋真实 `_publish_primary_adapter`＋真实 `recover_api_turns`
  记录了这条顺序（见下）。

## 测试

需要 Hermes checkout；**必须在一个不是本仓库根的目录下运行**（本仓库根的 `utils.py` 会遮蔽
Hermes 的 `utils`）：

```bash
cd /tmp && PYTHONPATH=/path/to/hermes-agent \
  /path/to/hermes-agent/venv/bin/python -m unittest discover \
  -s /path/to/this-repo/gateway_plugins/startup-recovery-thin/tests -v
```

覆盖：事件循环调用零原函数＋只记一次日志、worker 线程调用走原函数、绑定在 worker 线程且
`recover_native_sessions` 回主循环、恢复结果与 stock 函数一致、已修内核 defer、外来补丁／形状
不符抛错、重复安装不叠包。

隔离 PoC（临时 `HERMES_HOME`、临时环回端口、bundled 插件指到空目录；不连接、不重启主实例／
肥鱼）：

```bash
cd /tmp && PYTHONPATH=/path/to/hermes-agent \
  /path/to/hermes-agent/venv/bin/python \
  /path/to/this-repo/gateway_plugins/startup-recovery-thin/tests/poc_wiring_order.py
```

它做四件事：真实 `discover_plugins()` 从 `$HERMES_HOME/plugins` 加载本插件；真实
`APIServerAdapter.connect()` 触发 `wire`；真实 `_publish_primary_adapter` →
`recover_api_turns` 时启动图片扫描已被跳过（`db_path` 零访问＋跳过日志）；用
`git show HEAD:gateway/run_runtime.py` 的**真 stock 源**装载一份副本，证明打补丁后绑定在
worker 线程、`recover_native_sessions` 在 loop、结果与未打补丁的一致。

## 边界与后续（不在本插件内）

- 生产部署：`install_gateway_plugin.py startup-recovery-thin` → 受控重启该实例 gateway →
  启动日志里应出现 `startup-recovery-thin: media=installed native=...` 与（如触发）跳过告警。
- core clean：本机 `~/.hermes/hermes-agent/gateway/run_runtime.py` 仍处于"手工应用 PR #148062"
  的修改态（`git diff` 可见 binding 拆分 + shutdown 等待差异）。本卡**未**还原它，也未处理
  `run_runtimeb.py`；清理卡需要分别决定：binding 拆分（可由本插件接管）、shutdown 差异去留。
- 该插件只在 `api_server` adapter 连接时装一次；没有 api_server 的纯 native 网关也会在
  插件加载时 `install()`，但只有 `api_server` 连接会再校验（`wire`）。
- 跳过只发生在事件循环上的调用；将来若有新的 loop 内调用点，同样会被跳过（日志文案说的是
  启动扫描）。这是有意的窄口，不引入"仅启动期"状态机。
- 插件加载会 import 两个 gateway 模块并在进程内打补丁；在非 gateway 进程（CLI/TUI）里包装器
  没有调用点，处于惰性状态。
