# queued-lane-interim-thin — 候选 v2（**未部署**）

> **状态：候选 / 未部署。** 本目录是正式候选实现与测试，只在仓库与一次性临时
> `HERMES_HOME` 里验证过；没有安装进任何实例的 `$HERMES_HOME/plugins/`，没有重启
> 网关，没有改动生产 Kanban 订阅，也没有上传。本版是 t_a3297caa 对 t_71aa7781 候选
> 审核（R1–R4）的返修，边界见文末"部署 / 收窄契约"。

三件事：网关侧两处窄补丁（用户插件，**不改 Hermes 上游文件**）、NB 解析器一处段边界
修正（本仓库 `core/hermes_client.py`）、以及 Kanban 自动订阅围栏。

1. **queued-lane 前序答复**：`/v1/runs` 回合里，轮末还排着 steer 跟进时，stock 的
   queued lane 把这一份 first_response 交给 `ApiServerAdapter.send()`——HTTP
   请求/响应桩，必然失败，文本写进 state.db 后原地丢弃，`run.completed` 只带最后
   一份（生产 09:49 私聊连发 3 条 steer 只到末份）。本插件把这一份按 `_execute_run`
   的 `_interim_cb` **同一事件形状**投到**本回合 admission 绑定的那个 run** 自己的
   `_RunStream`，由 NB 既有分段发送逻辑发出；同文的 `run.completed` 不再重复。
2. **段边界（R1）**：`message.interim` 在 NB 侧同时是**段边界**——本段已到的 delta
   必须在这里结清，否则这段文本会在下一个工具边界随 `_seg_text` 再发一遍，前答混进
   后答（实测 `[A, A+P, C]`）。修正点是解析器，不是插件：只发文本的一帧无法自己关段。
3. **Kanban api_server 自投订阅**：新卡不再产生 `platform=api_server` 的自动订阅
   （那一路只能自投内部 `/v1/chat/completions`，QQ 上永远收不到）。NB
   `core.kanban_report_source` 照旧从 `tasks.session_id` 路由到后台汇报信箱，真实
   会话里跑成 turn。存量订阅的注销是部署步骤（`scripts/kanban_notify_handover.py`），
   不在插件里做，插件也不加轮询清理器。

## 文件

| 文件 | 作用 |
|---|---|
| `plugin.yaml` | Hermes 用户插件 manifest（discovery 必需）。 |
| `__init__.py` | `register(ctx)`：启动时安装两处补丁，并注册 `api_server` connect 工厂在首个 run 准入前再验一次（幂等）。 |
| `patches.py` | A：`GatewayTurnMixin._run_agent_deliver_first_response` 包装 + `_RunLaunch.put_event` 终态同文去重。 |
| `kanban_notify_guard.py` | B：三个**自动**订阅入口的围栏（`kanban_create` 自动订阅、子卡继承、`/kanban create`）。 |
| `core/hermes_client.py`（仓库主代码，非插件内） | R1：`RunsEventParser` 把 `message.interim` 当段边界，结清本段已到的 delta。 |
| `tests/test_plugin.py` | 46 项隔离测试（真 stock 投放方法、真 admission 表、真 `_RunStream`、**候选与已部署两份真解析器**、真 `PluginManager` 加载、真 `hermes kanban create` 子进程）。 |
| `tests/test_queued_lane_interim_client.py`（仓库 `tests/`） | 3 项真 client/handler 平台出口计数（真 `HermesClient.chat`、真 `_run_reactive_turn`、真台账/计划/组件链，只替 HTTP 与发送出口）。 |

## A. 绑定与语义（为什么不是"猜 active run"）

`bound_run(adapter, session_id)` 只用既有事实，逐条校验，任一不成立就**不发布**并留
`WARNING`（"这根本不是 /v1/runs 回合"除外，只记 debug）：

1. `api_execution` 里的 **本次 admission 的 adapter**（`prepare_api_execution` 登记，
   NB 回合的 `platform=api_server` 就是它）；
2. `active_authority(adapter.gateway_runner)` 与 `adapter._ensure_session_db()` 必须是
   同一个 `SessionDB`——上游准入门 `require_active_authority(runner, store=...)` 的同一
   条 profile 围栏。**证明不了就拒绝**：访问器缺失、返回 `None`、抛异常，一律
   `WARNING` 不发布（R2；旧版把这三条当"放行"，是 fail-open）。
3. `authority.logical_owner(turn_ctx.session_id)`：压缩续写时 admission 身份在 lineage
   **根**上，物理 tip 不参与查表（tip 查不到 → 可见失败，不换 key 乱试）；
4. `live.event_stream.execution` 的 `admission_id` / `authority_epoch` /
   `execution_generation`（`SessionAuthority._stamp_claim` 写的）；
5. `get_session_admission(db, admission_id=...)` 必须 `status == "started"`、
   `request_id` 是 `run_*`、`target_session_id == logical owner`、`owner_epoch` /
   `generation` 与 (4) 一致；
6. `adapter._run_streams[run_id]` 仍在（run 仍拥有 transport）。

`turn_ctx.run_generation` 是会话状态里的轮次代号，**不是** admission 的
`generation`，所以校验用的是 admission 行与 execution 戳这两个同源事实，不拿二者
互相比。

语义保留：

* `mute_notification_reply`（通知 hold）→ 不动；
* 非 `api_server` 平台 → 不动；
* `result["already_sent"]`（stock 真投递成功 / 已流式确认）→ 不发布，避免重复；
* `interrupted` → 不动（stock 本来就不进这一步）；
* 静默：允许的静默（`reply_expected is False` / 机器 display kind）不发布；人类回合
  的静默标记按 stock 换成 `_unexpected_silence_reply()` 再发布，不泄漏标记本身；
* 提前返回（stale-goal / `next_message is None` / 深度上限）语义不变：插件只在
  `:1713` 之后追加一次 SSE 入队，不碰 `result`，不改控制流；
* **SSE 入队不是 QQ ACK**：`result["already_sent"]` 绝不置位，落库/回执口径不变。

**同文终态不重复**：发布过的 interim 文本记在**该 run 自己的 `_RunStream` 上**
（`_queued_lane_interim_texts`，随 run 生命周期消失，不是全局表、没有恢复窗口）。
`run.completed` 帧的 `output` 与已发布 interim 同文时只清空**这一帧**的 `output`；
`_set_run_status`/admission 结果里的 output 不动（轮询 `GET /v1/runs/{id}` 仍拿到全文）。
这条同时覆盖"跟进轮早退、同一答案又将以终态发出"的既有重复（本卡之前补证过的
A/A 路径）。

## A2. 段边界：interim 必须结清本段 delta（R1）

真帧序（`delta(A)` → queued `interim(A)` → 下一轮工具前 `delta(P)` → `tool.started` →
`completed(C)`）在**两个真解析器**上的实测：

| 解析器 | 平台出口（`send_text_with_media` 实际调用） |
|---|---|
| 已部署（stock） | `["答复A", "答复A查B前的说明", "答复C"]`——3 次，前答混进后答 |
| 候选（本仓库） | `["答复A", "查B前的说明", "答复C"]`——3 次，`primary:0/1/2` |

stock 的 `message.interim` 只把文本发出去，`_seg_text` 里的 delta 缓冲留着，下一个
`tool.started` 封段时把「A+P」整段再发一遍。候选解析器把 interim 当**段边界**，用
前缀比对结清：

* `delta ⊂ interim`（含全等）：delta 由 interim 代表，丢弃——不发两遍；
* `interim ⊂ delta`：先发 interim，多出的尾部留给下一个工具边界补齐；
* 两者分叉：各发一次，一段文本都不吞。

**控制语义不动**：不新增帧、不动 `decision_batch`、不重置 3 秒节奏、不改 `tool.started`
封段点，也不改 `_emit_text` 的段内全文去重。没 delta 时行为与今天完全一致。

## B. Kanban：只保留 NB 插件通知

| 自动入口 | 处置 |
|---|---|
| `tools.kanban_tools._maybe_auto_subscribe` | 调用会话是 `api_server` 时直接返回 `False`（`kanban_create` 结果里 `subscribed=false`，与上游"无持久通道"同形）。 |
| `hermes_cli.kanban_db._inherit_notify_subs` | 继承后只删掉**本次刚复制**的 `api_server` 行；父卡自己的行不动，其他平台行原样。 |
| `gateway.slash_commands.GatewaySlashCommandsMixin._kanban_auto_subscribe` | 事件来源是 `api_server` 时返回 `False`，不再写行、也不再谎报"已订阅"。 |

不动：通知器（`gateway/kanban_watchers*`）、dispatcher、worker、看板首页订阅
（`plugins/kanban/dashboard`，用户点出来的真实 home 平台）、CLI/看板的**显式**
`notify-subscribe`（那是操作员决定，本插件只拦自动路径）、其他平台的自动订阅。

存量 `api_server` 订阅（本机实测 58 条，`scripts/kanban_notify_handover.py` dry-run）
由部署步骤用一个上游公开 CLI 命令批量注销；NB 的 `KanbanReportSource` 不依赖这些行。

### B2. 独立 CLI 与插件未加载进程（R4，收窄契约）

继承只有一个owner：`hermes_cli.kanban_db._inherit_notify_subs`。`create_task`（含
`creator_task_id` → `kanban_db_graph.inherit_creator_origin`）、`link_tasks`、
`decompose` 都走它，所以插件加载的进程里上述入口全部被围栏覆盖（有测试）。

**闭不上的一层**：`hermes kanban …` 是内建命令，上游 `_plugin_cli_discovery_needed()`
对 `_BUILTIN_SUBCOMMANDS` 直接跳过插件发现（`hermes_cli/main.py`），所以独立 CLI 进程
里没有围栏，子卡会继承父卡**仍然存在**的 `api_server` 行——本版实测复现，不掩饰。
要在这里闭合只能改上游（插件发现策略或继承语义），按本卡边界**不改**。

可证明的收窄条件（同一条部署链）：

1. 自动订阅需要**调用会话平台**（`_resolve_notify_target` → `HERMES_SESSION_PLATFORM`），
   独立 CLI 没有会话，`_maybe_auto_subscribe` 对它本就是 no-op；
2. 继承只复制**已存在**的行；部署步骤用公开 CLI 注销存量 58 条之后，没有 `api_server`
   行可以再被自动复制；
3. 于是"自动产生 api_server 行"的所有入口都消失：插件加载进程被围栏拦下，未加载进程
   既不能自己产生、也无可继承。**剩余的唯一来源是操作员显式 `notify-subscribe`**，
   那是契约允许的显式决定。

真 `hermes kanban create` 子进程的前后对照见 `tests/test_plugin.py::
IndependentCliCreateTest`（交接前继承 1 行 → `notify-unsubscribe` 后 0 行）。

## 测试

```bash
# 插件侧 46 项（必须离开仓库根目录跑：根目录 utils.py 会遮蔽 Hermes 的 utils）
cd /tmp && PYTHONPATH=/home/neonsky/.hermes/hermes-agent \
  /home/neonsky/.hermes/hermes-agent/venv/bin/python -B -m unittest discover \
  -s <repo>/gateway_plugins/queued-lane-interim-thin/tests -t <repo>/gateway_plugins/queued-lane-interim-thin/tests -v

# 真 client/handler 平台出口计数 3 项（仓库测试套内）
cd <repo> && PYTHONDONTWRITEBYTECODE=1 /home/neonsky/NeonBot/.venv/bin/python -B \
  -m unittest tests.test_queued_lane_interim_client -v
```

当前结果见 `DELIVERY_t_a3297caa.md` 的原始输出：插件 **46/46 OK**、真 client/handler
**3/3 OK**、仓库全量 **613/613 OK**（`verify.py` 19/19）。覆盖面在上一版 31 项基础上新增：

* R1：`delta(A)+interim(A)+delta(P)+tool.started` 在**候选**解析器下 `[A,P,C]`、在
  **已部署**解析器下 `[A, A+P, C]`（同一帧序、同一投放路径的前后对照）；delta 是
  interim 的真前缀、interim 是 delta 的真前缀、无 delta 三种边角；
* R2：`_ensure_session_db` 缺失／返回 `None`／抛异常三条负例都 `WARNING` 拒绝、run
  stream 零帧、stock result 原样；同一对象仍放行；
* R3：临时 home + 真 `PluginManager.discover_and_load()`：只有目录 → `enabled=false,
  error="not enabled in config"`、wrapper 未装；写了 `plugins.enabled` → 真加载、
  wrapper 装上；先关后开 + `force=True` 重扫 → wrapper 补上；
* R4：`link_tasks` 迟到继承、`inherit_creator_origin` 两条也算自动入口并被围栏覆盖；
  真 `hermes kanban create` 子进程在交接前后各建一张子卡的订阅行对照。

## 部署（未执行）

顺序是**先安装、后启用、再交接**；目录发现 ≠ 已加载（R3）。

```bash
HOME_DIR=/path/to/hermes-home          # 目标 profile 的 HERMES_HOME

# 1) 插件文件（脚本只复制，不写 enabled、不重启）
python scripts/install_gateway_plugin.py queued-lane-interim-thin --hermes-home "$HOME_DIR" --force
python scripts/install_gateway_plugin.py queued-lane-interim-thin --hermes-home "$HOME_DIR" --check

# 2) 目标 profile 显式启用（上游 gate 只认 plugins.enabled；缺省/空 = placeholder）
HERMES_HOME="$HOME_DIR" hermes plugins enable queued-lane-interim-thin
HERMES_HOME="$HOME_DIR" hermes plugins list --json | grep -A3 queued-lane-interim-thin
#    期望 "status": "enabled"；若显示 "not enabled" → 回到本步
#    status 只证明配置选择；实际 wrapper 安装由下方启动日志核验

# 3) NB 侧同版（R1 的解析器修正随插件包发布：site-packages 里的 core/hermes_client.py
#    必须是本候选版本），装完后 `hermes` 与 nonebot 都要重启
grep -c "pending, self._seg_text = self._seg_text" \
  /home/neonsky/NeonBot/.venv/lib/python3.13/site-packages/nonebot_plugin_agent_adapter/core/hermes_client.py
#    期望 ≥1；输出 0 说明 NB 侧还是旧包 → R1 未生效

# 4) 重启该实例网关 + nonebot（由有权限的一方执行）。启动日志应出现：
#    queued-lane-interim-thin: first_response=installed terminal_dedupe=installed
#    queued-lane-interim-thin: kanban guard auto_subscribe=installed inherit=installed slash=installed
#    queued-lane-interim-thin: verified at api_server connect: ...（每行一次）
#    若只有 discovery 日志、没有 install 日志，说明插件没被启用（回第 2 步）

# 5) 存量 api_server 自投订阅交接（部署步骤，先 dry-run 再加 --apply）
python scripts/kanban_notify_handover.py
python scripts/kanban_notify_handover.py --apply
```

状态核验（部署后一次性，只读）：

```bash
HERMES_HOME="$HOME_DIR" hermes plugins list --json | grep -A3 queued-lane-interim-thin
HERMES_HOME="$HOME_DIR" hermes kanban notify-list --json     # 期望不再有 platform=api_server
```

灰度断言：受控群里一轮中途连发 2 条不同内容的 steer → NB 依次发 A、B、C 各一次，
**且 B 不以前答 A 开头**（R1 的目标形态）；`agent.log` 出现 2 次
`queued-lane interim published`；该 run 的 SSE 有 2 帧 `message.interim` 且
`run.completed.output` 只有 C；`state.db` 该 session assistant 行数 3。负例：普通一轮
（无 steer）0 次 published、1 条消息。新卡（`kanban_create`）不再出现
`platform=api_server` 的 `kanban_notify_subs` 行，NB 仍能按 `tasks.session_id` 提醒。

## 回滚

删插件目录 + 重启该实例网关（adapter 重连即卸载 wrapper）；NB 解析器回滚同理由包版本
承担（段边界修正无状态、无落库格式变化，回滚不改数据）。Kanban 侧无需回滚数据
（存量行保留时只是回到"多一条无效自投行"的旧状态）。若只想回滚 B，把
`__init__.py` 里 `kanban_notify_guard.install()` 那行去掉再重启。

## 明确不做 / 未测

- 不改 Hermes 上游、不改 `_interim_cb`、不改 `_execute_run`，不新增表/队列/恢复窗口/run 映射；
- R1 的唯一非插件改动在**本仓库**的 NB 解析器（`core/hermes_client.py`），因为"只发文本
  的一帧"无法自己关段；上表已列，部署需 NB 侧同版；
- R4 的独立 CLI / 插件未加载进程不闭合（要改上游）；闭合条件是部署链上的 handover，
  见 B2，**本版不复现为"已闭合"**；
- 只发文本：排队答复里的 `MEDIA:` 附件不随 interim 补投（媒体补投单独立项）；
- 没有活网关验证：adapter/authority/回合循环在测试里是桩（明写在测试头），HTTP/SSE
  传输、真 OneBot 发送、LLM 回合未跑；终态帧按 `_RunLaunch.put_event` 真实入口构造；
  delta 帧按 `_execute_run::_text_cb` 的真实 `_run_event(..., "message.delta")` 形状构造
  （真 client/handler 那一组用例里走的是真 `HermesClient.chat` SSE 消费循环）；
- 压缩续写只测了"tip→root 命中"与"root 不在 `authority.sessions` → 可见失败"两条，
  `active_authority` 的多 profile 现场（共享 listener `/p/<profile>/`）未在真网关跑过；
- 同文终态去重只作用于 `run.completed` 帧；`run.failed` 的 output 不清理（NB 不发
  failed 的 output）；
- `_RunStream.put_nowait` 必须在网关事件循环线程上调用：本补丁位于 await 链条的
  coroutine 里（与 stock `adapter.send` 同一条 loop），若将来挪进工作线程需改成
  `loop.call_soon_threadsafe`（`_interim_cb` 同款）；
- R3 的 loader 集成测试用真 `PluginManager` + 临时 home，但没跑"重启后进程内日志断言"
  那一步（需要活网关）；文档给出的是可核验的日志/`plugins list --json` 口径；
- 肥鱼实例当前 interface 是否 Runs 仍需有权限方确认后才能灰度（本卡未读其 `.env`）。
