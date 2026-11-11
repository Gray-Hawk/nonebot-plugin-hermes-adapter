# queued-lane-interim-thin — 候选（**未部署**）

> **状态：候选 / 未部署。** 本目录是正式候选实现与测试，只在仓库与一次性临时
> `HERMES_HOME` 里验证过；没有安装进任何实例的 `$HERMES_HOME/plugins/`，没有重启
> 网关，没有改动生产 Kanban 订阅，也没有上传。

两件事，都是网关侧用户插件（**不改 Hermes 上游文件**）：

1. **queued-lane 前序答复**：`/v1/runs` 回合里，轮末还排着 steer 跟进时，stock 的
   queued lane 把这一份 first_response 交给 `ApiServerAdapter.send()`——HTTP
   请求/响应桩，必然失败，文本写进 state.db 后原地丢弃，`run.completed` 只带最后
   一份（生产 09:49 私聊连发 3 条 steer 只到末份）。本插件把这一份按 `_execute_run`
   的 `_interim_cb` **同一事件形状**投到**本回合 admission 绑定的那个 run** 自己的
   `_RunStream`，由 NB 既有分段发送逻辑发出；同文的 `run.completed` 不再重复。
2. **Kanban api_server 自投订阅**：新卡不再产生 `platform=api_server` 的自动订阅
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
| `tests/test_plugin.py` | 31 项隔离测试（真 stock 投放方法、真 admission 表、真 `_RunStream`、生产 `RunsEventParser`、真 Kanban 建卡/继承）。 |

## A. 绑定与语义（为什么不是"猜 active run"）

`bound_run(adapter, session_id)` 只用既有事实，逐条校验，任一不成立就**不发布**并留
`WARNING`（"这根本不是 /v1/runs 回合"除外，只记 debug）：

1. `api_execution` 里的 **本次 admission 的 adapter**（`prepare_api_execution` 登记，
   NB 回合的 `platform=api_server` 就是它）；
2. `active_authority(adapter.gateway_runner)` 与 `adapter._ensure_session_db()` 必须是
   同一个 `SessionDB`——上游准入门 `admit_api_steps` 用的同一条 profile 围栏；
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

## 测试

```bash
cd /tmp && PYTHONPATH=/home/neonsky/.hermes/hermes-agent \
  /home/neonsky/.hermes/hermes-agent/venv/bin/python -B -m unittest discover \
  -s /home/neonsky/nonebot-plugin-hermes/work/nph-20-work/control-mcp-20261109/gateway_plugins/queued-lane-interim-thin/tests -v
```

必须离开本仓库根目录跑（根目录的 `utils.py` 会遮蔽 Hermes 的 `utils`）。当前结果
**31 tests, OK**（`DELIVERY_t_71aa7781.md` 有原始输出）。覆盖：control 只到 C、
A→steer→B→steer→C 各一次、event-less `pending_event`、同 session 两 run 连续绑定、
压缩 tip→logical root、tip 无 live 的可见失败、profile/SessionDB 不一致拒绝、
generation/epoch 不一致拒绝、已结算/未知 admission 拒绝、非 api 平台、已流式、
hold、允许静默、人类静默替换、无 admission 戳的普通 API 回合、同文终态、异文终态、
stale-goal 早退、manifest discovery、register/connect 工厂幂等；Kanban 侧覆盖
自动订阅两种平台、子卡继承、`/kanban create` 两种来源、显式订阅仍允许、NB
`session_id` 路由、handover 选择器。

## 部署（未执行）

```bash
# 1) 从仓库安装到目标实例（脚本不重启服务、不写配置）
python scripts/install_gateway_plugin.py queued-lane-interim-thin \
  --hermes-home /path/to/hermes-home --force
python scripts/install_gateway_plugin.py queued-lane-interim-thin --check

# 2) 重启该实例网关（由有权限的一方执行）
#    启动日志应出现：
#    queued-lane-interim-thin: first_response=installed terminal_dedupe=installed
#    queued-lane-interim-thin: kanban guard auto_subscribe=installed inherit=installed slash=installed
#    queued-lane-interim-thin: verified at api_server connect: ...（每行一次）

# 3) 存量 api_server 自投订阅交接（部署步骤，先 dry-run 再加 --apply）
python scripts/kanban_notify_handover.py
python scripts/kanban_notify_handover.py --apply
```

灰度断言：受控群里一轮中途连发 2 条不同内容的 steer → NB 依次发 A、B、C 各一次；
`agent.log` 出现 2 次 `queued-lane interim published`；该 run 的 SSE 有 2 帧
`message.interim` 且 `run.completed.output` 只有 C；`state.db` 该 session assistant
行数 3。负例：普通一轮（无 steer）0 次 published、1 条消息。新卡
（`kanban_create`）不再出现 `platform=api_server` 的 `kanban_notify_subs` 行，NB 仍
能按 `tasks.session_id` 提醒。

## 回滚

删除插件目录并重启该实例网关（adapter 重连即卸载 wrapper）；Kanban 侧无需回滚数据
（存量行保留时只是回到"多一条无效自投行"的旧状态）。若只想回滚 B，把
`__init__.py` 里 `kanban_notify_guard.install()` 那行去掉再重启。

## 明确不做 / 未测

- 不改上游、不改 `_interim_cb`、不改 `_execute_run`，不新增表/队列/恢复窗口/run 映射；
- 只发文本：排队答复里的 `MEDIA:` 附件不随 interim 补投（媒体补投单独立项）；
- 没有活网关验证：adapter/authority/回合循环在测试里是桩（明写在测试头），HTTP/SSE
  传输、真 OneBot 发送、LLM 回合未跑；终态帧按 `_RunLaunch.put_event` 真实入口构造；
- 压缩续写只测了"tip→root 命中"与"root 不在 `authority.sessions` → 可见失败"两条，
  `active_authority` 的多 profile 现场（共享 listener `/p/<profile>/`）未在真网关跑过；
- 同文终态去重只作用于 `run.completed` 帧；`run.failed` 的 output 不清理（NB 不发
  failed 的 output）；
- `_RunStream.put_nowait` 必须在网关事件循环线程上调用：本补丁位于 await 链条的
  coroutine 里（与 stock `adapter.send` 同一条 loop），若将来挪进工作线程需改成
  `loop.call_soon_threadsafe`（`_interim_cb` 同款）；
- 肥鱼实例当前 interface 是否 Runs 仍需有权限方确认后才能灰度（本卡未读其 `.env`）。
