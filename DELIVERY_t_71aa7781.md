# DELIVERY t_71aa7781 — queued-lane interim 修复 + Kanban api_server 自投订阅围栏（候选，**未部署**）

> **状态：候选，未部署。** 本卡只做候选、测试与文档：没有安装插件、没有重启网关、
> 没有注销任何生产订阅、没有发真实消息、没有上传。下面所有"实测"都是隔离进程/临时
> `HERMES_HOME`/临时 SQLite 的输出；生产动作一律留到授权后的部署步骤。

## 1. 交付物（逐文件）

新增 `gateway_plugins/queued-lane-interim-thin/`（仓库内网关用户插件目录，Hermes
`$HERMES_HOME/plugins/<name>/` 布局，与 `startup-recovery-thin` / `session-compress` 同构）：

| 文件 | 行数 | 内容 |
|---|---|---|
| `plugin.yaml` | 3 | manifest（discovery 必需；name/version/description）。 |
| `__init__.py` | 30 | `register(ctx)` 安装 A+B，并注册 `api_server` connect 工厂再验一次（幂等；重连不叠 wrapper）。 |
| `patches.py` | 305 | **A**：`GatewayTurnMixin._run_agent_deliver_first_response` 包装（queued-lane 前序答复 → 本 run 的 `message.interim`，含 profile/logical-owner/generation/admission/transport 六项绑定校验，失败可见且不改 stock 结果）；`_RunLaunch.put_event` 包装（`run.completed` 与已发布 interim 同文时只清空该帧 `output`）。 |
| `kanban_notify_guard.py` | 153 | **B**：`tools.kanban_tools._maybe_auto_subscribe`（api_server 调用会话 → 不写行，返回 False）、`hermes_cli.kanban_db._inherit_notify_subs`（只删本次继承的 api_server 行）、`gateway.slash_commands…_kanban_auto_subscribe`（api_server 来源 → False）。 |
| `README.md` | — | 候选说明、绑定规则、语义保留、测试命令、部署/回滚、未测边界。 |
| `tests/test_plugin.py` | 789 | 31 项隔离测试（真 stock 投放方法、真 admission 表、真 `_RunStream`、生产 `RunsEventParser`、真 Kanban 建卡/继承）。 |

修改既有文件 1 个：`gateway_plugins/README.md`（新增本插件的条目，12 行）。

上游 Hermes 仓库**0 行改动**：`git -C /home/neonsky/.hermes/hermes-agent status --porcelain`
仍只有 11-09 既存的 `M gateway/run_runtime.py` / `?? gateway/run_runtimeb.py`（本卡未动）。

sha256（前 16 位）：`patches.py 5e611ad2150fda40`、`kanban_notify_guard.py ea5e01a2ff258c3c`、
`__init__.py 9c2547078f581524`、`plugin.yaml e0470ecebc6c0aa7`、`test_plugin.py 62bb7e35e6e9d7f2`、
`README.md 7659b1374dbce70d`。

## 2. 真实测试输出

```bash
cd /tmp && PYTHONPATH=/home/neonsky/.hermes/hermes-agent \
  /home/neonsky/.hermes/hermes-agent/venv/bin/python -B -m unittest discover \
  -s /home/neonsky/nonebot-plugin-hermes/work/nph-20-work/control-mcp-20261109/gateway_plugins/queued-lane-interim-thin/tests -v
```

```
----------------------------------------------------------------------
Ran 31 tests in 1.540s

OK
```

（完整 `-v` 清单：`EVIDENCE/queued_lane_interim_thin.txt`；每条都是 `... ok`，31/31，exit 0。）

跑的是真代码：stock `_run_agent_deliver_first_response` /
`_run_agent_queued_followup`（含 `_deliver_queued_first_response` →
`_send_queued_final_text` → `adapter.send()` 在 api_server 必失败的真路径）、
`_RunLaunch.put_event` / `_RunStream` / `_run_event`、真
`begin_runtime_epoch` / `admit_session_input` / `claim_session_input` /
`get_session_admission`、生产安装的 `RunsEventParser` + `decisions_from_text`、真
`hermes_cli.kanban_db.create_task` / `_inherit_notify_subs` / `add_notify_sub`、真
`PluginManager` 目录 discovery 与 `register()`。

覆盖（对卡面要求逐条）：

* **A→steer→B→steer→C 各一次**：`test_abc_each_once_in_order`（interim 2 帧、
  `client.sent == [A,B,C]`、`suppressed == []`）；对照 `test_control_loses_every_queued_answer`
  （stock 只有 C）。
* **event-less pending_event**：`test_event_less_pending_event_followup`（真
  `_run_agent_queued_followup(pending_event=None)`，递归 1 次，平台 A、C 各一次）。
* **两 run 连续绑定**：`test_two_runs_bound_one_after_another`（真 claim：A started →
  A 流收帧、B 流空；A 结算后 B claim 到 generation+1 → B 流收帧，A 流仍只 1 帧）。
* **不同 profile / 压缩 tip 拒绝错绑**：`test_profile_mismatch_is_refused`（adapter
  绑到另一个 SessionDB → WARNING + 0 帧 + `result` 无 `already_sent`）、
  `test_compression_tip_binds_logical_root`（tip→root 命中）、
  `test_compression_tip_without_live_root_is_visible_and_safe`（root 不在
  `authority.sessions` → 可见 WARNING"No live session…NOT published" + 0 帧）、
  `test_generation_or_epoch_mismatch_is_refused`、`test_settled_admission_is_refused`、
  `test_unknown_admission_is_refused`。
* **静默 / 已流式 / 同文 / 提前返回 / hold**：`test_allowed_silence_marker_publishes_nothing`、
  `test_human_turn_silence_marker_publishes_the_stock_replacement`、
  `test_already_streamed_is_untouched`、`test_non_api_platform_is_untouched`、
  `test_muted_turn_is_untouched`、`test_terminal_same_text_is_not_repeated`（同文终态 →
  SSE `output=""`，NB 只发一次）、`test_distinct_terminal_is_unchanged`（异文一字不改）、
  `test_stale_goal_early_return_does_not_duplicate`（真 helper 返回原 result、递归 0 次、
  平台只 A 一次）。
* **SSE 入队 ≠ QQ ACK**：`test_abc_each_once_in_order` 断言投递后 `result` 无
  `already_sent`（不冒充已送达）。
* **admission 的 adapter 才拥有发布权**：`test_admitted_adapter_owns_the_publication`
  （seam 传进来的投递 adapter 是另一个实例时，仍写到准入 adapter 的 run 流）。
* **真 RunsEventParser + 平台发送计数**：`NbClient` 用生产 parser 吃 backlog 帧，
  `drain_stream_decisions()` + 真 `decisions_from_text()` 产出 unit，逐个计入
  `sent`（平台出口计数）。
* **Kanban**：`test_new_card_from_api_server_session_gets_no_subscription`（0 行）、
  `test_new_card_from_telegram_session_keeps_its_subscription`（原行为）、
  `test_child_does_not_inherit_api_server_rows`（子卡只剩 telegram，父卡两行不动）、
  `test_slash_create_from_api_server_source_is_refused` /
  `test_slash_create_from_telegram_source_still_subscribes`、
  `test_explicit_subscription_is_still_allowed`（显式 CLI/看板订阅不被拦）、
  `test_nb_report_source_still_routes_from_task_session_id`（`tasks.session_id` 仍在、
  NB `KanbanReportSource._session_for` 取到同一 id、`select_handover_subs` 仍枚举存量
  api_server 行）。
* **打包**：`test_manifest_is_discovered_as_a_user_plugin`（真目录 discovery 找到
  `queued-lane-interim-thin`）、`test_register_wires_the_api_server_factory`（真
  `register()` 注册 `api_server` 工厂；connect 时再装一次仍是幂等）。

## 3. Range B 存量候选（dry-run，未注销）

```bash
cd /home/neonsky/nonebot-plugin-hermes/work/nph-20-work/control-mcp-20261109
/home/neonsky/NeonBot/.venv/bin/python -B scripts/kanban_notify_handover.py
```

```
通知订阅共 58 条，其中需交接 58 条
- t_2725073a platform=api_server chat=hermes-onebotv11+private+158****4503-g404 mode=notify+wake
- t_291a855f platform=api_server chat=hermes-onebotv11+private+158****4503-g404 mode=notify+wake
...
- t_2b8341d9 platform=api_server chat=hermes-onebotv11+private+158****4503-g404 mode=notify+wake

(dry-run) 确认无误后加 --apply 执行注销
```

exit 0。**本卡没有加 `--apply`**；58 条存量行仍在板上，注销是部署步骤。当前没有任何
非 api_server 的通知订阅（58/58 都是 api_server），所以本插件的围栏不会影响现存
其他平台通知。

## 4. 部署步骤（未执行）

1. `python scripts/install_gateway_plugin.py queued-lane-interim-thin --hermes-home <目标实例> --force`
   （脚本不重启服务；`--check` 复核 sha256 一致）。本卡只用 `--dest` 落到 scratch 临时目录
   做过一次 stage + `--check`，输出 `installed … (5 files, sha256 verified)` / `in sync`，
   未写任何实例的 `plugins/`。
2. 由有权限的一方重启该实例网关；启动日志三行（见 README §部署）。
3. 灰度一轮（受控群 462540549，中途连发 2 条不同内容 steer）：断言 A/B/C 各一次、
   `agent.log` 2 次 `queued-lane interim published`、该 run 的 SSE 2 帧
   `message.interim` 且 `run.completed.output` 只有 C、`state.db` assistant 行数 3。
4. `python scripts/kanban_notify_handover.py` → 复核 → `--apply`。
5. 新卡验证：`kanban_create` 后 `hermes kanban notify-list --json` 不再出现该卡的
   `platform=api_server` 行；NB 仍按 `tasks.session_id` 提醒。

## 5. 回滚

删除 `$HERMES_HOME/plugins/queued-lane-interim-thin/` 并重启网关（unstall wrapper）；
Kanban 无需数据回滚（存量行保留即回到旧状态）。只回滚 B：注释掉
`__init__.py` 的 `kanban_notify_guard.install()`。

## 6. 未完成边界（明确不声称）

* **没有活网关验证**：adapter/authority/回合循环是桩（测试文件头明写桩清单），没有
  真 HTTP/SSE 传输、真 OneBot 发送、真 LLM 回合；终态帧按真实 `put_event` 入口构造。
* 压缩续写只覆盖"tip→root 命中"和"root 无 live → 可见失败"两条；`active_authority`
  多 profile / 共享 listener 现场未跑。
* `_RunStream.put_nowait` 必须在网关事件循环上调用（本补丁在 await 链上，与 stock
  `adapter.send` 同一 loop）；若挪到工作线程需改 `call_soon_threadsafe`。
* 只补文本：`MEDIA:` 附件不随 interim 补投。
* 同文终态去重只对 `run.completed` 帧生效（`run.failed` 不动）。
* 早退路径只测了 stale-goal（`_run_agent_queued_followup:1730-1735`）；`:1748-1749`
  `next_message is None`、深度上限等分支未单独跑，但插件不改变这些早退的控制流。
* Kanban 围栏只拦三个**自动**入口；显式 `notify-subscribe`/看板开关按设计不拦；存量
  注销未执行。
* 肥鱼实例当前 interface 是否 Runs 需有权限方确认后才能灰度（本卡未读其 `.env`）；
  主实例 `.env` = `runs`（既有记录）。
