# DELIVERY t_a3297caa — queued-lane interim 候选 4 项审核返修（候选 v2，**未部署**）

> **状态：候选，未部署。** 本卡只做候选、测试与文档：没有安装插件、没有重启网关/nonebot、
> 没有注销任何生产订阅、没有发真实消息、没有上传。下面所有"实测"都是隔离进程 / 临时
> `HERMES_HOME` / 临时 SQLite 的输出；生产动作一律留到授权后的部署步骤。
>
> 沿用候选 c8c6a18 / 产品 19dcfcb 的设计，不另起方案：只修 t_71aa7781 审核评论
> （reviewer_gpt，2026-11-11 11:19）的 R1–R4。

## 0. 四问逐条对照

| 审核项 | 结论 | 落点 |
|---|---|---|
| R1 前答与既有 delta 段未隔离，`[A, A+P, C]` | **修好**：`message.interim` 成为本段的段边界，候选解析器下 `[A, P, C]`；并给出了真 client/handler 平台出口计数 | `core/hermes_client.py`（NB 解析器）+ `tests/test_queued_lane_interim_client.py` |
| R2 声称 fail-closed 的 SessionDB 门实际 fail-open | **修好**：证明不了同一 store 就 `WARNING` 拒绝、保持 stock result；三条负例补齐 | `gateway_plugins/queued-lane-interim-thin/patches.py` |
| R3 部署手册缺启用步骤、目录发现 ≠ 加载 | **修好**：手册加"目标 profile 显式启用 + 状态核验"；新增临时 home + 真 `PluginManager.discover_and_load()` 集成测试 | `…/README.md`、`…/tests/test_plugin.py` |
| R4 独立 CLI / 其它 profile worker 不继承 gateway 补丁 | **不闭合，按边界收窄并给出可证明条件**（要闭合须改上游，本卡不改） | `…/README.md` B2、`…/tests/test_plugin.py` |

## 1. R1 — interim 必须结清本段的 delta（前答不得混进后答）

**复现（同审核帧序，同一投放路径、同一真解析器族）**：`delta(A)` → queued `interim(A)`
→ 下一轮工具前 `delta(P)` → `tool.started` → `completed(C)`。

| 解析器 | 平台出口（`send_text_with_media` 实际调用） |
|---|---|
| 已部署 site-packages（stock） | `["答复A", "答复A查B前的说明", "答复C"]` |
| 本仓库候选 | `["答复A", "查B前的说明", "答复C"]`（3 次，`primary:0/1/2`） |

**改动**：`core/hermes_client.py::RunsEventParser.feed`，`message.interim` 分支在发文本前
先结清 `_seg_text`，只做前缀比对：

* `delta ⊂ interim`（含全等）→ delta 由 interim 代表，丢弃（不发两遍）；
* `interim ⊂ delta` → 先发 interim，多出的尾部留给下一个工具边界补齐；
* 两者分叉 → 各发一次，一段文本都不吞。

**为什么改在 NB 而不是插件**：插件的 `message.interim` 只携带文本，无法表达"关段"；stock
解析器的段边界只有 `tool.started`（会 `decision_batch+1`）与终态。伪造 `tool.started`
会改批次/节奏语义，因此按"最小必要"改解析器的段结清规则。**控制语义 0 改动**：不新增帧、
不动 `decision_batch`、不重置 3 秒节奏、不改封段点、不改段内全文去重。

**真 client/handler 计数**（`tests/test_queued_lane_interim_client.py`，3/3 OK）：真
`_run_reactive_turn` + 真 `HermesClient.chat` → `_chat_via_runs` → 真 SSE 消费循环 →
真 `_RunsReplyQueue` → 真 `RunTracker` 台账 / `runs_terminal` 计划 / 组件链；唯一替换的
是 HTTP 客户端（既有 `real_chat` 同形）与平台出口 `send_text_with_media`，`_runs_reply_sleep`
换即时返回（只为不真等 3 秒节奏，不参与内容判定）。三个用例：

```
test_queued_interim_then_next_delta_sends_abc_each_once ... ok      # [A,P,C]，3 次，primary:0/1/2
test_stock_parser_produces_the_reviewer_gap            ... ok      # 同一帧序换成 stock 解析器 → [A,A+P,C]
test_plain_turn_without_interim_is_unchanged           ... ok      # 无 interim → [A,C]
```

## 2. R2 — SessionDB 证明不了就拒绝

`patches._adapter_authority` 与上游 `require_active_authority(runner, store=...)` 对齐：
`_ensure_session_db` **必须可调用且返回 authority.db 本身**。缺失 / 返回 `None` / 抛异常
三条都 `WARNING "…NOT published… cannot prove its SessionDB …"`，`_RunStream` 零帧，
stock `result` 一字不动（不置 `already_sent`）。

负例三条 + 正例一条（`test_unprovable_session_db_none_is_refused` /
`test_unprovable_session_db_raises_is_refused` / `test_missing_session_db_accessor_is_refused` /
`test_same_session_db_object_is_accepted`）全部通过；旧版这三条都是 `binding_accepted=true`。

## 3. R3 — 部署手册 + 真 loader 集成测试

手册（`…/README.md`「部署（未执行）」）改为**先启用、后安装、再交接**：

1. `install_gateway_plugin.py … --force` → `--check`（只复制文件，不写 enabled）；
2. `HERMES_HOME=<目标 profile> hermes plugins enable queued-lane-interim-thin` +
   `hermes plugins list --json` 期望 `"enabled": true, "error": null`；出现
   `not enabled in config` 说明写错了 profile；
3. NB 侧同版核验（`grep -c "pending, self._seg_text = self._seg_text" …/core/hermes_client.py`
   期望 ≥1，否则 R1 未生效）；4) 重启（有权限方）；5) handover dry-run → `--apply`；
   末尾给"状态核验（只读）"两条命令。

集成测试（临时 home + 真 `PluginManager.discover_and_load()`）：

```
test_directory_alone_is_not_loaded                       ... ok   # enabled=false, error="not enabled in config"…，wrapper 未装
test_enabled_profile_loads_and_wraps                     ... ok   # enabled=true, first_response=installed + kanban guard 在场
test_enabling_then_rediscovering_installs_the_wrappers    ... ok   # 先关后开 + force=True 重扫 → wrapper 补上
```

## 4. R4 — 收窄契约（不闭合的部分明写）

* 继承只有一个 owner `hermes_cli.kanban_db._inherit_notify_subs`；`create_task`（含
  `creator_task_id` → `kanban_db_graph.inherit_creator_origin`）、`link_tasks`、`decompose`
  都经它 → 插件加载进程内全覆盖。新增两条入口测试：
  `test_late_link_inheritance_is_guarded`、`test_creator_origin_inheritance_is_guarded`（ok）。
* **不闭合**：`hermes kanban …` 是内建命令，上游 `_plugin_cli_discovery_needed()` 对
  `_BUILTIN_SUBCOMMANDS` 跳过插件发现 → 独立 CLI 进程没有围栏。真子进程实测：
  `test_independent_cli_inherits_only_what_still_exists` —— 交接前建子卡得到
  `[("api_server", …)]`，`hermes kanban notify-unsubscribe` 之后建子卡得到 `[]`。
* 收窄条件（可核验、不新增清理器/第二套通知）：自动订阅需要调用会话平台（独立 CLI 没有
  会话，`_maybe_auto_subscribe` 对其本就 no-op）；继承只复制已存在的行，部署 handover
  之后没有 api_server 行可继承；剩余唯一来源是操作员显式 `notify-subscribe`（契约允许）。
* 其它 profile 的 managed worker 与 gateway 是不同进程，同样不继承 monkeypatch——同一收窄
  条件适用（未跑活 worker，结论来自源码范围）。

## 5. 变更文件

| 文件 | 行数 | sha256(16) | 变更 |
|---|---|---|---|
| `core/hermes_client.py` | 2200 | `6225dbf26ecabcd5` | `message.interim` 段边界结清（+15/−4） |
| `gateway_plugins/queued-lane-interim-thin/patches.py` | 313 | `de7fd618456d1efb` | SessionDB 严格证明（fail-closed） |
| `…/tests/test_plugin.py` | 1100 | `416bfaa98e5598bb` | 31 → 46 项（R1 段边界、R2 负例、R3 真 loader、R4 入口/CLI 子进程） |
| `…/README.md` | 244 | `1254ddd40ee33fe3` | A2 段边界、B2 收窄契约、部署启用/核验步骤、未测边界 |
| `tests/test_queued_lane_interim_client.py` | 133 | `3f80c1ae5c803171` | 新增：真 client/handler 平台出口计数 3 项 |
| `gateway_plugins/README.md` | — | — | 条目更新为候选 v2 + NB 侧同版提示 |

上游 Hermes 仓库 **0 行改动**（本卡未碰 `/home/neonsky/.hermes/hermes-agent`）。

## 6. 真实测试输出（原始日志在 Artifacts）

| 断言 | 结果 | 原始输出 |
|---|---|---|
| 插件隔离测试 | **46 tests OK** | `EVIDENCE/queued_lane_interim_thin_v2.txt` |
| 真 client/handler 平台出口计数 | **3 tests OK** | `EVIDENCE/queued_lane_interim_client_v2.txt` |
| 仓库全量 | **613 tests OK**（上一版 610 + 本卡 3） | scratch `full_tests.log` |
| `verify.py` | **19/19**（提交后复跑） | `EVIDENCE/verify_v2.txt` |

> 运行环境注意：本机 `/tmp`（3.9G tmpfs）当时已被历史 pytest 目录占满，任何写 `/tmp` 的
> 子进程会被杀（`rc=120`）。上面所有命令都在 `TMPDIR=<profile scratch>` 下跑，命令原文见
> README「测试」一节；这不是代码问题，但复核时同样需要设置 `TMPDIR`。

## 7. 部署边界（未执行）

* 需要三件事同时到位：① 插件文件装到目标 `HERMES_HOME/plugins/`；② 目标 profile 的
  `plugins.enabled` 显式包含 `queued-lane-interim-thin`；③ **NB 侧同版**（R1 的解析器修
  正在本仓库 `core/hermes_client.py`，必须随包更新到实例 site-packages）。
* 只在上述全部到位并重启（网关 + nonebot）后才可灰度；灰度断言含"B 不以前答 A 开头"。
* 存量 58 条 `api_server` 订阅仍只 dry-run，未注销；`--apply` 是部署步骤。
* 回滚：删插件目录 + 重启；NB 侧回滚由包版本承担（无状态、无落库格式变化）。

## 8. 仍未测 / 明确不做

* 无活网关：无 HTTP/SSE 真传输、无真 OneBot 发送、无 LLM 回合（真 client 用例里 HTTP 是假的、
  SSE 行是脚本帧）；delta 帧按 `_execute_run::_text_cb` 的真实 `_run_event` 形状构造。
* 压缩续写只测 tip→root 命中与 root 无 live 的可见失败；多 profile 共享 listener 未跑。
* 媒体不随 interim 补投；同文终态去重只覆盖 `run.completed`。
* R3 未跑"重启后进程内日志断言"（需活网关），只给可核验口径；R4 未跑活 worker。
* 不改上游、不改 `_interim_cb`/`_execute_run`，不新增表/队列/恢复窗口/清理器/第二套通知。
