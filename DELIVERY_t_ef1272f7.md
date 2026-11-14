# DELIVERY 阶段4最小收口 — 重启回归与恢复边界（t_ef1272f7）

- 分支：`feat/antigravity-acp-4`；基线 `c85f5abffdec1b28d01fad9d297e6ecb7a5e3343`
- 候选提交：**`224ec82`**（父 `c85f5ab`，只含测试：`tests/fixtures/fake_agy_acp.py` +43、`tests/test_antigravity_restart.py` 新增 320 行）
- 定案依据：计划 123「最小收口定案」与预研 `REPORT.md` §6（A：把实测固化成回归，唯一必须项）
- 边界：**未改产品代码、未部署、未重启 NB、未上传、未跑真实 `agy`／模型／QQ、未动主实例与肥鱼、未改生产配置**。
  本说明不是部署批准。真实普通续聊与中断验收由主代理在肥鱼执行。

## 1. 结论

阶段 4 的「恢复」不需要新产品代码，只需要回归把已实测语义钉住。本次新增
**7 个焦点用例**，覆盖四件事：进程中途死亡后的出站边界、跨进程 `session/resume` 契约、
`_meta` 只按哈希补一次、跨轮同文照发。

- 产品代码改动：**0 行**（`git show --stat 224ec82` 只有两个测试文件）。
- 夹具改动：`fake_agy_acp.py` **+43 行**，全部由 `--persist-sessions` 一项控制，**默认关闭**；
  缺省路径与既有 879 用例逐字节一致（全量 886 = 879 + 7，无增删）。

## 2. 为什么夹具要加一个 flag（本次唯一的基础设施变更）

`fake_agy_acp.py` 的 `_sessions` 是**进程内内存**：真实 `agy` 把会话落在
`~/.gemini/antigravity-cli/conversations/<sessionId>.db`，front/worker 换进程后
`session/resume` 仍认得原 sessionId；而原夹具在新进程里一定回 `-32602 unknown session`，
于是客户端走「重建会话」分支。**跨进程 resume 这条语义在原夹具上无法表达**。

因此加 `--persist-sessions`：把 `{sessionId: {...}}` 落到 `<log>.sessions`，进程启动时读回。
这是同一份夹具的 43 行增量，不是第二套夹具体系；不传该 flag 时
`_persist_path()` 直接返回空串，读/写都不发生。

## 3. 新增用例（`tests/test_antigravity_restart.py`，7 例）

死亡轮的构造：夹具把 reply 对半切两帧，回复取
`§\n第一段已发\n§\n第二段还没写完的尾巴内容`，第一帧恰为 `§\n第一段已发\n§\n第`
（闭合一段 + 留下未闭合的 `§\n第`），`--exit-after-chunk` 在发完第一帧后 `os._exit(8)`
（等价 front 死亡，且**不发 result**）。

| # | 用例 | 断言 | 卡面条目 |
|---|---|---|---|
| 1 | `test_dead_turn_emits_only_closed_units_and_keeps_the_session_row` | 先核验前提（客户端实收的第一帧恰为 `§\n第一段已发\n§\n第`，即死亡发生在半截正文之后）；已发单位恰 `["第一段已发"]`（只一次）；未闭合尾巴不外发；`acp_sessions` 行仍在且 `(session_id, sha(SYS), "")` 不变 | 已发不重发／pending 不外发／会话行保留 |
| 2 | `test_dead_turn_has_no_result_frame_or_stop_reason` | 该 `session/prompt` 的 RPC id 在夹具出站帧里**没有 result**；整份出站帧不出现 `stopReason` | 无伪成功／无伪造 stopReason |
| 3 | `test_next_client_resumes_same_session_without_replaying_sent_units` | 死亡后新客户端 + 重开的 DB：`session/new` 仍为 1、`session/resume` 1 条且 `sessionId` 相同；新轮只出 `["第三段"]`；`session/load` 0 条 | 重启凭 `acp_sessions` 续聊／不重放／不回灌 |
| 4 | `test_partial_body_then_death_is_a_transport_error`（真 `HermesClient.chat`） | `is_transport_error=True`、`raw_text` 以「⚠️ Antigravity 调用失败」开头、`run_id is None`、无终态正文 | 无伪成功结果／不伪造 run_id |
| 5 | `test_restart_resumes_the_same_session_and_repeats_mcp_servers` | `session/new`=1、`session/resume`=1 且 `sessionId` 复用；`resume.params.mcpServers` **逐字段等于**本轮定义；`resume.params` **无 `_meta`**；cwd 为工作区；`session/load` 0 条 | resume 复用同 session／MCP 每轮重带／哈希不变不 append |
| 6 | `test_restart_with_changed_system_prompt_appends_meta_only_that_turn` | 哈希变化那一轮 `resume.params._meta…append == SYS-B` 且库内哈希更新；**同进程下一轮**不再带 `_meta` | 变化时才 append（只 append 一次） |
| 7 | `test_restart_sends_the_same_wording_again_in_a_new_turn` | 同一句「同一段」在重启前后两轮各出一次；`session/load` 0 条 | 不过滤／不去重跨轮同文 |

用例 6 的「变化那一轮 append / 下一轮不带」是**同进程内**完成的（restart 只发生一次），
因为真实语义就是「按当前 `system_hash` 决定本轮带不带」，与进程无关。

## 4. 覆盖缺口：只补缺口，未重复造测试

| 已有覆盖 | 位置 | 本次补的缺口 |
|---|---|---|
| 同进程 resume 复用 sessionId、哈希不变不带 `_meta`、变化时 append、`-32602` 重建 | `tests/test_antigravity_acp.py::ACPResumeTests`（L308、L309、L329） | **跨进程**（DB 重开 + 新子进程）后仍复用同一 sessionId（用例 3/5） |
| `session/new` 与 `session/resume` 传同一份 mcpServers | `tests/test_antigravity_mcp_tools.py::McpSessionDefinitionTests::test_new_and_resume_carry_the_same_http_definition`（L340） | 重启后的 **resume 本身**是否仍带（用例 5：跨进程断言 resume 的 mcpServers） |
| `acp_sessions` 行跨库重开仍在（store 层） | `tests/test_antigravity_acp.py::ACPSessionMappingTests::test_acp_session_mapping_roundtrip_survives_reopen`（L40） | **客户端层**：死亡后行仍在且下一次 prompt 真的用它 resume（用例 1/3） |
| 进程死亡归一化为传输错误（`--exit-on-prompt`，**任何正文之前**就死） | `tests/test_antigravity_acp.py::HermesAntigravityChatTests::test_process_exit_is_normalized_without_replay`（L496） | **半截正文之后**才死：已发单位保留、未闭合不外发、仍落传输失败（用例 1/4） |
| 跨轮同文照发（同一进程内两轮） | `tests/test_antigravity_stream_steer.py::test_same_body_in_a_new_turn_is_delivered_again`（L233） | **跨进程**同文照发（用例 7：证明没有落盘的发送台账） |

不重复的部分：取消／steer 接管／clear 代际／审批 once·deny／图片块／MCP 工具表与自动放行，
仍由 2A/2B/2C 既有套件覆盖（本次全量一并跑过）。

## 5. 实际执行结果（原始输出全部留档）

解释器 `/home/neonsky/NeonBot/.venv/bin/python`，全部 `-B` + `PYTHONDONTWRITEBYTECODE=1`，cwd＝交付目录。

| 验证 | 实际结果 | 原始输出 |
|---|---|---|
| 新增模块（单独跑，重复 3 次） | **7 OK** ×3（0.394/0.401/0.410s） | `EVIDENCE_T_ef1272f7/repeat-x3.txt` |
| 变异核验（RED） | 基线 0 失败；M1（忽略 `acp_sessions`）2 失败；M2（死亡当成功）1 失败 | `EVIDENCE_T_ef1272f7/RED-mutations.txt` |
| focused（acp/restart/cancel/clear_fence/permission/stream_steer/mcp_tools/mcp_files/images） | **156 OK**（14.940s；＝2C3 的 149 + 本次 7） | `focused.txt` |
| 相关批（上述 + continuation/control_regressions/steer_pre_debounce/control_mcp_registration/recent_messages_private/get_message_images_contract/inline_image_limit_runs/gif_preview_frames） | **222 OK**（15.195s） | `related.txt` |
| `unittest discover -s tests -t .` 全量 | **886 OK（skip=1）**（47.812s） | `full.txt` |
| `turn_tests.test_envelope_entry` | **6 OK** | `turn.txt` |
| `verify.py`（候选 `224ec82` 的干净检出，同一 sha、树干净） | **22/22 ALL PASS**（内含全量 886 OK/skip 1） | `verify.txt` |
| `verify.py`（交付工作区，候选提交后） | 20/22：3 条失败＝pycache×2＋「树干净」，见下节 | `verify-workspace.txt` |

全量从基线 863 → 879 → **886**（本次只加 7 例，既有用例无增删），skip 仍为 1。

### 关于两次 verify

工作区 `verify.py` 的 3 条失败**全部是卫生项**，与代码无关：

1. `core/__pycache__/` 是**本次开工前就存在**的未跟踪缓存——开工第一条 `git status` 已显示
   `?? core/__pycache__/`，内含 `runs_text`／`media_payload` 的 pyc，非本次运行产生（本次所有解释器都带 `-B`）；
   它同时触发「无 pyc」×2；
2. 跑这次 verify 时交付说明与证据目录**尚未入库**（`?? DELIVERY_t_ef1272f7.md`、`?? EVIDENCE_T_ef1272f7/`），
   触发「树干净」。

清理 pycache 需要 `rm -rf`，被本机单查询模式的审批策略拒绝（`recursive delete`）；按规则**没有换命令/脚本绕过**，
改为在候选提交 `224ec82` 的**干净检出**（`git worktree add --detach`，同一 sha、无 pyc、树干净）上跑 `verify.py`，
得到 22/22，头部 `HEAD=224ec82` 即交付提交。两份原始输出都留在 `EVIDENCE_T_ef1272f7/`，可逐项对照。
该检出留在 scratch：`/home/neonsky/.hermes/profiles/coder_deepseek/cache/scratch/acp4-verify`
（主仓库里对应一条 detached worktree 登记；删目录后 `git worktree prune` 即可清掉）。

工作区磁盘上还留着两份**早期运行**的 verify 输出（`verify-workspace-before-commit.txt`、
`verify-workspace-after-commit.txt`，对应本卡改写前的临时提交），**未入库**；最终口径以上面
`verify.txt`／`verify-workspace.txt` 两份为准。

## 6. 变异核验（证明新用例不是空断言）

只做进程内 monkeypatch，**不改任何文件**：

- **M1**：`_load_session` 恒返回 `None`（不读 `acp_sessions`，每轮新建会话）→ 用例 5/6 **2 失败**
  （`session/new` 变 2、`session/resume` 缺失、sessionId 不复用）。
- **M2**：`_await_turn` 吞掉 `ACPProcessError`（把半截正文当终态返回）→ 用例 1 **1 失败**
  （`assertRaises(ACPProcessError)` 不再成立）。

## 7. 未做 / 未真实验收（明确交接）

本卡只在离线夹具与真客户端上验证，**以下均未做**，由主代理在肥鱼按计划 123 §7 矩阵执行：

1. **真实 NB 重启后的普通续聊**（矩阵 #1）：重启 → 问上一句随机词 → 复述正确、无新 `session/new`；
2. **真实 `agy` 的跨进程会话库**：用例 5 的 `--persist-sessions` 是夹具等价物，不是 agy 自身行为；
   真实证据仍以预研 §2.1（P1→P2→P3）为准；
3. **在跑轮真 `kill -9` NB**（矩阵 #4/#5）：QQ 时间线与 `acp-front/worker` 日志对齐、缺口可解释为
   `missing_open`/`missing_close`；
4. **审批挂起瞬间崩溃**（预研 §9.4）与**多会话并发中断**（§9.3）、**cwd 变化后的 resume**（§9.1）；
5. 可选切片 B（`process_dead=1` 可观测日志）**未实施**：属预研 §6.2 的可选项，本卡定案只补回归，不加产品代码。

`docs/` 仓库与计划 123 文档**未改**：其「最小收口定案」已经写清本次范围，与实现一致；
运维口径见 §8，主代理可直接并入计划文档或部署说明。

## 8. 运维口径（可原样抄进计划 123 或部署说明）

- 重启后能续聊，靠的是 `session_keys.db::acp_sessions` 里的 `(acp_session_id, system_hash, model)`
  ＋ `cwd`（插件固定工作区）；**不需要** `session/load`，也不要拿 `session/list` 当身份。
- `mcpServers` **每次 resume 都要重带**（跨进程不粘）；系统提示只在 `system_hash` 变化的那一轮
  用 `_meta.antigravity.systemPrompt.append` 补一次。
- 工作中重启是违规操作：在跑轮无法恢复，只保证「已发不重复、未闭合不补发、没有恢复成功假回执」；
  旧轮的 `session/prompt` 永远没有 `stopReason`。
- 跨轮同文是不同正文（段内去重），不要为“看起来重复”加跨轮去重或发送台账。
- 当前无后台任务生产者：ACP 不接 Hermes 后台回报，`background_report_enabled()` 在 ACP 恒 False。
