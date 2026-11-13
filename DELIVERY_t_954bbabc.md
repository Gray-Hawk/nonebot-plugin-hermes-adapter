# ACP 阶段2B 最小返修交付说明（t_954bbabc）

返修卡：`t_954bbabc`（父复核卡 `t_2f755758` passed=false）。
分支：`feat/antigravity-acp-2b`；返修提交 `cc08519`（父 `02d2a37`）；产品基线仍为 `8fc8156`（父 `d64c845`）。
仅改候选代码与离线测试；**未部署、未重启、未上传、未跑真实模型／QQ、未动主实例与肥鱼**。本说明不是部署批准。

## 1. 三处修复（最小改动，不动共享正文协议／Runs FIFO／4000 字协议）

| 编号 | 根因 | 修复 | 改动点 |
|---|---|---|---|
| R1 | 取消已按 `reject_once` 成功关掉原权限 RPC，但共享审批旧单仍留在 FIFO 队首，`/approve` 永远打到这条死单，后面所有 scope 的新审批都答不到自己，直到懒过期 | 原 RPC 写帧**成功**后，按 `backend + session_key + request_id` 精确删除对应 ACP 审批条目；写帧失败／状态未知**不删**（留单可重试），Runs 条目一律不动 | `core/approvals.py: settle_permission()`；`backends/antigravity/client.py: _settle_permission()/_settle_shared_approval()` |
| R2 | prompt 超时与权限到期默认同为 300s，但权限 timer 启动更晚；prompt 先超时只发无 id cancel，收尾再撤 timer 丢权限 → 原权限 RPC 无人应答 | 超时／取消／异常收尾**先复用既有 reject_once 结清**挂起权限，再清内存与 timer（未加配置、未延长超时） | `backends/antigravity/client.py: prompt() finally` |
| R3 | `body_overflow` 先抛普通 `ACPError`，覆盖了随后真实到达的 `stopReason=cancelled`：`interrupted=False/transport_error=True`，平台出现「取消回执 → transport-fallback」，已有 QUEUED 余量继续出站 | 已确认 cancelled 时取消结果优先（正文错误只留日志），走 `ACPCancelledError → interrupted → finish(cancelled=True)`；未取消的超长正文仍原 fail-closed | `backends/antigravity/client.py: prompt()` 终态判定顺序 |

产品改动量：3 文件（2 产品 + 2 测试），净 +284/−11 行（含新增回归测试 216 行）。

## 2. 故障根因的 RED 证据（返修前，父提交 02d2a37 的原样产品代码）

在 `02d2a37` 的独立 worktree 里只放同一批新测试（产品代码一字未改）：

```
python -B -m unittest <5 个新用例> -v    → Ran 6 tests, FAILED (failures=4, errors=1)
AssertionError: 1 != 0 : 已结清的旧单不得继续占 FIFO 队首          (R1)
AssertionError: 0 != 1 : 原权限 RPC 必须收到一次安全拒绝: []        (R2)
AssertionError: False is not true : 取消结果必须优先于正文错误       (R3)
AssertionError: False is not true                                  (R3 队列余量)
ERROR: test_settle_keeps_entry_when_reject_write_fails（新函数此时还不存在）
ok: test_body_overflow_without_cancel_stays_transport_error（保留控制，本就应绿）
```

原始输出：`EVIDENCE_T_954bbabc/RED-before-fix.txt`。

## 3. 修复后实际执行结果

解释器 `/home/neonsky/NeonBot/.venv/bin/python`，全部 `-B`，cwd=候选目录。

| 验证 | 实际结果 | 原始输出 |
|---|---|---|
| 独立复核入口 `review_acceptance.py`（复核方原脚本，sha256 `6d0f9ca9…ff374`，跑 3 次） | **12/12 OK** ×3 | `EVIDENCE_T_954bbabc/independent-acceptance.txt` |
| focused（`test_antigravity_permission` + `test_antigravity_cancel`） | 34 OK（原 28 + 新增 6） | `focused.txt` |
| 相关 112 项那批（acp/stream_steer/clear_fence/permission/cancel/control_regressions/steer_pre_debounce） | 118 OK | `related.txt` |
| `unittest discover -s tests` 全量 | 820 OK（skip=1）（原 814） | `full.txt` |
| `tests._bootstrap` + `turn_tests.test_envelope_entry` | 6 OK | 见第 3 节末 |
| `verify.py` | **22/22 ALL PASS**（提交 `cc08519`、工作树干净时运行） | `verify.txt` |
| 新增 6 用例连跑 5 轮 | 每轮 OK（无节奏竞态） | 见交付会话记录 |

独立入口 12 项的逐例关键实测值（同 3 次一致）：

- R1：`FIFO_RESULT {"old": 2, "new": 3, "pending": 0, "oldest": null, "platform": ["🛑 已取消…", "✅ 审批 #3 已应答: 已放行一次"]}` —— 旧单 #2 已结清不再占队首，`/approve once` 直接答到新单 #3。
- R2：`TIMEOUT_PERMISSION_RESULT {"answers":[{"id":"timeout-id","result":{"outcome":{"outcome":"selected","optionId":"reject-once"}}}],"permissions":0}` —— 原权限 RPC 收到且只收到一次 `reject-once`。
- R3 A：`{"is_interrupted": true, "is_transport_error": false, "raw_text": "", "platform": ["🛑 已取消…"]}` —— 无 transport-fallback。
- R3 B：`{"interrupted": true, "platform": ["SENT", "🛑 已取消…"]}` —— 取消后 QUEUED 不再出站。

`auth: 6 OK` 原始行：

```
/home/neonsky/NeonBot/.venv/bin/python -B -m unittest tests._bootstrap turn_tests.test_envelope_entry -q
Ran 6 tests in 0.052s
OK
```

## 4. 明确保留的既有行为（回归覆盖，未放宽）

- 无 `approval_callback`：不登记、按 `reject_once` 本地安全拒绝；
- 原 RPC id 类型保留：字符串 id 与数字 id 均逐字节回原 id（`test_numeric_rpc_id_preserved…`、产品用例）；
- 选 allow 的依据是真实 `options` 的 `kind`，不猜 `optionId` 字面量；
- 无效输入（空/session/always/allow_once/多词/nope）：只回用法、**零后端调用**、条目保留；
- **真正发送失败/未知状态仍失败留单**（新增 `test_settle_keeps_entry_when_reject_write_fails`：写帧失败时条目保留、Runs 同 id 条目不受影响、缺键不删单）；
- 管理员并发应答共用既有 `answer_lock`，只产生一条原权限应答；
- Runs 审批/停止/恢复路径零改动（Runs 条目在 `settle_permission` 中显式跳过）；
- 未取消的普通超长正文仍 `transport_error` + 原兜底（`test_body_overflow_without_cancel_stays_transport_error`）；
- 2A 快速 B/C、EOF 代际、`§` 封段、`/clear` 栅栏：相关 118 与全量 820 复跑通过。

## 5. 证据边界

- 复现全部使用假 `agy acp` NDJSON 子进程 + 真实客户端/HermesClient/审批/命令/handler/出站代码；只替换平台发送边界、管理员白名单、隔离配置与发送节奏等待。
- R2 直接证明的是「原权限 RPC 无人应答且自动拒绝 timer 被撤销」，**不**宣称已有真实 agy 永久挂死的现场证据（与复核报告一致）。
- R1 的遗留窗口为「写帧真失败 / 子进程已死」的条目：按本轮要求保持失败留单，等懒过期；未新增跳过逻辑或第二队列。
- 未部署、未重启、未上传、未运行真实模型、未触碰主实例与肥鱼；返修自测通过不等于部署批准，交回独立复核。
