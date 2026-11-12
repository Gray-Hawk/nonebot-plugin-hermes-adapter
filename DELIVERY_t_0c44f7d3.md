# DELIVERY t_0c44f7d3 — Runs 内联图片超 10 张单轮降级（保留最新媒体，候选）

状态：**候选已提交，未部署、未重启、未上传。** 基线 `421d9dd`（`feat/plain-text-protocol`）。

## 1. 生产故障与根因

| 事实 | 证据 |
| --- | --- |
| 群 253384307 的 m:71446 于 2026-11-12 14:07:19 已到 NB | worker_context 生产证据 |
| 14:07:22 `POST /v1/runs` 返回 400 `invalid_params`，群里无提示（静默兜底） | 同上 |
| 触发窗口里 m:71429 含 12 张普通图片；media_config `mode=full` | 同上 |
| 上游 `gateway/session_api_media.py` `commit_api_images` 对 `data:` 图片设 `_IMAGE_LIMIT = 10`，超限即 `RuntimeStoreError('invalid_params')` | 只读复核 `/home/neonsky/.hermes/hermes-agent/gateway/session_api_media.py:9,44` |

即：插件侧把窗口内 12 张图全部内联成 `data:image/...` part 原样提交，上游按次上限 10 张直接拒整轮；
插件没有数量闸，400 落到既有静默兜底，用户侧完全无感。

## 2. 设计决策

1. **单点收口在 Runs 提交前**：唯一一处 Runs 请求体（`core/hermes_client.py::_chat_via_runs` 的
   `body["input"]`；全仓 `"input"` 另两处分别是请求转储视图与 steer 文本，均不发 runs user content）。
   被动/主动（`build_passive_user_content` / `build_reactive_user_content`）两条出站路径都汇到这里。
   completions 与 astrbot 通道不受影响（它们的上游上限/媒体通道不同，本次不动）。
2. **保留最新 10 张**：parts 顺序是旧→新，截断保留最后 10 个 data 图片 part，最早的超限部分被移除，
   顺序不变。当前/最新媒体优先留在 payload 里。
3. **只移图片 part，不动文本**：寻址标签 `[图 m:id#idx]` / `[表情包 m:id#idx]` 是独立 text part，
   一律保留，被丢的图仍可按 `m:id` 召回（不重写成另一套占位协议）。文本块、远程 URL part、
   其它任何 part 原样保留。
4. **不超限就零动作**：纯文本（str）、恰好 10 张、未达上限的 parts 原对象原样返回，
   不重建列表、不记日志。远程 `http(s)` 图片 part 不计入上限、永不被丢。
5. **一条 WARNING 只报数量**：`[media] 单轮内联图片超限：丢弃最早 N 张，保留最新 M 张（上限 10，文本与寻址标记保留）`。
   不打印 data URL、图片字节、提示词正文。
6. **不发散**：不新增配置项、不拆 run、不改 Hermes 上游、不碰交付队列/信封/steer。数量先降到 10 后，
   单图已由 `media_config.inline.max_bytes=500000` 压过，总字节远低于上游默认 128MiB，
   本次不另造总字节配置或状态。
7. **与 reclaim 一致**：截断发生在 `record_submission` 落账之前，`run_reclaim` 重放的
   `request_body` 就是同一份合规请求体。

## 3. 改动清单

| 文件 | 变更 |
| --- | --- |
| `core/inline_media_cap.py`（新，59 行） | `INLINE_IMAGE_LIMIT = 10` + `cap_inline_image_parts(content, limit=10)`：纯函数，只裁 `data:image/...` 的 image_url part |
| `core/hermes_client.py`（+7/−1） | `_chat_via_runs` 组 body 前调用 `cap_inline_image_parts(content)`；docstring 补口径（原「图片原样透传」改为带数量闸说明） |
| `tests/test_inline_image_limit_runs.py`（新，260 行，7 例） | 真实 builder → 真实 `HermesClient.chat` → 真实 `_chat_via_runs`，只在 HTTP 客户端截获真正要 POST 的 body |

## 4. 严格 TDD 证据

1. **RED（施工第一例）**：只写 `test_twelve_inline_images_keep_the_last_ten`（真实
   `build_reactive_user_content` 造 12 张 → 真实 chat 提交），跑出
   `AssertionError: Lists differ: [...IMG00, IMG01 ...] != [...IMG02 ...]`——真实出站入口确实把
   12 个 data 图片 part 原样发出去（= 生产 400 的形状）。
2. **GREEN**：新增 `cap_inline_image_parts` 并在 `_chat_via_runs` 接线后同一用例转绿。
3. **其余 6 例**（标签保留 / WARNING 计数 / 恰好 10 张 / 纯文本 / 图+表情混合 / 远程 URL）
   在基线 `421d9dd` 上同文件重跑：**5 例 FAIL、2 例 OK**（纯文本与恰好 10 张是「不改变」边界，
   基线本来就该通过，用作防过度裁剪的守卫）。

原始输出：
* 新模块：`EVIDENCE/inline_image_cap_runs.txt`（7 tests OK）
* 基线负对照：`EVIDENCE/inline_image_cap_baseline_red.txt`（Ran 7, FAILED failures=5）

## 5. 覆盖矩阵（验收口径 → 用例）

| 验收项 | 用例 |
| --- | --- |
| 12 张保留最后 10 张（旧→新） | `test_twelve_inline_images_keep_the_last_ten` |
| 文本召回标记保留（含被丢的两张） | `test_dropped_images_keep_recall_labels` |
| WARNING 报丢弃/保留数量、不泄露 payload | `test_over_limit_logs_counts_without_payload` |
| 恰好 10 张不变（同一对象、无 WARNING） | `test_exactly_ten_images_pass_through_untouched` |
| 纯文本不变 | `test_plain_text_passes_through_untouched` |
| 普通图 + 表情混合超 10 张 | `test_mixed_images_and_stickers_keep_the_last_ten` |
| 远程 URL part 保持原行为 | `test_remote_url_parts_are_not_counted_or_dropped` |

## 6. 回归与验收

| 项目 | 命令 | 结果 | 证据 |
| --- | --- | --- | --- |
| 本卡新模块 | `python -B -m unittest tests.test_inline_image_limit_runs -v` | **7/7 OK** | `EVIDENCE/inline_image_cap_runs.txt` |
| tests 全量 | `python -B -m unittest discover -s tests -t .` | **715 OK (skipped=1)** | `EVIDENCE/inline_image_cap_full.txt` |
| turn 门禁 | `python -B -m unittest discover -s turn_tests -p 'test_*.py'` | **6/6 OK** | `EVIDENCE/inline_image_cap_turn.txt` |
| 项目验收 | `python verify.py` | 见 `EVIDENCE/verify_inline_cap.txt` | 同左 |

## 7. 规模与回滚

* 生产代码净增 **66 行**（新模块 59 + `hermes_client.py` +7/−1），测试 260 行。
* 回滚：删除 `core/inline_media_cap.py` 与 `hermes_client.py` 中那一行调用（以及 import）即可；
  无配置项、无落盘状态、无数据结构变更。
