# DELIVERY 阶段2C2 — 图片输入转 ACP `image` 内容块（t_6382d66c）

- 分支：`feat/antigravity-acp-2c2`；基线 `73c9bfcdcb48dc62742bec0e4956e70c65f7f144`
- 候选提交：**`fe5f3bb`**（父 `73c9bfc`）
- 定案范围：计划 121（`alive/121_计划_ACP阶段2C2图片输入与MCP看图_2026-11-14_施工中_.md`）
- 仅改候选工作区：**未部署、未重启、未上传、未跑真实 `agy`／模型／QQ、未动主实例与肥鱼、
  未改任何生产配置**。本说明不是部署批准；肥鱼真机验收由主代理复核后执行。

## 1. 逐文件改动

候选提交净 **+432/−33**，其中产品 **+100/−25**（3 个产品文件），测试 **+332/−8**
（1 个新测试文件 + 2 个既有测试文件）。

### 1.1 `core/hermes_client.py`（+76/−18）

- 新增 `_ACP_IMAGE_DATA_URL_RE`（第 1119 行）：只认
  `data:image/<mime>;base64,<payload>`，mime 必须 `image/*`。
- 新增 `_antigravity_image_block()`（第 1124 行）：`image_url` part → ACP
  `{"type":"image","data":<base64>,"mimeType":"image/png"}`。远程 http(s) URL、
  非 `base64` 形态、空／非法 base64（`b64decode(..., validate=True)` 失败）、
  `image_url` 非 dict 一律返回 `None`（fail-closed，不另造下载）。
- `_antigravity_text_content()` 替换为 `_antigravity_content_blocks()`（第 1145 行）：
  - `str` → 单 text 块；
  - 全 text parts → 仍按 2B 同一规则合并成**单个** text 块（纯文本 wire 字节不变）；
  - `image_url` → image 块；其余类型（音频／视频／文件／resource）→ `None`；
  - 转换前先过既有 `cap_inline_image_parts()`（与 runs 出站同一个「单轮最多 10 张 data 图、
    保留最新」闸，不新增配置与状态）；
  - 块顺序就是 parts 顺序：正文 text → `[图 m:id#idx]` 标签 text → image → 下一组。
- 调用点（第 1527 行）：本地拒绝范围收窄为「非图片媒体」，回执改为
  `⚠️ Antigravity 后端暂不支持该消息形态；音频、视频、文件或未内联的图片请改用其他后端`
  （仍是 `is_local_refusal=True`，本轮不进模型、不推进水位）。
- 请求留痕（dump）的 antigravity 说明改为「文本与图片经 ACP 发送」，只改注释／提示文案。

### 1.2 `backends/antigravity/client.py`（+20/−6）

- `prompt_blocks()`（第 127 行）：`str` 行为逐字节不变；已转换的块列表按原顺序透传
  （浅拷贝，避免调用方与发送侧共享可变 dict）。**不做下载、压缩、寻址**。
- `_submit_prompt()`（第 854 行）与 `prompt(user_text=...)`（第 1020 行）类型放宽为
  `str | Sequence[dict]`，`session/prompt` 的 `prompt` 字段直接使用该块列表；steer 仍走
  `steer(text=...)` → `_submit_prompt(..., text)`，纯文本路径未改。
- 未触碰：`mcpServers` 构造（2C1）、`session/request_permission` 与 `mcp_` 自动放行、
  取消／流式／正文单位解析。

### 1.3 `core/inline_media_cap.py`（+4/−1）

- `_inline_image_url()` 增加 `image_url` 必须为 dict 的守卫：新调用方会拿「尚未校验的
  parts」先过这个闸，畸形 part 以前会在这里 `AttributeError`（runs 侧从未触发）。
  runs 侧语义不变（正常 parts 走的还是同一判断）。

### 1.4 测试

- 新增 `tests/test_antigravity_images.py`（311 行，17 个用例）：
  - 转换层：纯文本 str／全 text parts 字节行为、data URL 拆分、文本＋标签＋图片顺序、
    多图与动图帧组、>10 张保留最新、12 类坏数据／非图片 part 全部 `None`；
  - 客户端层：`prompt_blocks` 字符串不变、块列表原序透传且不共享可变 dict；
  - **真 handler→假 ACP**：`_run_passive_turn` → `HermesClient.chat` → 真
    `AntigravityACPClient` → `tests/fixtures/fake_agy_acp.py` 子进程，断言
    `session/prompt.params.prompt` 里的 text／`[图 m:1#0]`／image 块顺序与 `mimeType`，
    以及坏数据轮根本不启动 ACP（wire 为空）；另覆盖「纯文本仍只有一个 text 块」与
    「media off 群聊轮不再内联、wire 纯文本」。
- `tests/acp_2b_harness.py`（+4/−3）：`_row()`／`_turn()` 增加可选 `image_urls`（缺省 `[]`，
  2B/2C1 用例行为不变）。
- `tests/test_antigravity_acp.py`（+17/−5）：原「图片被本地拒绝」用例改为
  「非图片媒体（`input_audio`）仍被拒绝」+「远程图片 URL 仍被拒绝」，并覆盖收窄后的文案。

## 2. 与预研／卡面的差异（已知、未擅自扩大）

1. **不新建任何媒体机制**：data URL 拆分是唯一新增解析；抓取、压缩、动图抽帧、`m:id#idx`
   标签、`media_config` 三模式与 10 张上限全部复用原链。图片块数量由既有帧组决定
   （静图 1 块、GIF 按现有 `inline_gif_max_frames` 抽帧后每帧 1 块）。
2. **纯文本「字节不变」的范围**：`content` 为 `str`（媒体 off／无图轮的实际情况）时 wire
   与 2B 完全一致；`content` 为全 text parts 时仍合成单个 text 块，合并规则与旧函数相同
   （`"\n".join` 过滤空串）。
3. **坏数据 fail-closed 是整轮本地拒绝**，不是「悄悄丢一张图」：与既有 `is_local_refusal`
   语义一致（本轮不进模型、不推进增量水位），不新增回退占位符体系。
4. **不使用 resource／resource_link**，不接 `get_message_files` 与其余 12 个 MCP 工具，
   不做图片出站、不新增配置键；2C1 的两工具白名单、`mcp_` 自动审批、`run_command` 人工
   审批均未改（见回归测试）。
5. **`inline_media_cap` 的守卫**是本次唯一触碰的共享工具函数：只把畸形输入从
   `AttributeError` 变成 `None`（fail-closed），正常输入路径与 runs 出站行为不变。

## 3. RED → GREEN（先写对立测试）

在基线 `73c9bfc` 的干净 worktree（scratch 副本）只放新测试、产品代码不动，原始输出见
`EVIDENCE_T_6382d66c/RED-before-fix.txt`：

```
tests.test_antigravity_images → ERROR
  ImportError: cannot import name '_antigravity_content_blocks'
tests.test_antigravity_acp    → FAIL
  AssertionError: '暂不支持' not found in '⚠️ Antigravity 后端首片仅支持文本消息；图片或文件请改用其他后端'
```

修复后：`tests.test_antigravity_images` **17 OK**、`tests.test_antigravity_acp` **36 OK**
（同一文件合计 53 OK，`EVIDENCE_T_6382d66c/GREEN-focused.txt`）。

## 4. 实际执行结果

解释器 `/home/neonsky/NeonBot/.venv/bin/python`，全部 `-B`，cwd＝候选目录，全部在候选
`fe5f3bb` 上跑。

| 验证 | 实际结果 | 原始输出 |
|---|---|---|
| 新增 2C2 套件 + 既有 ACP 用例 | **53 OK** | `EVIDENCE_T_6382d66c/GREEN-focused.txt` |
| focused（acp/stream_steer/clear_fence/permission/cancel/mcp_tools/images） | **133 OK** | `EVIDENCE_T_6382d66c/focused.txt` |
| 相关批（上述 + control_regressions/steer_pre_debounce/control_mcp_registration/recent_messages_private/get_message_images_contract/inline_image_limit_runs/gif_preview_frames） | **186 OK** | `EVIDENCE_T_6382d66c/related.txt` |
| `unittest discover -s tests -t .` 全量 | **863 OK（skip=1）** | `EVIDENCE_T_6382d66c/full.txt` |
| `tests._bootstrap` + `turn_tests.test_envelope_entry` | **6 OK** | `EVIDENCE_T_6382d66c/turn.txt` |
| `verify.py`（产品代码与候选 `fe5f3bb` 相同；运行提交 `7a79aef`、工作树干净） | **22/22 ALL PASS** | `EVIDENCE_T_6382d66c/verify.txt`（内含 `HEAD=7a79aef`、全量 863 OK/skip 1） |

全量从基线 845 → **863**（新增 17 + 原图片拒绝用例拆成 2 个），skip 仍是 1。
2A（流式／steer 接管／clear 栅栏）、2B（once／deny／取消／双超时）、2C1（工具表、Bearer、
`mcp_` 自动放行、`run_command` 人工审批）由 focused／相关／全量批覆盖，均未改动。

## 5. 卡面验收项对照

| 卡面要求 | 落地 | 测试 |
|---|---|---|
| 复用消息入站／media_config／ImageFetcher·ImageCache／image_inline／`m:id#idx`／ChatRequest parts | 只新增 data URL→(data,mimeType) 拆分与块顺序映射；其余零新增 | 转换层 + wire 用例（标签号来自真实 `build_media_address_index`） |
| 文字与图片保持顺序；静图／多图／GIF 帧组 | parts 顺序原样映射；GIF 沿用现有抽帧帧组 | `test_multiple_images_and_gif_frame_group_keep_their_order`、`..._keep_wire_order` |
| 只放开图片；off 原行为不变；音频／视频／文件仍不支持 | `str` 与全 text parts 字节不变；`input_audio`／`video_url`／`file`／`resource` 仍 `None` | `test_non_image_or_bad_data_fails_closed`、`test_media_off_group_turn_keeps_text_only_wire` |
| 不用 resource／resource_link；不接其它工具；2C1 与 2B 审批不变 | 未改 `mcp/`、未新增配置键、未动权限链 | 2C1/2B 套件全绿（focused／related） |
| data URL 拆分、顺序、坏数据 fail-closed、off／纯文本不变 | 见 §1.4 | 17 个新用例 |
| 至少一组真 handler→fake ACP 检查 wire image 块 | `ImageWireHandlerTests` 6 例（真 handler + 真客户端 + 假子进程） | `GREEN-focused.txt` |
| 2A/2B/2C1 回归 | focused／related／full | 133／186／863 |

## 6. 未真实验收项（明确单列）

1. **肥鱼真机**：静图 canary 直读、`get_message_images` 自动审批并识图、纯文本／查记录／pwd
   人工审批回归——均未执行（本卡不动生产、不重启、不发 QQ）。
2. `media_config.yaml` 的真实取值（本卡只复用现有语义，未改生产配置）。
3. 真实 `agy` 1.5.8 对**本插件产出**的 data URL（JPEG 梯度压缩产物、GIF 抽帧帧组）的读图
   表现：预研只验证了手工构造的 PNG 单图（`ACP2C-IMG-7Q3`），多帧与 10 张上限的成本未实测。
4. ACP `image` 块在长会话／resume 后的稳定性（预研只覆盖单轮）。
5. 超大图与 HEIC 等未知 mime：现有 `image_inline` 会压到 JPEG，未知格式在抓取阶段已失败，
   ACP 侧只按 `image/*` 透传；真实账号表现未验。

## 7. 边界声明与回滚

- 测试使用假 `agy acp` NDJSON 子进程 + 真实客户端／HermesClient／handler／媒体组装／出站；
  只替换内联抓图（`inline_image_frames`）、平台发送边界、管理员白名单与隔离配置。
- 测试里的图片字节全是自造的离线 canary（`acp2c2-canary` / `frame-N`），不是任何真实图片。
- 候选是独立分支上的单个提交 `fe5f3bb`（父 `73c9bfc`）；回滚只需把该提交从部署树撤下，
  不涉及数据迁移、配置改动或外部状态（无新端口、无新凭据、无新库表）。产品侧
  `_antigravity_content_blocks` → 旧 `_antigravity_text_content` 即可恢复「图片本地拒绝」。
- 未部署、未重启、未上传；复核通过后再合回 ACP 分支并部署肥鱼。
