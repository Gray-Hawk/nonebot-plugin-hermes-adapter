<div align="center">
    <a href="https://v2.nonebot.dev/store">
    <img src="https://raw.githubusercontent.com/Gray-Hawk/nonebot-plugin-hermes-adapter/4d4fc0c5d7c840b4ff5f55caf498455729af26b5/NoneBotPlugin.svg" width="310" alt="logo"></a>
    </a>
    <h2>nonebot-plugin-hermes-adapter</h2>
</div>

NoneBot 插件，将 NoneBot2 桥接到 Hermes Agent 的 API Server

- 分发名：`nonebot-plugin-hermes-adapter`
- 模块名：`nonebot_plugin_hermes_adapter`
- 许可：MIT，见 [LICENSE](LICENSE)

## 安装

需要 Python 3.10 及以上与 NoneBot2，运行期依赖见 `pyproject.toml`。

## 配置

`.env` 只有 8 个 `AGENT_*` 启动项（API 地址与凭据、接口选择、MCP 监听、插件配置根目录），
逐键注释见 `.env.example`。

其余设置放在插件配置根目录（`AGENT_PLUGIN_CUSTOM_DIR`，留空则用 localstore 插件数据目录）：

`chat_config.yaml` ———— 私聊与群聊触发、防抖、活跃窗口、巡查、任务锚总开关。启动加载，管理员 `/reload-config` 手动重载
`misc_config.yaml` ———— 历史文本、记忆外挂、输出、排障、存储。手动重载
`media_config.yaml` ———— 图片投送模式、压缩梯度、缓存与限额。手动重载
`user_lists.yaml` ———— 黑名单、已知 bot、各处白名单与管理员。手动重载
`wake_limits.yaml` ———— 唤起限频令牌桶。自动热载
`feedback_rules.yaml` ———— 分级反馈、接力与拦截规则。自动热载

`user_lists.yaml` 的 `admin_users` 是唯一权限集合；可选的 `master_users` 只是它的
子集，用来把「主人」与「管理员」在聊天行身份栏里分开（缺键时整组管理员按主人呈现，
沿用 `active_session` 的 `master_ids=admin_users` 口径）。

四份设置文件在缺档时自动建带注释模板；样例同步放在仓库根目录的 `*.example.yaml`，其中前四份
逐字取自 `core/config_templates.py` 的缺档模板，避免样例与运行缺省漂移。

群聊和私聊共用 `reply_debounce_sec`，默认 `3.0` 秒，`0` 关闭普通防抖。
群聊同人续发不延长首窗，第二位其他真人插话可提前到期，bot／self 不触发提前；
私聊续发顺延，普通最长等待恒为时长 ×3（默认 9 秒）。已知 bot 的
`reply_delay_for_bot_sec` 专用延迟独立保留，首条可超过普通 ×3 上限，普通防抖为 `0` 时也可开窗。

## 用法

- 群聊：@机器人或命中关键词后对话；进入活跃窗口后，窗口内的相关话题会被按需接话。
- 私聊：直接发消息即可。
- 命令：`/clear`、`/ping`、`/help`、`/reload-config`（管理员）等，完整清单由 `/help` 动态生成。
- 需要上游反向调用消息、文件与推送工具时，在 `.env` 打开 `AGENT_MCP_ENABLED`。

## 聊天行身份栏与真实 @

群聊 reactive 与私聊 passive 的 `current_message` / `recent_messages` 共用同一套呈递：

```
[有人@我] [m:123] [主人] [id=10001 user=昵称]: 正文
[m:124] [普通用户] [id=10003 user=路人]: 正文
[m:125] [bot] [我] [id=999 user=机器人账号名]: 自己以前的回复
```

- 身份栏在 `[id=…]` 之前，优先级：`我`（`user_id == 当前 self_id`）＞ `其他BOT`
  （`known_bots` 或非当前 bot 的 bot 行）＞ `主人` / `管理员`（`admin_users`，可选
  `master_users` 细分）＞ `普通用户`（无法判权限时的降级，不猜）。
- `[有人@我]` 只来自平台事件的**结构化 At 段**，固定在该消息 `m:id` 之前；
  正文手打的 `@数字`、伪造的 `[CQ:at,…]`、引用块里转述的历史 @、@ 他人都不触发。
- At 目标随消息行落库（`messages.at_ids`，老库行 NULL = 未知），因此换轮后从历史
  重新水合仍能复现同一事实；老行绝不从正文反推。
- `[sys=…]` 合成消息不贴人类身份，也没有 m:id / @ 强提醒。
- 未给 `self_id`/`adapter` 的低层调用保持旧字节形态（离线工具与旧测试兼容）。

## QQ 官方 Bot 通道（实验样板）

除 OneBot v11/NapCat 外，插件可以额外接一路 **QQ 官方 Bot**（`nonebot-adapter-qq`，事件类
`C2CMessageCreateEvent` / `GroupAtMessageCreateEvent` / `GroupMessageCreateEvent`）。
这是**新增、可回退**的通道：不替换 NapCat，不改上游 Hermes，也不修改任何既有平台行为。

接线（实例侧，不进插件代码）：

1. `pip install nonebot-plugin-hermes-adapter[qq]`（或单独装 `nonebot-adapter-qq>=1.7.0`）；
2. 实例 `run.py` 里 `driver.register_adapter(QQAdapter)`；
3. 提供 `qq_bots`（AppID/Secret/intents），仓库内 `chat_config` 等配置不新增配额状态机。

平台差异集中在 `core/platform_compat.py`（业务代码不写 `if adapter == "qq"`）：

- **取号**：官方事件的主消息 id 是 `event.id`（没有 `message_id`），引用号在
  `QQReplyMessage.msg_idx`，正文在 `.content`。入库、触发 fragment、steer、引用对账
  走同一个 `platform_message_id()` / `reply_content()`。
- **重复投递**：同一 `event.id` 被投递两次时只起一轮（perception 与主 handler 各自
  `claim_inbound(scope, …)`，有界 TTL 300s / 4096 条）。
- **身份**：用户/群是 OpenID，与 Bot 的 AppID 没有数值关系；全量群消息里 @bot 由
  `mentions.is_you` 给出（适配器把该 mention 的 `user_id` 改写成 `bot.self_info.id`），
  所以「@ 我」只看 At 段 + `is_tome()`；`author.bot=true` 的机器人消息直接丢弃。

首版支持：C2C 私聊、群 @、开启「获取群内全部消息」后的非 @ 群消息；文字、基础图片、
§ 多段正文；会话/活跃窗/排队/交付台账全部复用现有链路。

**明确 unsupported / 降级**（能力表 `platform_compat.capabilities()` 为准，工具遇不支持
适配器时明确拒绝，不伪造成功）：

| 能力 | 官方 QQ 口径 |
| --- | --- |
| 戳一戳（`poke_user`） | unsupported → `local_rejected` |
| 撤回（`recall_message`） | unsupported → `local_rejected` |
| 群文件库（`group_files`） | unsupported → `ok=false` |
| 表情回执（429 忙信号） | 不调平台，直接跳过 |
| 合并转发（长文） | 不转发，走既有截断 |
| 本地表情库 / 本地文件直传 | unsupported（本地表情原样拒绝） |
| 审批私聊通知 | 跳过并记日志（原有 onebotv11 门控） |
| 正文受控标记 `@QQ号` | 不转义，原样当文本（官方 @ 目标是 openid） |
| 对任意历史消息显式引用 | 只保留「被动回复触发消息」语义（`msg_id`/`msg_seq` 由适配器带），不伪造引用段 |

官方主动消息额度（群 20 条/分、1000 条/天；单聊 20 条/分/用户、1000 条/天；被动回复
60 分钟 4/5 次）不在插件里建状态机：平台拒绝时按既有交付台账记失败/unknown，并在日志里
带出平台 `code`/`message`。

## 网关侧插件

Runs 车道的手动压缩控制端点由 Hermes gateway 用户插件提供（不改 Hermes 上游核心），
源码在 [`gateway_plugins/`](gateway_plugins/README.md)。安装用
`python scripts/install_gateway_plugin.py`（默认装到 `~/.hermes/plugins/session-compress`），
装完需重启该实例的 gateway 才会加载。管理员也可在 QQ 侧用 `/compress`（别名 `/压缩`）触发；
加 `--preview`（如 `/normi compress --preview`）是只读预览，回执显示“当前上下文约 占用 / 上限
tokens（百分比），N 条消息”，不建 agent、不写历史。

## 来源与归属

本项目自 [nonebot-plugin-hermes](https://github.com/gsskk/nonebot-plugin-hermes)（上游作者 gsskk）演进，
上游仍作为功能与实现参考。上游作者的版权与本项目贡献者的版权一并在 LICENSE 中保留。
