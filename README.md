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

`chat_config.yaml` ———— 私聊与群聊触发、防抖、活跃窗口、巡查。启动加载，管理员 `/reload-config` 手动重载
`misc_config.yaml` ———— 历史文本、记忆外挂、输出、排障、存储。手动重载
`media_config.yaml` ———— 图片投送模式、压缩梯度、缓存与限额。手动重载
`user_lists.yaml` ———— 黑名单、已知 bot、各处白名单与管理员。手动重载
`wake_limits.yaml` ———— 唤起限频令牌桶。自动热载
`feedback_rules.yaml` ———— 分级反馈、接力与拦截规则。自动热载

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
