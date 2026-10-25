# nonebot-plugin-hermes-adapter

基于 [nonebot-plugin-hermes](https://github.com/gsskk/nonebot-plugin-hermes) 的 NoneBot 插件，
把群聊与私聊消息桥接到 Hermes Agent 的 API Server，由模型自行决定是否回复、怎么回复。

- 分发名：`nonebot-plugin-hermes-adapter`
- 模块名：`nonebot_plugin_hermes_adapter`
- 许可：MIT，见 [LICENSE](LICENSE)

## 来源与归属

本项目 fork 自 [nonebot-plugin-hermes](https://github.com/gsskk/nonebot-plugin-hermes)（上游作者 gsskk），
自上游 0.5.4 版本起独立演进，不再跟随上游升级，上游仍作为功能与实现参考。上游作者的版权与本项目
贡献者的版权一并在 LICENSE 中保留。

## 安装

需要 Python 3.10 及以上与 NoneBot2，运行期依赖见 `pyproject.toml`。装载前请先卸载上游
`nonebot-plugin-hermes`：两者模块名不同，同时装载会重复注册匹配器。

## 配置

`.env` 只放 8 个 `AGENT_*` 启动项（API 地址与凭据、接口选择、MCP 监听、插件配置根目录），
逐键注释见 `.env.example`。

其余设置放在插件配置根目录（`AGENT_PLUGIN_CUSTOM_DIR`，留空则用 localstore 插件数据目录）：

| 文件 | 内容 | 生效方式 |
| --- | --- | --- |
| `chat_config.yaml` | 私聊与群聊触发、防抖、活跃窗口、巡查 | 启动加载，管理员 `/reload-config` 手动重载 |
| `misc_config.yaml` | 历史文本、记忆外挂、输出、排障、存储 | 同上 |
| `media_config.yaml` | 图片投送模式、压缩梯度、缓存与限额 | 同上 |
| `user_lists.yaml` | 黑名单、已知 bot、各处白名单与管理员 | 同上 |
| `wake_limits.yaml` | 唤起限频令牌桶 | 现读现用 |
| `feedback_rules.yaml` | 分级反馈、接力与拦截规则 | 现读现用 |

四份设置文件在缺档时自动建带注释模板；样例同步放在仓库根目录的 `*.example.yaml`，其中前四份
逐字取自 `core/config_templates.py` 的缺档模板，避免样例与运行缺省漂移。

## 用法

- 群聊：@机器人或命中关键词后对话；进入活跃窗口后，窗口内的相关话题会被按需接话。
- 私聊：直接发消息即可。
- 命令：`/clear`、`/ping`、`/help`、`/reload-config`（管理员）等，完整清单由 `/help` 动态生成。
- 需要上游反向调用消息、文件与推送工具时，在 `.env` 打开 `AGENT_MCP_ENABLED`。

## 已知边界

- 以 OneBot v11 适配器为主要目标，其他适配器只保证不崩，不做功能对齐。
- 插件不做配置文件监听：改完配置需要手动重载或重启。
