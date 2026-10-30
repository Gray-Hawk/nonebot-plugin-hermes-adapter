"""配置注释模板与 v6 目标态键说明（离线单点真源）。

本模块不依赖 nonebot，仅需标准库 + PyYAML（插件运行环境已装），可被离线
冒烟脚本与迁移工具直接导入。职责：

- 定义四份 YAML（chat/misc/media/user_lists）与一份 .env（AGENT_*）的
  “完整默认模板”，每个设置键逐个功能注释，键与键之间恰好至少一个空行；
  media 保留既有嵌套结构（inline/fetch/sticker），user_lists 保留
  private_allow 并新增被动 allow_users / allow_groups / admin_users。
- 定义 v6 目标态 56 键的规格（文件、默认值、类型），供对账与迁移取值。
- 定义旧 82 键的完整处置表（迁移 / 撤并 / 固定 / 改名），供迁移工具与
  “82 键映射无遗漏”冒烟对账共用，保证不出现两处各自维护的漂移。

模板排版要求（见 S2 计划 §九.11）只约束“缺档建档模板”：键间至少一个
空行、逐键注释、按需标注单位 / 可选值 / 重启生效。手动重载不得重排或覆盖
用户已有文件——本模块只产出字符串，不写盘、不改运行读点。
"""

from __future__ import annotations

import re
from typing import Any

import yaml

# --------------------------------------------------------------------------- #
# 节点模型：把“逐键注释 + 键间空行”的排版要求落到数据结构里，模板与迁移
# 输出共用同一套渲染器，杜绝手写两份模板漂移。
# --------------------------------------------------------------------------- #


class _Header:
    """文件头注释块（原样多行，不加缩进）。"""

    def __init__(self, lines: list[str]):
        self.lines = lines


class _Section:
    """分节横幅（如“# ---- 私聊：2 键 ----”）。"""

    def __init__(self, title: str):
        self.title = title


class _Key:
    """一个扁平设置键。target=True 计入 v6 目标 56 键；False 为外围既有键。

    optional=True 且取值为 None 时，渲染成注释掉的示例行（用于 private_allow：
    未配置 = 回落管理员集合，模板不能写显式空表，否则把语义改成“拒绝所有人”）。
    """

    def __init__(
        self,
        name: str,
        default: Any,
        comment: list[str],
        *,
        type_: str = "str",
        target: bool = True,
        optional: bool = False,
        example: Any = None,
    ):
        self.name = name
        self.default = default
        self.comment = comment
        self.type = type_
        self.target = target
        self.optional = optional
        self.example = example


class _Group:
    """嵌套映射（media 的 inline / fetch / sticker）。children 均为 _Key。"""

    def __init__(self, name: str, comment: list[str], children: list[_Key]):
        self.name = name
        self.comment = comment
        self.children = children


# --------------------------------------------------------------------------- #
# 值渲染
# --------------------------------------------------------------------------- #


def _dump(value: Any) -> str:
    """把 Python 值渲染成 YAML 单行片段（列表用 flow 风格，字典用块风格）。

    PyYAML 6.0.x 对标量文档会额外附上显式文档结束符 ``...``，这里统一剥掉，
    否则模板会被解析成多文档导致 safe_load 报错。
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple, set)):
        return yaml.safe_dump(
            list(value), allow_unicode=True, default_flow_style=True, sort_keys=False
        ).strip()
    if isinstance(value, dict):
        return yaml.safe_dump(
            value, allow_unicode=True, default_flow_style=True, sort_keys=False
        ).strip()
    s = yaml.safe_dump(value, allow_unicode=True, default_flow_style=True)
    if s.endswith("...\n"):
        s = s[:-4]
    return s.strip()


def _comment_lines(lines: list[str], indent: int) -> list[str]:
    pad = "  " * indent
    return [pad + "# " + ln for ln in lines]


def _key_block(node: _Key, value: Any, indent: int) -> list[str]:
    pad = "  " * indent
    out = _comment_lines(node.comment, indent)
    if node.optional and value is None:
        example = node.example if node.example is not None else node.default
        out.append(f"{pad}# {node.name}: {_dump(example)}")
    else:
        out.append(f"{pad}{node.name}: {_dump(value)}")
    return out


def _group_block(node: _Group, values: dict, indent: int = 0) -> list[str]:
    pad = "  " * indent
    out = _comment_lines(node.comment, indent)
    out.append(f"{pad}{node.name}:")
    for child in node.children:
        out.append("")
        out.extend(_key_block(child, values.get(child.name, child.default), indent + 1))
    return out


def _yaml12_safe_loader():
    """构造 YAML 1.2 语义的安全加载器：on/off/yes/no 保持字符串，只认 true/false。

    迁移读既有 media/user YAML 必须用这个——PyYAML 默认 SafeLoader 走 YAML 1.1，
    会把 ``mode: off`` 强转成 False，重渲染后丢值（off 变 false）。
    """

    class Loader(yaml.SafeLoader):
        pass

    bool_tag = "tag:yaml.org,2002:bool"
    resolvers: dict[str, list] = {}
    for first, entries in Loader.yaml_implicit_resolvers.items():
        kept = [(tag, rx) for tag, rx in entries if tag != bool_tag]
        if kept:
            resolvers[first] = kept
    Loader.yaml_implicit_resolvers = resolvers
    Loader.add_implicit_resolver(
        bool_tag,
        re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"),
        list("tTfF"),
    )
    return Loader


def load_yaml(text: str):
    """YAML 1.2 语义安全加载（on/off/yes/no 不转布尔）。"""
    return yaml.load(text, Loader=_yaml12_safe_loader())


def render_yaml(spec: list[Any], values: dict) -> str:
    """按 spec 渲染带注释 YAML。values 为顶层取值（缺键回退 default）。"""
    blocks: list[list[str]] = []
    for node in spec:
        if isinstance(node, _Header):
            blocks.append(list(node.lines))
        elif isinstance(node, _Section):
            blocks.append([f"# ---- {node.title} ----"])
        elif isinstance(node, _Key):
            blocks.append(_key_block(node, values.get(node.name, node.default), 0))
        elif isinstance(node, _Group):
            blocks.append(_group_block(node, values.get(node.name, {}) if isinstance(values.get(node.name), dict) else {}))
    return "\n\n".join("\n".join(b) for b in blocks if b).rstrip() + "\n"


# --------------------------------------------------------------------------- #
# .env 规格（8 键，AGENT_ 前缀）
# --------------------------------------------------------------------------- #

ENV_SPEC: list[tuple[str, Any, str, list[str]]] = [
    (
        "AGENT_API_URL",
        "http://127.0.0.1:8642",
        "str",
        ["默认上游 Agent 接入点，全部群聊与私聊统一使用。"],
    ),
    (
        "AGENT_API_KEY",
        "",
        "str",
        ["全局上游 API 鉴权密钥。仅写在 .env，不写入任何 YAML、不回显到日志。"],
    ),
    (
        "AGENT_API_TIMEOUT",
        300,
        "int",
        ["默认 API 请求超时秒数（Agent 执行可能较慢）。"],
    ),
    (
        "AGENT_API_INTERFACE",
        "completions",
        "str",
        [
            "API 接口选择：completions / runs / astrbot。取代旧 runs_channel_enabled 布尔键，",
            "默认沿用现行缺省 completions；旧 True 映射为 runs；AstrBot 仅开放基础对话。",
        ],
    ),
    (
        "AGENT_MCP_ENABLED",
        False,
        "bool",
        ["是否启动内嵌 MCP server，供上游反向调用消息、文件与推送工具。重启生效。"],
    ),
    (
        "AGENT_MCP_HOST",
        "127.0.0.1",
        "str",
        ["内嵌 MCP server 监听地址。重启生效。"],
    ),
    (
        "AGENT_MCP_PORT",
        8643,
        "int",
        ["内嵌 MCP server 监听端口。重启生效。"],
    ),
    (
        "AGENT_PLUGIN_CUSTOM_DIR",
        "",
        "str",
        [
            "插件配置与记忆文件根目录；留空使用 localstore 默认目录。由旧 custom_dir",
            "改名，是定位其他 YAML 的自举例外。",
        ],
    ),
]

# --------------------------------------------------------------------------- #
# 四份 YAML 规格
# --------------------------------------------------------------------------- #

YAML_SPECS: dict[str, list[Any]] = {
    "chat_config.yaml": [
        _Header(
            [
                "# Hermes 聊天配置（chat_config.yaml）。",
                "# 落位：<AGENT_PLUGIN_CUSTOM_DIR>/chat_config.yaml（未配置该目录时用 localstore 插件数据目录）。",
                "# 加载：启动缓存 + 管理员手动重载；除逐键标注“重启生效”外不做文件监听。",
                "# 坏档：完整校验失败时保留旧缓存。键间空行仅为排版，可自由增删。",
            ]
        ),
        _Section("私聊：2 键"),
        _Key(
            "private_trigger",
            "all",
            ["私聊响应范围：all = 全部用户；allowlist = 仅白名单（名单本体在 user_lists.yaml 的 allow_users）。"],
            type_="str",
        ),
        _Key(
            "reply_debounce_private_sec",
            2.5,
            ["私聊触发后的防抖等待秒数，用于合并连发碎片；0 = 立即起跑。"],
            type_="float",
        ),
        _Section("群聊触发、历史与回复：10 键"),
        _Key(
            "group_trigger",
            "keyword",
            [
                "群聊激活模式：keyword = 关键词隐式激活 + @／引用；at = 只认 @／引用。",
                "非法值一律按 keyword 处理并 WARN 一次。",
            ],
            type_="str",
        ),
        _Key(
            "keywords",
            ["/ai"],
            ["keyword 模式下可隐式激活对话的关键词集合（命中即隐式激活）。"],
            type_="list",
        ),
        _Key(
            "anchor_max_age_sec",
            150,
            ["委托锚最长寿命秒数，超时自动释放，防止模型一直忘记松锚。"],
            type_="int",
        ),
        _Key(
            "reply_debounce_group_sec",
            3.5,
            ["群聊触发后的防抖等待秒数；0 = 立即起跑。"],
            type_="float",
        ),
        _Key(
            "reply_delay_for_bot_sec",
            0.0,
            [
                "只对已知 bot 强制设置的最低回复延迟秒数，即使普通防抖关闭也会开窗；",
                "由 reply_delay_sec 改名。0 = 关闭。",
            ],
            type_="float",
        ),
        _Key(
            "long_reply_forward",
            True,
            ["群聊超长回复是否用合并转发发送完整内容；关闭则截断。"],
            type_="bool",
        ),
        _Key(
            "rate_limit_emoji_id",
            96,
            ["429 重试仍失败时贴在群聊触发消息上的表情 ID，用作轻量忙信号。"],
            type_="int",
        ),
        _Key(
            "active_session_timeout_sec",
            0,
            [
                "原 T5：活跃窗口只靠隐式续期时的最长存活秒数，0 = 不设此上限；",
                "由 active_session_t5_sec 改名。",
            ],
            type_="int",
        ),
        _Key(
            "active_session_ttl_sec",
            300,
            ["活跃窗口滑动 TTL 秒数。重启生效。"],
            type_="int",
        ),
        _Key(
            "history_message_limit",
            10,
            [
                "聊天历史条数上限；统一接管增量补历史、关键词软上下文、主动巡查三条链路，",
                "取代 perception_buffer / keyword_soft_context / proactive_max_messages。",
                "MCP 工具不走这个键（见 mcp_recent_limit_max）。",
            ],
            type_="int",
        ),
        _Section("公私共用：3 键"),
        _Key(
            "ignore_prefix",
            ["."],
            ["以这些前缀开头的消息不触发回复；实际作用于私聊与群聊。"],
            type_="list",
        ),
        _Key(
            "reply_debounce_max_wait_sec",
            8.0,
            ["公私聊共用的防抖最长等待秒数，从首条消息起算，防止不断顺延。"],
            type_="float",
        ),
        _Key(
            "session_idle_reset_sec",
            0,
            [
                "原 T1：公私聊多久无活动后静默重置会话，0 = 关闭；",
                "手动重载时重建任务。",
            ],
            type_="int",
        ),
        _Section("主动回复巡查：4 键"),
        _Key(
            "proactive_interval_sec",
            0,
            ["原 T2：定时隐性激活的基础检查间隔秒数，0 = 关闭；手动重载时重建调度。"],
            type_="int",
        ),
        _Key(
            "proactive_jitter_sec",
            0,
            ["原 T3：每次定时检查可附加的随机偏移上限秒数。"],
            type_="int",
        ),
        _Key(
            "proactive_window_sec",
            300,
            ["原 T4：每次定时激活向前回看新消息的时间窗口秒数。"],
            type_="int",
        ),
        _Key(
            "proactive_groups",
            [],
            ["主动发言群范围（adapter:group_id 复合 id）；空集 = BotRegistry 已知的全部群。"],
            type_="list",
        ),
        _Section("工具、转发与通知：4 键"),
        _Key(
            "mcp_recent_limit_max",
            50,
            [
                "get_recent_messages 工具单次最多返回多少条群消息；作为 MCP 专用上限独立保留，",
                "不并入聊天历史。MCP server 启用时该工具强制注册，不另设开关。",
            ],
            type_="int",
        ),
        _Key(
            "forward_extract_max_nodes",
            10,
            ["合并转发最多展开多少个节点（由媒体域迁入聊天域）。"],
            type_="int",
        ),
        _Key(
            "forward_extract_max_chars",
            800,
            ["合并转发展开文本的总字符上限（由媒体域迁入聊天域）。"],
            type_="int",
        ),
        _Key(
            "greet_on_join",
            False,
            ["群里有人加入时是否触发 reactive turn，由模型自行决定是否欢迎。"],
            type_="bool",
        ),
    ],
    "misc_config.yaml": [
        _Header(
            [
                "# Hermes 杂项配置（misc_config.yaml）：历史文本、记忆外挂、输出、排障、后台任务与存储。",
                "# 落位：<AGENT_PLUGIN_CUSTOM_DIR>/misc_config.yaml（未配置该目录时用 localstore 插件数据目录）。",
                "# 加载：启动缓存 + 管理员手动重载；除逐键标注“重启生效”外不做文件监听。",
            ]
        ),
        _Section("历史、记忆与输出：10 键"),
        _Key(
            "history_text_max_chars",
            800,
            [
                "单条历史文本在入库和模型渲染时共用的字符上限；取消“入库 200、渲染 800”双层口径，",
                "不提供 0 = 无限制。由 perception_text_length 与旧硬编码 800 合并。",
            ],
            type_="int",
        ),
        _Key(
            "group_profile_enabled",
            True,
            ["群档案注入及相关画像路由开关；不影响反馈规则等其他定制功能。"],
            type_="bool",
        ),
        _Key(
            "sticker_enabled",
            False,
            [
                "本地表情库总开关，重启生效；关闭时不注册工具、不注入提示词，也不解析表情标记。",
                "缺省关闭；表情目录与 index.csv 范本仍在启动时缺档创建。",
            ],
            type_="bool",
        ),
        _Key(
            "max_length",
            4000,
            ["单条回复最大字符数，与超长截断或合并转发行为联动。"],
            type_="int",
        ),
        _Key(
            "transport_error_fallback_text",
            "",
            [
                "上游 4xx、5xx 与传输错误的统一用户侧兜底文本；默认空串表示静默。",
                "内部日志与既定重试仍保留，不被兜底文本开关吞掉。",
            ],
            type_="str",
        ),
        _Key(
            "interim_echo_enabled",
            False,
            ["是否实时转发长任务中工具调用间的旁白，默认关闭。"],
            type_="bool",
        ),
        _Key(
            "help_menu_file",
            "",
            ["/help 菜单头尾自定义文件路径；中间命令清单仍由插件动态生成。空 = 使用内置动态菜单。"],
            type_="str",
        ),
        _Key(
            "prompt_dump_enabled",
            False,
            ["是否在本地保存每轮完整 system prompt 供排障（含画像与主机路径，仅存本地）。"],
            type_="bool",
        ),
        _Key(
            "approval_expiry_sec",
            300,
            ["/approve 挂起审批超时秒数，默认 300，与上游默认审批超时一致。"],
            type_="int",
        ),
        _Key(
            "prompt_timezone",
            "",
            ["模型上下文时间戳的时区覆盖（IANA 名，如 Asia/Shanghai）；留空使用部署机本地时区。"],
            type_="str",
        ),
        _Section("后台任务与存储：5 键"),
        _Key(
            "delivery_poll_interval_sec",
            30,
            [
                "后台完成结果汇报轮询间隔秒数；新旧投递通道共用。重启生效。",
            ],
            type_="int",
        ),
        _Key(
            "bot_name",
            "",
            ["多 bot 共存时的全局指令寻址前缀。重启生效。"],
            type_="str",
        ),
        _Key(
            "storage_db_path",
            "",
            ["消息库等持久化 Store 的数据库路径；缺省跟随 AGENT_PLUGIN_CUSTOM_DIR，可独立覆盖。重启生效。"],
            type_="str",
        ),
        _Key(
            "storage_message_retention_days",
            30,
            ["消息库保留天数，超龄记录在 vacuum 时删除。"],
            type_="int",
        ),
        _Key(
            "storage_message_max_rows",
            100_000,
            ["消息库最大行数，超限时从最旧记录开始清理。"],
            type_="int",
        ),
    ],
    "media_config.yaml": [
        _Header(
            [
                "# Hermes 媒体投送配置（media_config.yaml）：图片投送管线三模式 + v6 迁入的 7 键。",
                "# 落位：<AGENT_PLUGIN_CUSTOM_DIR>/media_config.yaml（未配置该目录时用 localstore 插件数据目录）。",
                "# 加载：启动缓存 + 管理员手动重载；除逐键标注“重启生效”外不做文件监听。",
                "# 坏档：完整校验失败时保留旧缓存；单字段非法值回退默认。",
            ]
        ),
        _Section("既有媒体 schema（三模式改造）"),
        _Key(
            "mode",
            "semi",
            ["投送总模式：full = 全内联直通；semi = 窗口内有 @bot 才内联；off = 全部占位符。"],
            type_="str",
            target=False,
        ),
        _Key(
            "semi_window",
            3,
            ["semi 模式的窗口半径：当前消息前后各 N 条内存在 @bot 才内联。"],
            type_="int",
            target=False,
        ),
        _Group(
            "inline",
            ["内联压缩与字节门（逐级从原图重压）。"],
            [
                _Key("ladder", [[2048, 80], [1024, 70]], ["内联压缩梯度 [[max_edge, quality], ...]。"], type_="list", target=False),
                _Key("max_bytes", 500_000, ["内联单图压缩后字节上限（大小门），梯度走完仍超则回退占位符。"], type_="int", target=False),
                _Key("gif_max_frames", 5, ["内联动图抽帧上限。"], type_="int", target=False),
            ],
        ),
        _Group(
            "fetch",
            ["主动拉图门与压缩梯度。"],
            [
                _Key("gate_bytes", 2_000_000, ["主动拉图大小门：原图不超过则直出，超过走 fetch 梯度压缩。"], type_="int", target=False),
                _Key("ladder", [[4096, 95], [4096, 70]], ["拉图撞门后的压缩梯度（与内联独立）。"], type_="list", target=False),
                _Key("gif_max_frames", 10, ["拉图动图抽帧上限。"], type_="int", target=False),
            ],
        ),
        _Group(
            "sticker",
            ["表情门控。"],
            [
                _Key("gate_bytes", 200_000, ["表情门控：不超过直出，超过强制走 fetch 梯度压缩。"], type_="int", target=False),
            ],
        ),
        _Key(
            "forward_image_cap",
            30,
            ["单条消息合并转发图的注册上限（按内容去重）；转发图恒不内联。"],
            type_="int",
            target=False,
        ),
        _Key(
            "exemptions",
            {},
            ["例外名单：用户 id → off（永不内联，可拉图）/ semi（按半内联规则收窄）。全局生效。"],
            type_="dict",
            target=False,
        ),
        _Section("v6 迁入的 7 键（旧 .env IMAGE/MEDIA 系列）"),
        _Key(
            "media_allow_dirs",
            "",
            ["额外允许发送的本地文件与图片根目录白名单（os.pathsep 分隔，支持 ~）。"],
            type_="str",
        ),
        _Key(
            "local_file_max_mb",
            50,
            ["本地文件通过群文件或私聊文件上传时的尺寸上限（MB）。"],
            type_="int",
        ),
        _Key(
            "sticker_vision_enabled",
            False,
            ["是否抓取收藏表情并内联给模型，默认关闭。"],
            type_="bool",
        ),
        _Key(
            "image_cache_dir",
            "",
            ["图片字节缓存根目录；缺省跟随 AGENT_PLUGIN_CUSTOM_DIR，也可独立覆盖。重启生效。"],
            type_="str",
        ),
        _Key(
            "image_cache_quota_mb",
            200,
            ["图片缓存总容量上限（MB），超出后按访问时间从旧到新淘汰。重启生效。"],
            type_="int",
        ),
        _Key(
            "image_fetch_timeout_s",
            10,
            ["单张图片 HTTP 抓取超时秒数。重启生效。"],
            type_="int",
        ),
        _Key(
            "image_fetch_max_attempts",
            2,
            ["单张图片总抓取尝试次数，包含首次尝试。重启生效。"],
            type_="int",
        ),
    ],
    "user_lists.yaml": [
        _Header(
            [
                "# Hermes 统一用户设置清单（user_lists.yaml）：名单表按数据内容命名，不改名 config。",
                "# 落位：<AGENT_PLUGIN_CUSTOM_DIR>/user_lists.yaml（未配置该目录时用 localstore 插件数据目录）。",
                "# 加载：启动缓存 + 管理员手动重载；坏档完整校验失败时保留旧缓存。",
                "# id 只允许数字/字母/下划线/横线；识别不了的条目自动跳过。",
            ]
        ),
        _Key(
            "blacklist",
            {},
            [
                "黑名单：{群号: [账号id, ...]}——名单账号的消息不置 explicit/addressed（仅群聊生效；私聊不受影响）。",
                "语义 = 不主动响应，消息照常入库、窗内跟帖照常可见。",
            ],
            type_="dict",
            target=False,
        ),
        _Key(
            "known_bots",
            [],
            [
                "已知 bot 账号表（裸 id，全局）——名单账号的发言不钉委托锚（防 bot 互相接话挤占锚位/刷锚龄），",
                "仍可被正常响应。",
            ],
            type_="list",
            target=False,
        ),
        _Key(
            "private_allow",
            None,
            [
                "私聊主动下行白名单（裸 id，全局）——允许被主动下行私聊的账号，唤起（wake_turn）与直发（push_message）",
                "两通道共用。与下方 allow_users（被动聊天白名单，复合 id）语义不同，不得合并。",
                "未配置本键时回落管理员集合（admin_users 复合键按当前适配器拆前缀取裸 id）；",
                "显式配置（含空表 []）以显式值为准——显式空表 = 不允许任何人被主动下行。",
                "需要限制时取消注释并填写真实账号。",
            ],
            type_="list",
            target=False,
            optional=True,
            example=[],
        ),
        _Key(
            "allow_users",
            [],
            [
                "私聊 allowlist 模式的被动聊天白名单（{adapter}:{user_id} 复合 id）。",
                "仅当 chat_config.yaml 的 private_trigger = allowlist 时生效；与 private_allow 语义不同，不得合并。",
            ],
            type_="list",
        ),
        _Key(
            "allow_groups",
            [],
            ["允许响应的群集合（{adapter}:{group_id} 复合 id）；空集 = 全部群。"],
            type_="list",
        ),
        _Key(
            "admin_users",
            [],
            [
                "敏感命令与管理操作的管理员集合（{adapter}:{user_id} 复合 id）；空集 = 默认 deny。",
                "手动重载先按旧集合鉴权，成功后再替换新集合。",
            ],
            type_="list",
        ),
    ],
}

# --------------------------------------------------------------------------- #
# 目标态 56 键规格（派生自 ENV_SPEC + YAML_SPECS，单一真源）
# --------------------------------------------------------------------------- #


def _build_target_keys() -> dict[str, dict]:
    out: dict[str, dict] = {}
    for name, default, type_, _ in ENV_SPEC:
        out[name] = {"file": ".env", "default": default, "type": type_}
    for fname, spec in YAML_SPECS.items():
        for node in spec:
            if isinstance(node, _Key) and node.target:
                out[node.name] = {"file": fname, "default": node.default, "type": node.type}
    return out


TARGET_KEYS: dict[str, dict] = _build_target_keys()

# 按文件分组的 56 键名单（对账用）
TARGET_BY_FILE: dict[str, dict[str, dict]] = {}
for _k, _v in TARGET_KEYS.items():
    TARGET_BY_FILE.setdefault(_v["file"], {})[_k] = _v


# --------------------------------------------------------------------------- #
# 旧 82 键处置表（有序，保证与 config.py 字段一一对应）
# kind 取值：
#   env / chat / misc / media / user_lists -> 迁移到目标键
#   merge_history -> 并入 history_message_limit（三键冲突需报错）
#   merge_text    -> 与硬编码 800 合并为 history_text_max_chars
#   deleted       -> 撤键删除（无目标）
#   fixed         -> 撤键并固定行为（无目标）
# --------------------------------------------------------------------------- #

OLD_KEYS: dict[str, tuple[str, str | None, str]] = {
    "api_url": ("env", "AGENT_API_URL", ""),
    "api_key": ("env", "AGENT_API_KEY", ""),
    "api_timeout": ("env", "AGENT_API_TIMEOUT", ""),
    "interim_echo_enabled": ("misc", "interim_echo_enabled", ""),
    "steer_reply_push_enabled": ("fixed", None, "跟随 AGENT_API_INTERFACE：runs 通道自动开启，其他接口不开启"),
    "steer_reconcile_enabled": ("fixed", None, "固定开启 run 终态 steer 对账补投"),
    "media_allow_dirs": ("media", "media_allow_dirs", ""),
    "local_file_max_mb": ("media", "local_file_max_mb", ""),
    "group_endpoints": ("deleted", None, "拆除多接入点路由；需要多上游时部署多实例"),
    "group_trigger": ("chat", "group_trigger", ""),
    "keywords": ("chat", "keywords", ""),
    "bot_name": ("misc", "bot_name", ""),
    "private_trigger": ("chat", "private_trigger", ""),
    "allow_users": ("user_lists", "allow_users", ""),
    "allow_groups": ("user_lists", "allow_groups", ""),
    "admin_users": ("user_lists", "admin_users", ""),
    "session_share_group": ("fixed", None, "固定群 session 共享模式"),
    "honcho_enabled": ("deleted", None, "随 honcho 记忆链删除"),
    "group_sessions_per_user": ("deleted", None, "随 honcho 记忆链删除"),
    "group_session_key_format": ("deleted", None, "随 honcho 记忆链删除"),
    "group_per_user_session_key_format": ("deleted", None, "随 honcho 记忆链删除"),
    "private_session_key_format": ("deleted", None, "随 honcho 记忆链删除"),
    "max_length": ("misc", "max_length", ""),
    "ignore_prefix": ("chat", "ignore_prefix", ""),
    "perception_enabled": ("fixed", None, "固定开启消息感知与入库"),
    "perception_buffer": ("merge_history", "history_message_limit", "并入 history_message_limit"),
    "perception_text_length": ("merge_text", "history_text_max_chars", "与硬编码 800 渲染截断合并为 history_text_max_chars"),
    "perception_image_mode": ("fixed", None, "固定保留历史图片 [图片] 占位"),
    "buffer_per_group_cap": ("deleted", None, "上游早已废弃的每群内存缓冲条数上限，零运行读点"),
    "buffer_total_groups_cap": ("deleted", None, "上游早已废弃的跨群缓冲容量与 LRU 阈值，零运行读点"),
    "active_session_enabled": ("fixed", None, "固定启用活跃窗口模式"),
    "active_session_ttl_sec": ("chat", "active_session_ttl_sec", ""),
    "active_sweep_interval_sec": ("fixed", None, "过期窗口清扫间隔固定 30 秒"),
    "poke_trigger_enabled": ("fixed", None, "固定开启戳一戳触发，语义等同 @ 的隐式激活"),
    "greet_on_join": ("chat", "greet_on_join", ""),
    "reactive_post_reply_cooldown_sec": ("deleted", None, "旧 post-reply 冷却闸整体删除，不新增替代键"),
    "prompt_timezone": ("misc", "prompt_timezone", ""),
    "reply_debounce_group_sec": ("chat", "reply_debounce_group_sec", ""),
    "reply_debounce_private_sec": ("chat", "reply_debounce_private_sec", ""),
    "reply_debounce_max_wait_sec": ("chat", "reply_debounce_max_wait_sec", ""),
    "reply_delay_sec": ("chat", "reply_delay_for_bot_sec", "改名：只服务 bot 延迟"),
    "reply_delay_users": ("deleted", None, "额外强制延迟名单；延迟只保留给已知 bot"),
    "help_menu_file": ("misc", "help_menu_file", ""),
    "custom_dir": ("env", "AGENT_PLUGIN_CUSTOM_DIR", "改名：插件配置根目录"),
    "group_profile_enabled": ("misc", "group_profile_enabled", ""),
    "prompt_dump_enabled": ("misc", "prompt_dump_enabled", ""),
    "blacklist": ("deleted", None, "旧 Config 字段已迁入 user_lists.yaml，零运行读点"),
    "anchor_max_age_sec": ("chat", "anchor_max_age_sec", ""),
    "known_bots": ("deleted", None, "旧 Config 字段已迁入 user_lists.yaml，零运行读点"),
    "keyword_soft_context": ("merge_history", "history_message_limit", "并入 history_message_limit"),
    "session_idle_reset_sec": ("chat", "session_idle_reset_sec", ""),
    "active_session_t5_sec": ("chat", "active_session_timeout_sec", "改名：移除内部 T5 代号"),
    "proactive_interval_sec": ("chat", "proactive_interval_sec", ""),
    "proactive_jitter_sec": ("chat", "proactive_jitter_sec", ""),
    "proactive_window_sec": ("chat", "proactive_window_sec", ""),
    "proactive_max_messages": ("merge_history", "history_message_limit", "并入 history_message_limit"),
    "proactive_groups": ("chat", "proactive_groups", ""),
    "delivery_poll_interval_sec": ("misc", "delivery_poll_interval_sec", ""),
    "transport_error_fallback_text": ("misc", "transport_error_fallback_text", ""),
    "mcp_enabled": ("env", "AGENT_MCP_ENABLED", ""),
    "mcp_host": ("env", "AGENT_MCP_HOST", ""),
    "mcp_port": ("env", "AGENT_MCP_PORT", ""),
    "mcp_recent_limit_max": ("chat", "mcp_recent_limit_max", ""),
    "storage_db_path": ("misc", "storage_db_path", ""),
    "storage_message_retention_days": ("misc", "storage_message_retention_days", ""),
    "storage_message_max_rows": ("misc", "storage_message_max_rows", ""),
    "image_cache_dir": ("media", "image_cache_dir", ""),
    "image_cache_quota_mb": ("media", "image_cache_quota_mb", ""),
    "image_fetch_timeout_s": ("media", "image_fetch_timeout_s", ""),
    "image_fetch_max_attempts": ("media", "image_fetch_max_attempts", ""),
    "image_inline_enabled": ("deleted", None, "三模式改造后由 media_config.yaml 的 mode 接管"),
    "image_inline_max_edge": ("deleted", None, "由 media_config.yaml 的 inline.ladder 接管"),
    "image_inline_quality": ("deleted", None, "由 media_config.yaml 的 inline.ladder 接管"),
    "image_inline_max_bytes": ("deleted", None, "由 media_config.yaml 的 inline.max_bytes 接管"),
    "image_inline_total_max_bytes": ("deleted", None, "三模式改造后已无对应机制"),
    "sticker_vision_enabled": ("media", "sticker_vision_enabled", ""),
    "forward_extract_max_nodes": ("chat", "forward_extract_max_nodes", ""),
    "forward_extract_max_chars": ("chat", "forward_extract_max_chars", ""),
    "long_reply_forward": ("chat", "long_reply_forward", ""),
    "runs_channel_enabled": ("env", "AGENT_API_INTERFACE", "改名 + 布尔改枚举 completions/runs"),
    "rate_limit_emoji_id": ("chat", "rate_limit_emoji_id", ""),
    "approval_expiry_sec": ("misc", "approval_expiry_sec", ""),
}

# 三条历史条数链路的旧键（并入 history_message_limit），默认均为 10
HISTORY_LIMIT_SOURCES = ("perception_buffer", "keyword_soft_context", "proactive_max_messages")
HISTORY_LIMIT_DEFAULT = 10

# perception_text_length 旧默认（入库 200）与旧硬编码渲染截断 800
TEXT_MAX_INPUT_DEFAULT = 200
TEXT_MAX_RENDER_LEGACY = 800
TEXT_MAX_DEFAULT = 800


def render_default_template(filename: str) -> str:
    """渲染某份 YAML 的完整默认模板（缺档建档用）。"""
    spec = YAML_SPECS[filename]
    values = {}
    for node in spec:
        if isinstance(node, _Key):
            values[node.name] = node.default
        elif isinstance(node, _Group):
            values[node.name] = {c.name: c.default for c in node.children}
    return render_yaml(spec, values)


def known_top_level_keys(filename: str) -> set[str]:
    """某份 YAML 规格覆盖的顶层键名集合（含 Group 名）。"""
    spec = YAML_SPECS[filename]
    keys: set[str] = set()
    for node in spec:
        if isinstance(node, (_Key, _Group)):
            keys.add(node.name)
    return keys


def render_migrated_yaml(filename: str, values: dict) -> str:
    """渲染带注释 YAML，并原样保留 values 中未建模的顶层自定义键（追加在末尾）。

    迁移输出用：媒体/名单文件里若有人加了规格之外的自定义键，不能因为重渲染而丢掉。
    """
    text = render_yaml(YAML_SPECS[filename], values)
    known = known_top_level_keys(filename)
    extra = [k for k in values if k not in known]
    if extra:
        blocks = ["# 既有自定义键（未参与 v6 迁移，原样保留）"]
        for k in extra:
            blocks.append(f"{k}: {_dump(values[k])}")
        text = text.rstrip() + "\n\n" + "\n\n".join(blocks) + "\n"
    return text


TEMPLATES: dict[str, str] = {fname: render_default_template(fname) for fname in YAML_SPECS}

# 固定导出常量名（运行加载器按名引用）
CHAT_CONFIG_TEMPLATE: str = TEMPLATES["chat_config.yaml"]
MISC_CONFIG_TEMPLATE: str = TEMPLATES["misc_config.yaml"]
MEDIA_CONFIG_TEMPLATE: str = TEMPLATES["media_config.yaml"]
USER_LISTS_TEMPLATE: str = TEMPLATES["user_lists.yaml"]


def render_env_default() -> str:
    """渲染 AGENT_* .env 默认模板（8 键，逐键注释 + 键间空行）。"""
    blocks: list[list[str]] = []
    for name, default, _type, comment in ENV_SPEC:
        blocks.append(_comment_lines(comment, 0) + [f"{name}={_dump(default)}"])
    return "\n\n".join("\n".join(b) for b in blocks).rstrip() + "\n"


ENV_TEMPLATE: str = render_env_default()
