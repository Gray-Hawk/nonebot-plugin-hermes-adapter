"""
配置模型

所有配置项通过 NoneBot 的 .env 文件读取，前缀为 HERMES_。
"""

from nonebot import get_plugin_config
from pydantic import BaseModel, Field


class HermesEndpoint(BaseModel):
    """一个 Hermes 接入点。"""

    url: str
    """接入点 base URL。两种部署形态用同一个字段:
      - 多路复用(gateway.multiplex_profiles):http://host:8642/p/<profile>
      - 独立进程:                              http://host:8643
    """

    key: str = ""
    """该接入点的 API key。空 = 用全局 hermes_api_key,但那只对"与默认接入点同一把 key"
    的部署成立。

    指向命名 profile 时这里必填,原因有三条,任一条都足以让留空成为错误配置:
      - 上游校验的是该 profile 自己的 API_SERVER_KEY(且 >=16 字符),全局 key 必然 401
      - 它是"忘开 gateway.multiplex_profiles"唯一的探测器(否则前缀被静默忽略、
        请求落到默认 profile,零隔离却一切正常)
      - 它同时是反向通道认身份的依据;留空 = 该群没有独立的反向权限(落回补集)
    """

    timeout: int = 0
    """该接入点的请求超时(秒)。0 = 用全局 hermes_api_timeout。"""


class Config(BaseModel):
    # --- Hermes API Server ---
    hermes_api_url: str = "http://127.0.0.1:8642"
    """Hermes API Server 地址"""

    hermes_api_key: str = ""
    """Hermes API Server 密钥(对应 api_server.extra.key)"""

    hermes_api_timeout: int = 300
    """API 请求超时时间(秒),Agent 执行可能较慢"""

    # 本部署补丁: 长任务中间旁白实时转发。
    hermes_interim_echo_enabled: bool = False
    """长任务中间旁白实时转发(本部署补丁 维护者拍板默认关, 需在 .env 显式开启)。

    True 时 chat() 走 SSE 流式: agent 每轮工具调用前的旁白文本段以
    hermes.tool.progress(status=running) 帧为切分点实时转发到群/私聊,
    最终回复段仍走原解析管线一次性发出。False 退回一次性非流式(旧行为)。

    零 LLM 成本(流式只是传输方式不同, 生成量不变); 旁白转发是机械行为。
    流式下上游不内联本地图片, 由插件侧 inline_local_media_images 按白名单
    补做(见 core/media_whitelist.py)。"""

    # 本部署补丁: 本地文件/图片投递的白名单与尺寸上限。
    hermes_media_allow_dirs: str = ""
    """额外的本地文件投递白名单根目录(os.pathsep 分隔, 支持 ~)。

    默认白名单只有 ~/.hermes/cache/scratch 与 hermes 内建媒体缓存目录
    (images/audio/videos/documents/screenshots 及 legacy *_cache 布局);
    工作目录刻意不进默认(2026-10-08 维护者拍板), 需要时在这里加, 如:
        HERMES_MEDIA_ALLOW_DIRS="/home/user:/mnt/nas/share"
    判定宁严勿宽: 不在白名单内的本地路径一律退回 [生成了文件] 占位符。"""

    hermes_local_file_max_mb: int = 50
    """本地文件经群文件/私聊文件上传的尺寸上限(MB)。图片内联(转 data URL)
    不受此限, 固定 5MB 与上游 _MEDIA_DATA_URL_MAX_BYTES 对齐。"""

    hermes_group_endpoints: dict[str, HermesEndpoint] = {}
    """按群路由的接入点表。键为 `{adapter}:{group_id}`(与 hermes_admin_users 同构)。

    未列出的群与全部私聊走默认接入点(hermes_api_url)。用途是让特定群拥有
    独占的工具集 / 模型 / 文件工作区 —— 这些在 Hermes 侧是按 profile 分的。

    这张表也是反向通道 scope 的唯一来源:MCP 调用方呈上哪个接入点的 key,
    就只能操作该接入点名下的群;呈上全局 key 则只能操作补集(不在表内、
    或条目没有自己 key 的那些群)。没有第二张 token 表。

    .env 里写 JSON:
        HERMES_GROUP_ENDPOINTS='{"onebotv11:12345": {"url": "http://127.0.0.1:8642/p/team-a", "key": "sk-a"}}'

    每多一个接入点,Hermes 侧就要多维护一份 profile(含独立的 API_SERVER_KEY 与
    skill 安装),所以这张表是给"少数需要特殊待遇的群"手工指定的,不适合按群自动扩。
    """

    # --- 触发模式 ---
    hermes_group_trigger: str = "keyword-soft"
    """群聊触发方式(本部署补丁:凌晨批项2 全隐式化):
    at / all / keyword 三模式已作废,收敛为 keyword-soft 单路径——
    真 @bot / 引用 bot = 寻址事实(addressed_to_bot,开窗 + 进决策,不再强制回复);
    关键词全文命中 = 隐式激活(不点名、不开窗只续期,窗口外跑临时 turn),推最近
    hermes_keyword_soft_context(N1) 条消息给 LLM 自判是否发言。
    配置其它值时按 keyword-soft 处理并 WARN 一次。字段保留仅为 .env 兼容。"""

    hermes_keywords: set[str] = {"/ai"}
    """keyword-soft 隐式激活的关键词集合(全文匹配,命中即隐式激活推 N1 条上下文)"""

    hermes_private_trigger: str = "all"
    """私聊触发方式: all / allowlist"""

    hermes_allow_users: set[str] = set()
    """允许私聊的用户 ID(allowlist 模式)"""

    hermes_allow_groups: set[str] = set()
    """允许响应的群组 ID(空 = 全部允许)"""

    hermes_admin_users: set[str] = set()
    """管理员白名单(adapter+user 复合 ID)。用于 /hermes-status 等敏感命令。
    格式:`{adapter}:{user_id}`,adapter 取小写 + 去空格点(同 get_adapter_name)。
    例:["telegram:7000000001", "onebotv11:12345678"]
    空集 = 不允许任何人使用敏感命令(默认 deny)。"""

    # --- 会话 ---
    hermes_session_share_group: bool = False
    """群内是否共享同一个 session(False = 每人独立)"""

    # --- 长期记忆作用域 (X-Hermes-Session-Key) ---
    hermes_honcho_enabled: bool = False
    """是否向 Hermes 发送 X-Hermes-Session-Key(长期记忆作用域)。

    与 X-Hermes-Session-Id(短期 transcript)是两个独立维度:
      - Session-Id 决定"这轮对话接哪段历史",/clear 与上游自动压缩都会让它轮换
      - Session-Key 决定"长期记忆记在谁名下",跨 transcript、跨 /clear 稳定不变

    关闭时(默认)不发该头,上游 memory provider 按自己的 strategy 兜底 ——
    典型后果是所有群共写一份记忆,或记忆随压缩轮换而重新开始。

    需要 Hermes 端已配置 memory provider(目前是 Honcho)才有实际效果;
    没配时该头被忽略,无副作用。开启还要求本插件配了 HERMES_API_KEY ——
    上游对这个头要求鉴权,没 key 时插件不发头并在启动日志 WARN。
    """

    hermes_group_sessions_per_user: bool = False
    """群聊记忆作用域是否按人细分(与 Hermes 原生同名参数同语义)。

    False(默认): 一个群一份记忆,群内成员共享。bot 记住的是"这个群"。
    True:        群内每人一份记忆。bot 记住的是"这个群里的这个人",
                 成员之间互不可见,代价是群级共识无处存放。
    """

    hermes_group_session_key_format: str = "agent:main:nonebot-{adapter}:group:{group_id}"
    """群聊 + hermes_group_sessions_per_user=False 时的记忆 key 模板。

    可用变量: adapter, group_id
    默认值对齐 Hermes 原生 build_session_key 的 4 段格式,platform 段加 `nonebot-`
    前缀防止与 Hermes 原生 adapter 写进同一个 memory workspace 时撞名。
    自定义时建议保留该前缀。
    """

    hermes_group_per_user_session_key_format: str = "agent:main:nonebot-{adapter}:group:{group_id}:{user_id}"
    """群聊 + hermes_group_sessions_per_user=True 时的记忆 key 模板。

    可用变量: adapter, group_id, user_id
    """

    hermes_private_session_key_format: str = "agent:main:nonebot-{adapter}:dm:{user_id}"
    """私聊记忆 key 模板。

    可用变量: adapter, user_id
    """

    # --- 消息 ---
    hermes_max_length: int = 4000
    """单条回复最大长度(超出截断,QQ 限制约 4500 字符)"""

    hermes_ignore_prefix: set[str] = {"."}
    """以这些字符开头的消息不触发回复"""

    # --- 被动感知 (Chat Awareness) ---
    hermes_perception_enabled: bool = False
    """是否开启被动感知(监听但不回复非触发消息,为下次对话提供背景)"""

    hermes_perception_buffer: int = 10
    """被动感知缓存的历史消息数量"""

    hermes_perception_text_length: int = 200
    """被动感知单条历史消息最大长度(超出截断)"""

    hermes_perception_image_mode: str = "placeholder"
    """历史记录中的图片处理模式:
    - placeholder: 历史里图只用 [图片] 占位 + URL 引用,多模态 content 只发当前图 (默认)
    - inline_labeled: 历史最后一张图带 <<HISTORICAL IMAGES>> 标签放入多模态 content,与当前图清晰分隔
    - none: 完全不提历史图
    旧值 'last' 视为 'inline_labeled' 别名 (已废弃,启动时 WARN)

    **DEPRECATED (2026-05-13)**: 历史图召回改走 MCP 工具 (get_message_images),
    本配置仅控制 [图片] 占位是否出现在历史里;inline_labeled 模式已不再实装。
    """

    # --- M1: 内存缓冲 ---
    hermes_buffer_per_group_cap: int = 200
    """每群在 MessageBuffer 中保留多少条最近消息(LRU 之外的硬上限)"""

    hermes_buffer_total_groups_cap: int = 50
    """MessageBuffer 跨群总容量,超出按 LRU 驱逐"""

    # --- M1: 活跃态 ---
    hermes_active_session_enabled: bool = False
    """是否开启 @ 触发的群活跃态(False 退化为 v0.1.6 等价行为)"""

    hermes_active_session_ttl_sec: int = 300
    """活跃态默认 TTL(秒),滑动续期"""

    hermes_active_sweep_interval_sec: int = 30
    """expire_active_sessions cron 频率(秒)"""

    # --- Notice 事件触发 (Phase A) ---
    hermes_poke_trigger_enabled: bool = False
    """OneBot v11: 被戳一戳时触发对话(私聊/群无差别),等价 @。
    其他适配器无效,缺省全关 = 老用户行为零变化。"""

    hermes_greet_on_join: bool = False
    """OneBot v11: 群里有人加入时,在 active_session 开启的群触发一次 reactive turn
    让 Hermes 自决是否欢迎(decision_protocol 的 noop 是合法选择)。
    active_session 关时不触发——passive 是 1:1 Q&A 语义,不适用欢迎场景。"""

    # --- Phase B-0: ack 反馈 + 非文本段感知 ---
    hermes_ack_feedback_enabled: bool = False
    """显式触发 (用户主动 @ bot 或私聊) 时给一个'已收到'视觉回执。
    B-0 实装: OneBot v11 **群聊** (NapCat/LLOneBot/LuckyLilliaBot) 贴 emoji,
    chat 完成后撤销。其他适配器或场景 silently no-op。

    开关名通用,B-0.5 规划里要扩 Telegram/Discord 私聊 typing 状态,避免改名 breaking。

    **已知限制**:
      - 私聊不贴 emoji: QQ NT 协议下 set_msg_emoji_like 仅支持群聊
        (LuckyLilliaBot/NapCat 在私聊调用都会 raise '只支持群聊消息')
      - emoji 撤销依赖 OneBot 实现端较新版本:
          NapCat: 全版本支持 (用 set=False)
          LuckyLilliaBot / 较新 LLOneBot: 支持 (unset_msg_emoji_like 或 set=False)
          老 LLOneBot: 两条路径都不支持, emoji 永久留在消息上 (启动后 WARN-once 告知)
    若 emoji 不撤销但你不想 disable, 接受'永久已读痕迹'即可——不影响主流程。"""

    hermes_ack_emoji_id: int = 341
    """B-0 OneBot v11 路径下贴的 QQ 表情 id。默认 341 (/打招呼) ——
    动画"hi 打招呼",bot 收到 @ 的最自然反馈。
    其他推荐:
      - 373 (/忙):一只小动物在打字,typing-indicator 风格
      - 129 (/挥手):经典小表情版的挥手,跨版本更稳但视觉平淡
    注意:341/373 是 QQ NT 超表情 (EMCode 10000+),NapCat 内部有个
    `length > 3 ? type=2 : type=1` 启发式,3 位 id 会被当经典型——
    实测多数 NapCat 版本仍能正常 render,但跨版本不保证。
    若实测视觉异常,可换经典型小表情 (EMCode < 300,如 129)。
    NapCat schema 接受 Number | String,走 int 是为了 .env 直接写数字
    (HERMES_ACK_EMOJI_ID=237 不用引号),pydantic-settings 不会撞类型错。
    完整列表见 NapCat face_config.json。仅 OneBot 路径用到。"""

    hermes_reactive_post_reply_cooldown_sec: int = 8
    """reactive 模式下,bot 刚回复完群里 N 秒内,非显式 @ 触发的新消息直接静默。
    用来阻断「我刚说完别人接话→我又凑一句」类型的过触发。
    0 = 关闭(回退到旧行为)。显式 @bot 不受影响,任何时候都会立刻进入决策。"""

    # --- 应答防抖(本部署补丁:凌晨批项6,维护者拍板) ---

    hermes_reply_debounce_group_sec: float = 3.5
    """应答防抖(群聊,秒):消息触发后不立即起 turn,先等 N 秒合并连发碎片——
    QQ 用户「[图片]→3→哦那不怪你」式一秒一条的连发,原来每条各烧一轮 turn,
    防抖后合并成一轮,且 LLM 能看到完整想法再开口。期内新消息以最新触发为准
    并顺延截止时间(受 max_wait 硬上限约束)。0 = 关闭(回退旧行为:立即起跑)。
    防抖期间不占 inflight slot;ack emoji 仍在首条消息到达时秒贴。"""

    hermes_reply_debounce_private_sec: float = 2.5
    """应答防抖(私聊,秒),语义同 hermes_reply_debounce_group_sec。私聊碎片连发同样
    普遍,默认值略短于群聊。0 = 私聊不防抖。"""

    hermes_reply_debounce_max_wait_sec: float = 8.0
    """防抖顺延硬上限(秒):自首条消息起算最多等待这么久,防止活跃群里截止时间
    被新消息永远顺延、bot 永远不开口。0 = 无上限(不推荐)。"""

    # --- 黑名单(本部署补丁:凌晨批项3,维护者拍板) ---

    hermes_blacklist: dict[str, list[str]] = {}
    """黑名单:{群号: [账号QQ, ...]}。名单账号的消息不置 explicit/addressed——
    它们的 @ 降级为普通隐式信号,从入口消解 AI 互聊循环的燃料;消息照常入
    buffer、窗口内跟帖照常可见(人类委托「去跟 X 说」的会话能继续)。
    「软」语义在协议层:模型按锚/任务上下文自决是否与名单账号对话(默认不),
    代码不硬断。仅群聊生效;私聊不受名单影响。.env 写 JSON,如
    HERMES_BLACKLIST={"2000000001": ["1000000002"]}"""

    # --- 委托锚(本部署补丁:凌晨批项4,维护者拍板) ---

    hermes_anchor_max_age_sec: int = 150
    """委托锚的硬寿命上限(秒):锚龄超过即自动松掉,防「模型一直忘了松」。
    与活跃窗口寿命取 min(窗口死锚随窗死,无需额外处理)。建议 120-180,
    短于 hermes_active_session_ttl_sec。"""

    hermes_known_bots: list[str] = []
    """已知 bot 账号表(本部署补丁:锚优化短期项A,维护者拍板)。
    名单账号的消息不钉委托锚——bot 的 quote 接话不是委托,钉上只会挤占锚位、
    刷新锚龄。语义与 hermes_blacklist 不同:黑名单=不响应;known_bots 仍可
    被正常响应(人类委托「去跟 X 说」),只是其发言永远不成锚。全局生效
    (不分群),.env 写 JSON 数组,如 HERMES_KNOWN_BOTS=["1000000003"]"""

    # --- 激活体系改造:隐性激活 + T1/T5 + T2 定时激活(本部署新增) ---

    hermes_keyword_soft_context: int = 10
    """N1: keyword-soft 触发模式下,关键词命中(全文匹配)时推送给 LLM 的最近消息
    条数。关键词命中是隐式激活:不升级 addressed_to_bot,窗口外跑临时 turn 不开窗,
    窗口内只滑动续期。"""

    hermes_session_idle_reset_sec: int = 0
    """T1: 会话空闲自动重置(秒)。以最后一次 chat() 完成或 push_message 成功为活动
    基准,T1 内无任何显式/隐式激活或 LLM 主动行为,则静默重置会话(等价 /clear 的
    session 效果,但不发任何群提示)。0 = 禁用。
    正在执行的 turn(inflight / active session 租约)绝不会被重置。
    超控命令:/delay 暂停本会话 T1 计时,/resume 恢复并重置计时。"""

    hermes_active_session_t5_sec: int = 0
    """T5: 活跃窗口仅靠隐式续期(touch)的最长存活时间(秒)。自最后一次实质激活
    (显式触发 trigger / bot 发言 mark_bot_replied)起算,超 T5 后窗口在读路径
    (is_active / get_if_active)视为不活跃,随后按 TTL 自然过期。0 = 禁用。
    应大于 hermes_active_session_ttl_sec,否则等效于「窗口只能活 T5」(启动时 WARN)。"""

    hermes_proactive_interval_sec: int = 0
    """T2: 定时隐性激活的基础检查间隔(秒)。0 = 禁用整个功能。
    仅在「群无活跃窗口 且 无 in-flight turn」时检查;命中则跑一发无窗口的临时
    reactive turn(ephemeral),LLM 自决是否发言;发言即开窗,沉默不开窗。"""

    hermes_proactive_jitter_sec: int = 0
    """T3: 每次检查间隔附加的随机偏移上限(秒),实际间隔 = T2 + random(0, T3)。
    避免回复节奏过于机械。0 = 不附加偏移。"""

    hermes_proactive_window_sec: int = 300
    """T4: 每次检查时向前回看的新消息窗口(秒)。实际起点取 max(now-T4, 上次检查
    时刻)——不重复推送已判断过的记录。窗口内无新消息则不打扰 LLM。"""

    hermes_proactive_max_messages: int = 10
    """N2: 定时激活推送给 LLM 的最大消息条数,窗口内超出时取最新 N2 条。"""

    hermes_proactive_groups: set[str] = set()
    """允许定时激活的群 ID 集合。空 = BotRegistry 已知的全部群(重启后需群里先有人
    发言才会进入巡查名单)。"""

    # 本部署补丁: 后台任务完成投递轮询
    hermes_delivery_poll_interval_sec: int = 30
    """投递轮询间隔(秒)。0 = 禁用。
    背景:api_server 无状态会话的异步委派(delegate_task 后台结果)完成时,上游只把
    结果作为 display_kind=async_delegation_complete 的投递记录写进会话转录,不主动
    跑 turn(客户端拥有下一轮);TUI/桌面端靠轮询 /api/sessions/{id}/messages 看到它,
    而本插件原先没有等价通道——结果只能干等下一条群消息才被折叠进上下文。
    开启后每拍轮询全部已知会话的该端点,发现新投递记录就跑一发 LLM turn 让 agent
    用自己的人设口径汇报(群=reactive 临时 turn,私聊=passive turn)。
    成本语义:每次任务完成 = 一发额外 LLM turn(完成是低频事件);轮询本身只是本地
    HTTP + 小分页查询,无 LLM 调用。首轮轮询对存量记录做基线播种不播报(nb 停机期间
    的完成仍会随下一条真实消息自然折叠进上下文,不补播避免刷屏);目标群/私聊不在
    BotRegistry 或有 turn 在跑时顺延下一拍,不丢。"""

    hermes_transport_error_fallback_text: str = "嗯…我这边遇到点状况,稍后再问一次"
    """Hermes 上游返 5xx / 传输错误时, 替代 LLM raw_text 的兜底文本。
    没有这条兜底, 服务端英文错误信息(如 "Model generated invalid tool call: ...")
    会被原文当 raw_text 发到群里,体验差也泄露内部信息。
    空串 → 不发任何文本(等价于 silent)。仅在 reactive 显式触发 / passive 路径生效——
    非显式触发本来就静默,不受影响。"""


    # --- M1: 反向 MCP 通道 ---
    hermes_mcp_enabled: bool = False
    """是否启动内嵌 FastMCP server(False 时 Hermes 反向调用全失败,出向不影响)"""

    hermes_mcp_host: str = "127.0.0.1"
    """MCP server bind host. 默认 loopback (127.0.0.1)。
    改监听公网/局域网在技术上可行,但安全代价:push_message 能让 bot 往群里发
    任意内容,前端防御只有 Bearer token(明文 HTTP,且与 HERMES_API_KEY 同
    钥匙)。要改请配套反向代理 + TLS + 来源 IP ACL。"""

    hermes_mcp_port: int = 8643
    """MCP server bind port"""

    hermes_mcp_recent_limit_max: int = Field(default=50, ge=1)
    """get_recent_messages 工具单次返回上限。最小 1——0/负值会让工具静默返空,
    Pydantic 在启动期校验防 misconfig。"""

    # --- M1: 持久化存储 ---
    hermes_storage_db_path: str = ""
    """SQLite 消息日志路径。空串走默认 ~/.local/share/nonebot-plugin-hermes/messages.db"""

    hermes_storage_message_retention_days: int = 30
    """消息日志保留天数,超龄行 vacuum 时删"""

    hermes_storage_message_max_rows: int = 100_000
    """消息日志总行数硬上限,超出 vacuum 时按 ts 老到新删"""

    hermes_image_cache_dir: str = ""
    """图字节缓存目录。空串走默认 ~/.cache/nonebot-plugin-hermes/images/"""

    hermes_image_cache_quota_mb: int = 200
    """图缓存总体积上限(MB),超出按 atime 老到新淘汰"""

    hermes_image_fetch_timeout_s: int = 10
    """单图 HTTP 抓取超时秒数"""

    hermes_image_fetch_max_attempts: int = 2
    """单图总尝试次数(1=不重试,2=一次重试,以此类推)"""

    hermes_image_inline_enabled: bool = True
    """当前轮图片是否内联成 data: URL 后再发给 Hermes。

    关掉会退回直发平台图片 URL —— 上游对 URL 是纯透传,由 provider 自行抓取,
    因而受制于 URL 时效与对端出网能力,且部分 provider 形态会静默丢图。
    仅在需要与旧行为对照时临时关闭。
    """

    hermes_image_inline_max_edge: int = 1280
    """内联前下采样的最长边像素。超过才缩,小图不放大"""

    hermes_image_inline_quality: int = 82
    """内联重编码的 JPEG 质量"""

    hermes_image_inline_max_bytes: int = 2_000_000
    """单图重编码后的字节上限。降质阶梯走完仍超出则丢弃该图"""

    hermes_image_inline_total_max_bytes: int = 4_000_000
    """单轮内联图片的总字节预算。

    上游请求体有硬上限且要与文本历史共享,这里留出余量;超出预算的图片被丢弃
    而非撑爆整个请求 —— 少一张图仍能答话,请求被拒则整轮失败。
    """

    hermes_sticker_vision_enabled: bool = False
    """用户收藏/自定义表情(OneBot image sub_type=1，alconna Image.sticker=True)是否抓字节内联喂给模型。

    纯开关,无门控:False=完全没有表情包感知;True=只要消息进入 chat 就把表情图片
    直接投送给模型,并仍与普通图片分开标注。默认关是 token 预算考虑;开启意味着接受
    活跃窗口/私聊里每张收藏表情都占一份多模态预算。QQ 默认 face 小表情无图片 URL,
    仍走 [表情] 占位。
    """

    # --- Phase B-1: 合并转发处理 ---
    hermes_forward_extract_max_nodes: int = 10
    """合并转发消息最多展开的节点数。超出标注 '...另有 N 条已省略'。
    默认 10:常见'图集 / 聊天记录'类型转发,前 10 条已足够让 LLM 判断主题,
    更多会显著膨胀 system prompt 中的 <recent_messages> 段。"""

    hermes_forward_extract_max_chars: int = 800
    """合并转发展开文本的总字符上限(节点数之外的第二道闸)。
    超出按字符截断并标注 '...因字符上限截断'。
    防止 10 个节点里每条都是长文的退化场景。"""

    hermes_long_reply_forward: bool = True
    """超长回复在群聊(OneBot v11)下使用合并转发发送完整内容。
    False 走回原截断行为(hermes_max_length + 截断 suffix)。
    私聊永远走截断(send_private_forward_msg 实现端覆盖差)。
    非 onebotv11 适配器亦不受此开关影响,沿用截断。"""


plugin_config = get_plugin_config(Config)
