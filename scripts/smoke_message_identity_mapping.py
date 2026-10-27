"""身份映射 smoke：m:id(库内自增主键) → qq_message_id(平台真实 message_id) 持久映射。

跑法（先让 PYTHONPATH 指向测试 shim，在中立目录执行）：
    PYTHONPATH=<测试 shim 目录> <NoneBot 虚拟环境 Python> scripts/smoke_message_identity_mapping.py

覆盖（全部离线，不连平台、不启 NoneBot 运行时）：
  1. 老库迁移：无 qq_message_id 列的存量库就地 ALTER，旧行 NULL、不猜值
     （即使 message_files 里躺着可用的平台 id 也不回填）；
  2. 真实入库映射：带 qq_message_id 的消息落库后按 (adapter, 会话, m:id) 可查回；
  3. 合成/无 ID：合成消息与空串平台 id 一律 NULL，查询返回 None；
  4. 会话隔离：跨群、跨 adapter、群↔私聊、私聊换 owner 一律查不到；
  5. buffer 重读：get_recent 回读水合 qq_message_id；
  6. bot 映射：is_bot=True 的站内回执同样可写/可查；
  7. 裸平台 ID 不当寻址：把 qq_message_id 当 m:id 传入不命中，字符串 id 一律拒绝。
"""

from __future__ import annotations

import dataclasses
import sqlite3
import sys
import tempfile
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_hermes_adapter.core.message_buffer import (  # noqa: E402
    BufferedMessage,
    MessageBuffer,
)
from nonebot_plugin_hermes_adapter.core.storage.message_store import MessageStore  # noqa: E402
from nonebot_plugin_hermes_adapter.core import reply_resolution as reply_resolver  # noqa: E402

FAILS: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("PASS " if cond else "FAIL ") + name + (f" | {detail}" if detail else ""))
    if not cond:
        FAILS.append(name)


# 老版 messages 表结构（本改动之前）：无 qq_message_id 列。
_LEGACY_SCHEMA = """
CREATE TABLE messages (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           INTEGER NOT NULL,
    adapter      TEXT    NOT NULL,
    group_id     TEXT,
    user_id      TEXT    NOT NULL,
    nickname     TEXT    NOT NULL,
    content      TEXT    NOT NULL,
    is_bot       INTEGER NOT NULL,
    reply_to_ts  INTEGER
)
"""


def tmp_db(prefix: str = "nph_idmap_") -> Path:
    return Path(tempfile.mkdtemp(prefix=prefix)) / "messages.db"


def columns(conn: sqlite3.Connection, table: str) -> dict[str, sqlite3.Row]:
    return {r["name"]: r for r in conn.execute(f"PRAGMA table_info({table})")}


_HAS_FIELD = "qq_message_id" in {f.name for f in dataclasses.fields(BufferedMessage)}
_HAS_OWNER_FIELD = "private_owner_id" in {f.name for f in dataclasses.fields(BufferedMessage)}


def mk(adapter: str, group_id: str | None, user_id: str, content: str, **kw) -> BufferedMessage:
    """构造消息；RED 阶段字段还没加，也要能跑完整套（不许在构造处炸掉）。"""
    qq = kw.pop("qq_message_id", None)
    private_owner = kw.pop("private_owner_id", None)
    base = dict(
        ts=kw.pop("ts", 1_700_000_000_000),
        adapter=adapter,
        group_id=group_id,
        user_id=user_id,
        nickname=kw.pop("nickname", "某人"),
        content=content,
        **kw,
    )
    if _HAS_FIELD:
        base["qq_message_id"] = qq
    if _HAS_OWNER_FIELD:
        base["private_owner_id"] = private_owner
    return BufferedMessage(**base)


def resolve(st, adapter: str, group_id: str | None, message_id, *, owner_user_id: str | None = None):
    """调用待测映射接口；接口缺席时返回哨兵，让 RED 以 FAIL 行呈现而不是 AttributeError。"""
    fn = getattr(st, "get_qq_message_id", None)
    if fn is None:
        return "<missing-api>"
    return fn(adapter, group_id, message_id, owner_user_id=owner_user_id)


def qid(msg) -> str | None:
    return getattr(msg, "qq_message_id", "<missing-field>")


# ---------------------------------------------------------------- 1. 老库迁移
db = tmp_db()
raw = sqlite3.connect(db)
raw.row_factory = sqlite3.Row
raw.execute(_LEGACY_SCHEMA)
raw.execute(
    "INSERT INTO messages (ts, adapter, group_id, user_id, nickname, content, is_bot, reply_to_ts) "
    "VALUES (?,?,?,?,?,?,?,?)",
    (1_600_000_000_000, "onebotv11", "3000000001", "10001", "老用户", "旧行 [文件:7543123.pdf]", 0, None),
)
raw.execute(
    "INSERT INTO messages (ts, adapter, group_id, user_id, nickname, content, is_bot, reply_to_ts) "
    "VALUES (?,?,?,?,?,?,?,?)",
    (1_600_000_000_001, "onebotv11", None, "10001", "老私聊用户", "旧私聊", 0, None),
)
raw.execute(
    "INSERT INTO messages (ts, adapter, group_id, user_id, nickname, content, is_bot, reply_to_ts) "
    "VALUES (?,?,?,?,?,?,?,?)",
    (1_600_000_000_002, "onebotv11", None, "99999", "老 bot", "旧私聊 bot", 1, None),
)
# 旧库里 file 行带着真实平台 id —— 迁移不许拿它猜主行 qq_message_id。
raw.execute(
    """
    CREATE TABLE message_files (
        message_id    INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
        idx           INTEGER NOT NULL,
        file_id       TEXT,
        file_name     TEXT,
        file_size     INTEGER,
        url           TEXT,
        qq_message_id TEXT,
        sha256        TEXT,
        local_path    TEXT,
        actual_size   INTEGER,
        status        TEXT    NOT NULL DEFAULT 'pending',
        error         TEXT,
        PRIMARY KEY (message_id, idx)
    )
    """
)
raw.execute(
    "INSERT INTO message_files (message_id, idx, file_id, file_name, file_size, qq_message_id) "
    "VALUES (1, 0, 'fid', '7543123.pdf', 10, '7543123')"
)
raw.commit()
check("迁移前老库确无 qq_message_id 列", "qq_message_id" not in columns(raw, "messages"))
raw.close()

store = MessageStore(db_path=db)
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row
msg_cols = columns(conn, "messages")
_qq_col = msg_cols.get("qq_message_id")
check("迁移后 messages 新增 qq_message_id 列", _qq_col is not None, str(sorted(msg_cols)))
check(
    "新列为可空(NULL 语义,不上默认值)",
    _qq_col is not None and _qq_col["notnull"] == 0,
    "" if _qq_col is None else str(dict(_qq_col)),
)
try:
    legacy_row = conn.execute("SELECT id, qq_message_id FROM messages WHERE id=1").fetchone()
except sqlite3.OperationalError:
    legacy_row = None  # RED：列还没迁移出来
check(
    "旧行 qq_message_id 保持 NULL(不猜值)",
    legacy_row is not None and legacy_row["qq_message_id"] is None,
    "" if legacy_row is None else str(tuple(legacy_row)),
)
check(
    "旧行即使 file 行有平台 id 也不回填",
    conn.execute("SELECT qq_message_id FROM message_files WHERE message_id=1").fetchone()["qq_message_id"] == "7543123",
)
try:
    legacy_owner = conn.execute("SELECT private_owner_id FROM messages WHERE id=2").fetchone()[0]
except sqlite3.OperationalError:
    legacy_owner = "<no-column>"
check(
    "老库私聊用户行可安全回填 private_owner_id=user_id",
    legacy_owner == "10001",
    repr(legacy_owner),
)
try:
    legacy_bot_owner = conn.execute("SELECT private_owner_id FROM messages WHERE id=3").fetchone()[0]
except sqlite3.OperationalError:
    legacy_bot_owner = "<no-column>"
check(
    "老库私聊 bot 行归属未知保持 NULL",
    legacy_bot_owner is None,
    repr(legacy_bot_owner),
)
check(
    "老库旧行按 m:id 查询明确返回 None",
    resolve(store, "onebotv11", "3000000001", 1) is None,
)
conn.close()
store.close()

# 迁移幂等：再开一次不炸、数据不丢、列不重复。
store = MessageStore(db_path=db)
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row
check("迁移幂等(重开不炸且列唯一)", list(columns(conn, "messages")).count("qq_message_id") == 1)
check("迁移幂等(旧行仍在)", conn.execute("SELECT COUNT(*) c FROM messages").fetchone()["c"] == 3)
conn.close()
store.close()

# ------------------------------------------------- 2/3/4/6/7. 新库写入与寻址
db = tmp_db()
store = MessageStore(db_path=db)
fetcher_calls: list[tuple] = []


class _DummyFetcher:
    def submit(self, msg_id, urls, start_index=0):
        fetcher_calls.append((msg_id, tuple(urls), start_index))


buffer = MessageBuffer(store=store, fetcher=_DummyFetcher())

# 真实入站
real = mk("onebotv11", "3000000001", "10001", "真消息", qq_message_id="7180000001")
buffer.append(real)
check("真实消息写入回填 m:id", real.id is not None and real.id > 0, str(real.id))

# 同群第二条：平台 id 与主键错开，便于验证「裸平台 ID 不命中」
real2 = mk("onebotv11", "3000000001", "10002", "第二条", qq_message_id="7180000002")
buffer.append(real2)

# 合成消息：无平台 id
syn = mk("onebotv11", "3000000001", "10001", "（系统合成）", is_synthetic=True)
buffer.append(syn)
# 空串平台 id：归一成 NULL
blank = mk("onebotv11", "3000000001", "10001", "空串 id", qq_message_id="")
buffer.append(blank)

# 另一个群 + 另一 adapter + 私聊
other_group = mk("onebotv11", "3000000009", "10001", "别的群", qq_message_id="7180000003")
buffer.append(other_group)
other_adapter = mk("astrbot", "3000000001", "10001", "别的 adapter", qq_message_id="7180000004")
buffer.append(other_adapter)
priv = mk("onebotv11", None, "10001", "私聊", qq_message_id="7180000005")
buffer.append(priv)
priv_peer = mk("onebotv11", None, "10086", "别人的私聊", qq_message_id="7180000006")
buffer.append(priv_peer)

# bot 站内回执
bot_msg = mk("onebotv11", "3000000001", "99999", "[bot] 我发的", is_bot=True, qq_message_id="7180000007")
buffer.append(bot_msg)
# 私聊 bot 行的 user_id 仍是真实发送者（bot），私聊归属另存 private_owner_id。
# 两个私聊对象可以对应同一个 bot 账号，查询绝不能靠 is_bot 全局放行。
bot_priv = mk(
    "onebotv11", None, "99999", "[bot] 私聊回复", is_bot=True,
    qq_message_id="7180000008", private_owner_id="10001",
)
buffer.append(bot_priv)
bot_priv_other = mk(
    "onebotv11", None, "99999", "[bot] 另一私聊回复", is_bot=True,
    qq_message_id="7180000009", private_owner_id="10086",
)
buffer.append(bot_priv_other)

check(
    "真实消息按 (adapter,群,m:id) 查回平台 id",
    resolve(store, "onebotv11", "3000000001", real.id) == "7180000001",
    repr(resolve(store, "onebotv11", "3000000001", real.id)),
)
check(
    "同群另一条各自映射正确(不串行)",
    resolve(store, "onebotv11", "3000000001", real2.id) == "7180000002",
    repr(resolve(store, "onebotv11", "3000000001", real2.id)),
)
check(
    "MessageBuffer 门面透出同一接口",
    resolve(buffer, "onebotv11", "3000000001", real.id) == "7180000001",
)
check("合成消息平台 id 为 NULL", resolve(store, "onebotv11", "3000000001", syn.id) is None)
check("空串平台 id 归一为 NULL", resolve(store, "onebotv11", "3000000001", blank.id) is None)
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row
try:
    _blank_qq = conn.execute("SELECT qq_message_id FROM messages WHERE id=?", (blank.id,)).fetchone()["qq_message_id"]
except sqlite3.OperationalError:
    _blank_qq = "<no-column>"
check("空串落库确为 NULL(不是空串)", _blank_qq is None, repr(_blank_qq))
conn.close()

# 会话隔离
check("跨群查不到", resolve(store, "onebotv11", "3000000009", real.id) is None)
check("跨 adapter 查不到", resolve(store, "astrbot", "3000000001", real.id) is None)
check("群消息当私聊查不到", resolve(store, "onebotv11", None, real.id, owner_user_id="10001") is None)
check("私聊消息当群消息查不到", resolve(store, "onebotv11", "3000000001", priv.id) is None)
check(
    "私聊按正确 owner 查得到",
    resolve(store, "onebotv11", None, priv.id, owner_user_id="10001") == "7180000005",
)
check(
    "私聊换 owner 查不到",
    resolve(store, "onebotv11", None, priv.id, owner_user_id="10086") is None,
)
check(
    "私聊缺 owner 一律查不到(失败即闭)",
    resolve(store, "onebotv11", None, priv.id) is None,
)
check(
    "别人的私聊不被串号",
    resolve(store, "onebotv11", None, priv_peer.id, owner_user_id="10086") == "7180000006"
    and resolve(store, "onebotv11", None, priv_peer.id, owner_user_id="10001") is None,
)

check(
    "bot 站内回执的映射可写可查",
    resolve(store, "onebotv11", "3000000001", bot_msg.id) == "7180000007",
)
check(
    "私聊 bot 行按 private_owner_id 查回且保留真实发送者",
    resolve(store, "onebotv11", None, bot_priv.id, owner_user_id="10001") == "7180000008"
    and bot_priv.user_id == "99999",
)
check(
    "私聊 bot 行跨 owner 不串号",
    resolve(store, "onebotv11", None, bot_priv.id, owner_user_id="10086") is None
    and resolve(store, "onebotv11", None, bot_priv_other.id, owner_user_id="10001") is None
    and resolve(store, "onebotv11", None, bot_priv_other.id, owner_user_id="10086") == "7180000009",
)
check(
    "私聊 bot 行缺归属时失败即闭",
    resolve(store, "onebotv11", None, bot_priv.id) is None,
)
resolve_reply = getattr(reply_resolver, "resolve_reply", None)
check("Reply 解析器可提供目标发送者", callable(resolve_reply))
if callable(resolve_reply):
    group_reply = resolve_reply(
        buffer, adapter="onebotv11", group_id="3000000001", owner_user_id=None,
        reply_to=f"m:{real.id}", log_tag="smoke",
    )
    check(
        "群聊 Reply 同时解析平台 ID 与被引用者",
        group_reply is not None
        and group_reply.qq_message_id == "7180000001"
        and group_reply.reply_at_user_id == "10001",
        repr(group_reply),
    )
    private_reply = resolve_reply(
        buffer, adapter="onebotv11", group_id=None, owner_user_id="10001",
        reply_to=f"m:{bot_priv.id}", log_tag="smoke",
    )
    check(
        "私聊 Reply 保留平台映射但不产生自动 @",
        private_reply is not None
        and private_reply.qq_message_id == "7180000008"
        and private_reply.reply_at_user_id is None,
        repr(private_reply),
    )
    cross_reply = resolve_reply(
        buffer, adapter="onebotv11", group_id="3000000009", owner_user_id=None,
        reply_to=f"m:{real.id}", log_tag="smoke",
    )
    check("跨会话 Reply 连发送者也不返回", cross_reply is None, repr(cross_reply))
counts = store.counts_by_scope()
known_scopes = set(store.known_groups())
check(
    "私聊统计按归属折叠且不制造 bot 假会话",
    counts.get(("onebotv11", "@private:10001")) == 2
    and counts.get(("onebotv11", "@private:10086")) == 2
    and ("onebotv11", "@private:99999") not in counts
    and ("onebotv11", "@private:99999") not in known_scopes,
    repr(counts),
)
targets = store.get_message_targets([priv.id, bot_priv.id, bot_priv_other.id, bot_msg.id])
check(
    "反向工具私聊路由取对端归属而不是 bot 发送者",
    targets.get(priv.id) == ("onebotv11", None, "10001")
    and targets.get(bot_priv.id) == ("onebotv11", None, "10001")
    and targets.get(bot_priv_other.id) == ("onebotv11", None, "10086")
    and targets.get(bot_msg.id) == ("onebotv11", "3000000001", "99999"),
    repr(targets),
)

# 裸平台 ID 当寻址 / 非法入参
check(
    "拿平台 id 当 m:id 传入不命中",
    resolve(store, "onebotv11", "3000000001", 7180000001) is None,
)
check(
    "字符串型 m:id 被拒(不做隐式转换)",
    resolve(store, "onebotv11", "3000000001", str(real.id)) is None,
)
check("不存在的 m:id 返回 None", resolve(store, "onebotv11", "3000000001", 999_999_999) is None)
check("None m:id 返回 None", resolve(store, "onebotv11", "3000000001", None) is None)
check("bool m:id 返回 None", resolve(store, "onebotv11", "3000000001", True) is None)
check("空 adapter 返回 None", resolve(store, "", "3000000001", real.id) is None)

# ---------------------------------------------------------- 5. buffer 重读水合
rows = store.get_recent("onebotv11", "3000000001", 50)
by_id = {r.id: r for r in rows}
check("get_recent 回读带真实 id", real.id in by_id, str(sorted(by_id)))
check(
    "get_recent 水合 qq_message_id",
    by_id.get(real.id) is not None and qid(by_id[real.id]) == "7180000001",
    repr(qid(by_id.get(real.id))),
)
check(
    "get_recent 合成消息水合为 None",
    by_id.get(syn.id) is not None and qid(by_id[syn.id]) is None,
    repr(qid(by_id.get(syn.id))),
)
check(
    "get_recent 空串消息水合为 None",
    by_id.get(blank.id) is not None and qid(by_id[blank.id]) is None,
    repr(qid(by_id.get(blank.id))),
)
check(
    "get_recent bot 行 is_bot + 平台 id 同时水合",
    by_id.get(bot_msg.id) is not None
    and by_id[bot_msg.id].is_bot is True
    and qid(by_id[bot_msg.id]) == "7180000007",
    repr(qid(by_id.get(bot_msg.id))),
)
priv_rows = buffer.get_recent("onebotv11", None, 10, owner_user_id="10001")
check(
    "私聊回读水合(用户与 bot 共用 private_owner_id 归属)",
    len(priv_rows) == 2
    and {r.id for r in priv_rows} == {priv.id, bot_priv.id}
    and all(getattr(r, "private_owner_id", None) == "10001" for r in priv_rows)
    and qid(next(r for r in priv_rows if r.id == priv.id)) == "7180000005"
    and qid(next(r for r in priv_rows if r.id == bot_priv.id)) == "7180000008",
    repr([(r.user_id, qid(r)) for r in priv_rows]),
)

# 默认值语义：不传 qq_message_id 的 BufferedMessage 默认 None（老调用方零改动）
check("BufferedMessage.qq_message_id 默认 None", qid(mk("onebotv11", "1", "2", "x")) is None)

# ---------------------------------------------------------- 8. perception 接线（静态）
# perception 的 handle 体依赖 alconna/平台 event，离线跑不起完整链路；这里按 AST 断言
# 「从 event.message_id 取真实 id」并「传进 BufferedMessage」，防掉落成死字段。
import ast  # noqa: E402

_perception = Path(__file__).resolve().parent.parent / "handlers" / "message_perception.py"
_tree = ast.parse(_perception.read_text(encoding="utf-8"))
_reads_event_id = False
_passes_qq = False
for node in ast.walk(_tree):
    if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "getattr":
        if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant) and node.args[1].value == "message_id":
            _reads_event_id = True
    if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "BufferedMessage":
        _passes_qq = any(kw.arg == "qq_message_id" for kw in node.keywords)
check("perception 从 event.message_id 取平台 id", _reads_event_id)
check("perception 把 qq_message_id 传进 BufferedMessage", _passes_qq)

# 三条 bot 私聊回写路径都必须显式给对端归属，不能只写 user_id=bot。
for rel in (
    "handlers/message_turns.py",
    "mcp/tools/push_message.py",
    "tasks/run_resume.py",
):
    tree = ast.parse((Path(__file__).resolve().parent.parent / rel).read_text(encoding="utf-8"))
    passes_owner = any(
        isinstance(node, ast.Call)
        and getattr(node.func, "id", "") == "BufferedMessage"
        and any(kw.arg == "private_owner_id" for kw in node.keywords)
        for node in ast.walk(tree)
    )
    check(f"{rel} 的 bot 回写显式设置私聊归属", passes_owner)

# 持久性：关库重开，映射还在（进程重启不丢身份映射）
store.close()
store2 = MessageStore(db_path=db)
check(
    "重开库后映射仍可查(持久)",
    resolve(store2, "onebotv11", "3000000001", real.id) == "7180000001",
    repr(resolve(store2, "onebotv11", "3000000001", real.id)),
)
check(
    "重开库后会话隔离仍生效",
    resolve(store2, "onebotv11", "3000000009", real.id) is None,
)
check(
    "重开库后旧 NULL 行仍 None",
    resolve(store2, "onebotv11", "3000000001", syn.id) is None,
)
store2.close()

store.close()

print()
if FAILS:
    print(f"共 {len(FAILS)} 项失败: {FAILS}")
    sys.exit(1)
print("全部通过")
