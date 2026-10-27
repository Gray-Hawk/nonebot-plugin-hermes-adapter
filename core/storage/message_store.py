"""SQLite 后端的消息日志。

负责:
- 自增 id 分配(BufferedMessage.id 回填)
- 多图存储(message_images 表)
- 按 (adapter, group_id [, user_id]) 查询最近 N 条
- vacuum:按 retention 天数 + 行数硬上限淘汰
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

from nonebot import logger

from ..message_buffer import BufferedMessage, iter_message_media
from ..message_origin import MessageOrigin

# 写法上分两段:CREATE TABLE 之间用分号分隔的多 statement,
# sqlite3 单次 execute 只接一条,逐条执行更稳。
_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS messages (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        ts            INTEGER NOT NULL,
        adapter       TEXT    NOT NULL,
        group_id      TEXT,
        user_id       TEXT    NOT NULL,
        nickname      TEXT    NOT NULL,
        content       TEXT    NOT NULL,
        is_bot        INTEGER NOT NULL,
        reply_to_ts   INTEGER,
        qq_message_id TEXT,
        private_owner_id TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_msgs_group_ts ON messages (adapter, group_id, ts DESC)",
    """
    CREATE TABLE IF NOT EXISTS message_images (
        message_id   INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
        idx          INTEGER NOT NULL,
        url          TEXT    NOT NULL,
        sha256       TEXT,
        mime_type    TEXT,
        kind         TEXT    NOT NULL DEFAULT 'image',
        PRIMARY KEY (message_id, idx)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_imgs_message ON message_images (message_id)",
    """
    CREATE TABLE IF NOT EXISTS message_forwards (
        message_id   INTEGER PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
        content      TEXT    NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS message_files (
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
    """,
    "CREATE INDEX IF NOT EXISTS idx_files_message ON message_files (message_id)",
]


def _norm_qq_message_id(value: Any) -> str | None:
    """平台 message_id 归一:空串/空白/None → NULL,其余转 str(容忍 int 型 adapter)。

    不在这里做「从内容里猜 id」之类的推断——拿不到真实 id 就存 NULL,
    NULL 是明确事实(「未知」),猜出来的数字是假事实。
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


class MessageStore:
    """SQLite WAL 模式的消息日志,单进程内多线程读写安全。"""

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False:NoneBot driver 可能在多 thread 上访问 ASGI 应用。
        # SQLite WAL 模式天然支持并发读 + 单写;我们应用层目前都是单 event loop,
        # 不存在跨线程写竞争,本参数只为放宽 sqlite3 模块自身的线程检查。
        self._conn = sqlite3.connect(self._db_path, isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.row_factory = sqlite3.Row
        for stmt in _SCHEMA_STATEMENTS:
            self._conn.execute(stmt)
        self._migrate()
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_msgs_private_owner_ts "
            "ON messages (adapter, private_owner_id, ts DESC)"
        )
        self._closed = False

    def _migrate(self) -> None:
        """老库就地升级。新增列按各自语义迁移，不用统一默认值伪造历史事实。

        - message_images.kind:前只有普通图片,存量行补 'image' 即正确。
        - messages.qq_message_id:平台(OneBot/NapCat)真实 message_id,用于把模型可见的
          m:id(自增主键)映射回平台侧发消息/取文件。老库存量行**留 NULL、一律不猜**:
          历史内容里的数字、message_files.qq_message_id 都只是线索,不是该消息的平台 id,
          猜错会把回执发到别的消息上。NULL 语义 = 「该行平台 id 未知」。
        - messages.private_owner_id:私聊归属与真实发送者分列。普通私聊旧行可确定
          归属就是 user_id，按此前建；老私聊 bot 行无法反推对端，保持 NULL，
          绝不拿 bot 自己当 owner 或把该行开放给所有私聊。
        """
        img_cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(message_images)")}
        if "kind" not in img_cols:
            self._conn.execute("ALTER TABLE message_images ADD COLUMN kind TEXT NOT NULL DEFAULT 'image'")
            logger.info("[message_store] migrated: message_images +kind column")

        msg_cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(messages)")}
        if "qq_message_id" not in msg_cols:
            # 可空、无默认值:存量行 NULL,新行由 append 写入。
            self._conn.execute("ALTER TABLE messages ADD COLUMN qq_message_id TEXT")
            logger.info("[message_store] migrated: messages +qq_message_id column (legacy rows NULL)")

        msg_cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(messages)")}
        if "private_owner_id" not in msg_cols:
            self._conn.execute("ALTER TABLE messages ADD COLUMN private_owner_id TEXT")
            logger.info("[message_store] migrated: messages +private_owner_id column")
        # 普通私聊行的 sender 就是对端，可确定回填；老 bot 行没有这条事实，
        # 保持 NULL。重复执行只补仍缺归属的普通行，不覆盖显式归属。
        self._conn.execute(
            "UPDATE messages SET private_owner_id = user_id "
            "WHERE group_id IS NULL AND is_bot = 0 AND private_owner_id IS NULL"
        )

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._conn.close()
        except Exception:
            pass
        self._closed = True

    def append(self, msg: BufferedMessage) -> int | None:
        """写入消息,回填 msg.id。失败返回 None 但不抛。

        主行 + 图片行 + 转发行圈成一个显式事务——连接是
        autocommit(isolation_level=None),逐语句各自提交,中途崩溃会留「有消息缺图」
        的半截行。
        """
        if not msg.origin.allows_store:
            raise ValueError(f"来源 {msg.origin.source.value} 不允许写入消息库")
        try:
            private_owner_id = msg.private_owner_id
            if msg.group_id is None and not msg.is_bot and private_owner_id is None:
                private_owner_id = msg.user_id
            self._conn.execute("BEGIN")
            try:
                cur = self._conn.execute(
                    "INSERT INTO messages "
                    "(ts, adapter, group_id, user_id, nickname, content, is_bot, reply_to_ts, "
                    "qq_message_id, private_owner_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        msg.ts,
                        msg.adapter,
                        msg.group_id,
                        msg.user_id,
                        msg.nickname,
                        msg.content,
                        1 if msg.is_bot else 0,
                        msg.reply_to_ts,
                        _norm_qq_message_id(msg.qq_message_id),
                        private_owner_id,
                    ),
                )
                msg_id = cur.lastrowid
                # 枚举序不变量:own → sticker → quote → forward,连续 idx。
                # 枚举产物与渲染侧共用 iter_message_media,禁止两处各自拼序。
                # kind 列区分语义,PK 是 (message_id, idx)。
                for i, (kind, url) in enumerate(iter_message_media(msg)):
                    self._conn.execute(
                        "INSERT INTO message_images (message_id, idx, url, sha256, mime_type, kind) VALUES (?, ?, ?, NULL, NULL, ?)",
                        (msg_id, i, url, kind),
                    )
                if msg.forward_content:
                    self._conn.execute(
                        "INSERT INTO message_forwards (message_id, content) VALUES (?, ?)",
                        (msg_id, msg.forward_content),
                    )
                # 文件元数据:独立编号(idx 不与图片共享),只写元数据不下载。
                # 每条 dict 键 file_id/name/size/url/qq_message_id;下载态
                # (sha256/local_path/actual_size/status/error)由 get_message_files
                # 工具下载成功后回填。列表顺序即 idx。
                for i, entry in enumerate(msg.file_entries):
                    self._conn.execute(
                        "INSERT INTO message_files "
                        "(message_id, idx, file_id, file_name, file_size, url, qq_message_id) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            msg_id,
                            i,
                            entry.get("file_id") or None,
                            entry.get("name") or None,
                            int(entry.get("size") or 0),
                            entry.get("url") or None,
                            entry.get("qq_message_id") or None,
                        ),
                    )
            except BaseException:
                # 任何一步失败整体回滚,不留半截消息行。
                try:
                    self._conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            self._conn.execute("COMMIT")
            msg.id = msg_id
            msg.private_owner_id = private_owner_id
            return msg_id
        except sqlite3.Error as exc:
            logger.error(f"[message_store] append failed: {exc}; msg dropped (ts={msg.ts})")
            return None

    def get_recent(
        self,
        adapter: str,
        group_id: str | None,
        limit: int,
        before_ts: int | None = None,
        owner_user_id: str | None = None,
    ) -> list[BufferedMessage]:
        """新→旧顺序返回最近 limit 条;同 MessageBuffer.get_recent 语义。"""
        if group_id is None and owner_user_id is None:
            raise ValueError("owner_user_id is required when group_id is None (private chat lookup)")
        params: list[Any] = [adapter]
        where = "adapter = ?"
        if group_id is None:
            where += " AND group_id IS NULL AND private_owner_id = ?"
            params.append(owner_user_id)
        else:
            where += " AND group_id = ?"
            params.append(group_id)
        if before_ts is not None:
            where += " AND ts < ?"
            params.append(before_ts)
        params.append(limit)
        rows = self._conn.execute(
            "SELECT id, ts, adapter, group_id, user_id, nickname, content, is_bot, reply_to_ts, "
            "qq_message_id, private_owner_id "
            f"FROM messages WHERE {where} ORDER BY ts DESC LIMIT ?",
            params,
        ).fetchall()
        if not rows:
            return []
        ids = [r["id"] for r in rows]
        placeholders = ",".join("?" for _ in ids)
        img_rows = self._conn.execute(
            "SELECT message_id, idx, url, kind FROM message_images "
            f"WHERE message_id IN ({placeholders}) ORDER BY message_id, idx",
            ids,
        ).fetchall()
        urls_by_msg: dict[int, list[str]] = defaultdict(list)
        stickers_by_msg: dict[int, list[str]] = defaultdict(list)
        quote_by_msg: dict[int, list[str]] = defaultdict(list)
        fwdimg_by_msg: dict[int, list[str]] = defaultdict(list)
        for ir in img_rows:
            kind = ir["kind"]
            # 枚举序 kind → 各自的桶;未知 kind(理论上只有历史遗留)按普通图兜底,
            # 保持旧库行为不变。
            if kind == "sticker":
                bucket = stickers_by_msg
            elif kind == "quote":
                bucket = quote_by_msg
            elif kind == "forward_image":
                bucket = fwdimg_by_msg
            else:
                bucket = urls_by_msg
            bucket[ir["message_id"]].append(ir["url"])
        fwd_rows = self._conn.execute(
            f"SELECT message_id, content FROM message_forwards WHERE message_id IN ({placeholders})",
            ids,
        ).fetchall()
        fwd_by_msg = {fr["message_id"]: fr["content"] for fr in fwd_rows}
        file_rows = self._conn.execute(
            "SELECT message_id, idx, file_id, file_name, file_size, url, qq_message_id "
            f"FROM message_files WHERE message_id IN ({placeholders}) ORDER BY message_id, idx",
            ids,
        ).fetchall()
        files_by_msg: dict[int, list[dict]] = defaultdict(list)
        for fr in file_rows:
            files_by_msg[fr["message_id"]].append(
                {
                    "file_id": fr["file_id"],
                    "name": fr["file_name"],
                    "size": fr["file_size"],
                    "url": fr["url"],
                    "qq_message_id": fr["qq_message_id"],
                }
            )
        return [
            BufferedMessage(
                ts=r["ts"],
                adapter=r["adapter"],
                group_id=r["group_id"],
                user_id=r["user_id"],
                nickname=r["nickname"],
                content=r["content"],
                origin=MessageOrigin.from_stored(is_bot=bool(r["is_bot"])),
                image_urls=urls_by_msg.get(r["id"], []),
                sticker_urls=stickers_by_msg.get(r["id"], []),
                quote_image_urls=quote_by_msg.get(r["id"], []),
                forward_image_urls=fwdimg_by_msg.get(r["id"], []),
                reply_to_ts=r["reply_to_ts"],
                is_bot=bool(r["is_bot"]),
                id=r["id"],
                qq_message_id=r["qq_message_id"],
                private_owner_id=r["private_owner_id"],
                forward_content=fwd_by_msg.get(r["id"]),
                file_entries=files_by_msg.get(r["id"], []),
            )
            for r in rows
        ]

    def get_message_images_meta(self, message_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
        """按 message_id 拉对应的 image 元数据(url + sha256 + mime + kind)。

        未找到的 message_id 不出现在返回 dict 里(不是 KeyError)。
        """
        if not message_ids:
            return {}
        placeholders = ",".join("?" for _ in message_ids)
        rows = self._conn.execute(
            "SELECT message_id, idx, url, sha256, mime_type, kind FROM message_images "
            f"WHERE message_id IN ({placeholders}) ORDER BY message_id, idx",
            message_ids,
        ).fetchall()
        out: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for r in rows:
            out[r["message_id"]].append(
                {
                    "idx": r["idx"],
                    "url": r["url"],
                    "sha256": r["sha256"],
                    "mime_type": r["mime_type"],
                    "kind": r["kind"],
                }
            )
        return dict(out)

    def get_message_owners(self, message_ids: list[int]) -> dict[int, tuple[str, str | None]]:
        """message_id → (adapter, group_id)。未找到的 id 不出现在返回里。

        反向通道的范围判定要它:按 id 取内容的工具(get_message_images)拿到的是自增
        小整数,不解析归属就等于把全库暴露给任何一把 token。
        """
        if not message_ids:
            return {}
        placeholders = ",".join("?" for _ in message_ids)
        rows = self._conn.execute(
            f"SELECT id, adapter, group_id FROM messages WHERE id IN ({placeholders})",
            message_ids,
        ).fetchall()
        return {r["id"]: (r["adapter"], r["group_id"]) for r in rows}

    def get_message_targets(self, message_ids: list[int]) -> dict[int, tuple[str, str | None, str]]:
        """message_id → (adapter, group_id, route_user_id)。未找到的 id 不出现。

        反向通道需要私聊对端 ID 来选 bot 连接(多 bot 部署下按私聊对象路由)。
        私聊必须取 private_owner_id，不能取真实发送者 user_id：bot 自己发出的
        消息也由同一个对端会话路由。归属未知的老 bot 行跳过，不拿 bot 自己冒充对端。
        """
        if not message_ids:
            return {}
        placeholders = ",".join("?" for _ in message_ids)
        rows = self._conn.execute(
            "SELECT id, adapter, group_id, "
            "CASE WHEN group_id IS NULL THEN private_owner_id ELSE user_id END AS route_user_id "
            f"FROM messages WHERE id IN ({placeholders})",
            message_ids,
        ).fetchall()
        return {
            r["id"]: (r["adapter"], r["group_id"], r["route_user_id"])
            for r in rows
            if r["route_user_id"] is not None
        }

    def get_qq_message_id(
        self,
        adapter: str,
        group_id: str | None,
        message_id: int,
        *,
        owner_user_id: str | None = None,
    ) -> str | None:
        """m:id → 平台真实 message_id；范围校验与 get_reply_target 完全一致。"""
        target = self.get_reply_target(
            adapter, group_id, message_id, owner_user_id=owner_user_id
        )
        return target[0] if target is not None else None

    def get_reply_target(
        self,
        adapter: str,
        group_id: str | None,
        message_id: int,
        *,
        owner_user_id: str | None = None,
    ) -> tuple[str, str] | None:
        """m:id → (平台真实 message_id, 真实发送者 user_id)，失败即闭。

        群聊按 (adapter, group_id) 收口；私聊必须给 owner_user_id 并按
        (adapter, private_owner_id) 收口。返回的 user_id 始终是该行真实发送者，
        供群聊 Reply 后自动 @ 被引用者；私聊路由不能使用这个值代替对端归属。
        只认正整数内部 m:id，字符串/布尔/None 不隐式转换。
        """
        if isinstance(message_id, bool) or not isinstance(message_id, int) or message_id <= 0:
            return None
        if not isinstance(adapter, str) or not adapter.strip():
            return None
        if group_id is None:
            if not isinstance(owner_user_id, str) or not owner_user_id.strip():
                return None
            sql = (
                "SELECT qq_message_id, user_id FROM messages "
                "WHERE id = ? AND adapter = ? AND group_id IS NULL AND private_owner_id = ?"
            )
            params: tuple[Any, ...] = (message_id, adapter, owner_user_id)
        else:
            sql = (
                "SELECT qq_message_id, user_id FROM messages "
                "WHERE id = ? AND adapter = ? AND group_id = ?"
            )
            params = (message_id, adapter, group_id)
        try:
            row = self._conn.execute(sql, params).fetchone()
        except sqlite3.Error as exc:
            logger.warning(f"[message_store] get_reply_target m={message_id} failed: {exc}")
            return None
        if row is None:
            return None
        qq_message_id = _norm_qq_message_id(row["qq_message_id"])
        if qq_message_id is None:
            return None
        return qq_message_id, str(row["user_id"] or "")

    def update_content(self, message_id: int, content: str) -> None:
        """回填指定消息的 content(perception 拿真实 db id 后对含文件消息补
        m:id#fN 标签)。只改 content 列,不动 ts/媒体行。"""
        try:
            self._conn.execute(
                "UPDATE messages SET content=? WHERE id=?", (content, message_id)
            )
        except sqlite3.Error as exc:
            logger.warning(f"[message_store] update_content m={message_id} failed: {exc}")

    def update_image_sha(self, message_id: int, idx: int, sha256: str, mime_type: str) -> None:
        try:
            self._conn.execute(
                "UPDATE message_images SET sha256=?, mime_type=? WHERE message_id=? AND idx=?",
                (sha256, mime_type, message_id, idx),
            )
        except sqlite3.Error as exc:
            logger.warning(f"[message_store] update_image_sha m={message_id} idx={idx} failed: {exc}")

    def get_message_files(self, message_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
        """按 message_id 拉对应的文件元数据(含下载状态)。未找到的 id 不出现在返回里。

        每行键:idx/file_id/name/size/url/qq_message_id/sha256/local_path/
        actual_size/status/error。其中 file_id/name/size/url/qq_message_id 与
        BufferedMessage.file_entries 的 dict 键同构(name=file_name、size=file_size,
        取值同列),其余为下载态扩展——get_message_files 工具据此判缓存命中与刷新。
        """
        if not message_ids:
            return {}
        placeholders = ",".join("?" for _ in message_ids)
        rows = self._conn.execute(
            "SELECT message_id, idx, file_id, file_name, file_size, url, qq_message_id, "
            "sha256, local_path, actual_size, status, error FROM message_files "
            f"WHERE message_id IN ({placeholders}) ORDER BY message_id, idx",
            message_ids,
        ).fetchall()
        out: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for r in rows:
            out[r["message_id"]].append(
                {
                    "idx": r["idx"],
                    "file_id": r["file_id"],
                    "name": r["file_name"],
                    "size": r["file_size"],
                    "url": r["url"],
                    "qq_message_id": r["qq_message_id"],
                    "sha256": r["sha256"],
                    "local_path": r["local_path"],
                    "actual_size": r["actual_size"],
                    "status": r["status"],
                    "error": r["error"],
                }
            )
        return dict(out)

    def update_file_download(
        self, message_id: int, idx: int, *, sha256: str, local_path: str, actual_size: int
    ) -> None:
        """下载成功后回填缓存态(sha256/路径/实际大小),status 置 cached、清 error。"""
        try:
            self._conn.execute(
                "UPDATE message_files SET sha256=?, local_path=?, actual_size=?, "
                "status='cached', error=NULL WHERE message_id=? AND idx=?",
                (sha256, local_path, actual_size, message_id, idx),
            )
        except sqlite3.Error as exc:
            logger.warning(f"[message_store] update_file_download m={message_id} idx={idx} failed: {exc}")

    def update_file_failed(self, message_id: int, idx: int, *, error: str) -> None:
        """下载失败后回填 failed 态与可见错误,不返回半截路径。"""
        try:
            self._conn.execute(
                "UPDATE message_files SET status='failed', error=? WHERE message_id=? AND idx=?",
                (error, message_id, idx),
            )
        except sqlite3.Error as exc:
            logger.warning(f"[message_store] update_file_failed m={message_id} idx={idx} failed: {exc}")

    def update_file_ref(
        self,
        message_id: int,
        idx: int,
        *,
        file_id: str | None = None,
        url: str | None = None,
        qq_message_id: str | None = None,
    ) -> None:
        """刷新 file_id/url/qq_message_id(群/私聊 file url 或 get_msg 过期恢复后)。
        只覆盖显式传入的非 None 字段,其余保持原值。"""
        try:
            self._conn.execute(
                "UPDATE message_files SET "
                "file_id=COALESCE(?, file_id), url=COALESCE(?, url), "
                "qq_message_id=COALESCE(?, qq_message_id) "
                "WHERE message_id=? AND idx=?",
                (file_id, url, qq_message_id, message_id, idx),
            )
        except sqlite3.Error as exc:
            logger.warning(f"[message_store] update_file_ref m={message_id} idx={idx} failed: {exc}")

    def vacuum(self, min_ts: int, max_rows: int) -> int:
        """删 ts < min_ts 的行,以及超 max_rows 上限的最老的行。返回删除行数。

        FK + ON DELETE CASCADE 会自动级联清理 message_images。
        """
        deleted = 0
        cur = self._conn.execute("DELETE FROM messages WHERE ts < ?", (min_ts,))
        deleted += cur.rowcount
        total = self._conn.execute("SELECT COUNT(*) AS c FROM messages").fetchone()["c"]
        if total > max_rows:
            extra = total - max_rows
            self._conn.execute(
                "DELETE FROM messages WHERE id IN (SELECT id FROM messages ORDER BY ts ASC LIMIT ?)",
                (extra,),
            )
            deleted += extra
        return deleted

    def counts_by_scope(self) -> dict[tuple[str, str], int]:
        """返回 (adapter, scope_id) → 消息条数。scope_id 口径同 known_groups。

        群按 group_id 合并(不按发言人拆),私聊折成 `@private:<private_owner_id>`；
        归属未知的老 bot 行不伪造会话，也不计入任何对端桶。
        这个折叠在 SQL 里做,省得把整表拉回来再在 Python 里分组。
        """
        rows = self._conn.execute(
            "SELECT adapter, COALESCE(group_id, '@private:' || private_owner_id) AS scope, COUNT(*) AS n "
            "FROM messages WHERE group_id IS NOT NULL OR private_owner_id IS NOT NULL "
            "GROUP BY adapter, scope"
        ).fetchall()
        return {(r["adapter"], r["scope"]): r["n"] for r in rows}

    def known_groups(self) -> list[tuple[str, str]]:
        """返回所有已有消息的 (adapter, scope_id) 唯一集合。

        scope_id:群聊 = group_id,私聊 = `@private:<private_owner_id>`(私聊桶按
        `@private:` 前缀判别)；归属未知的老 bot 行不生成假会话。
        """
        rows = self._conn.execute(
            "SELECT DISTINCT adapter, group_id, private_owner_id FROM messages"
        ).fetchall()
        seen: set[tuple[str, str]] = set()
        out: list[tuple[str, str]] = []
        for r in rows:
            if r["group_id"] is not None:
                k = (r["adapter"], r["group_id"])
            elif r["private_owner_id"] is not None:
                k = (r["adapter"], f"@private:{r['private_owner_id']}")
            else:
                continue
            if k not in seen:
                seen.add(k)
                out.append(k)
        return out
