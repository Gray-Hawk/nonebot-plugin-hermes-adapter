"""活跃窗口的 SQLite 持久化(本部署补丁:runs 迁移阶段2a)。

痛点:活跃窗口原是纯内存态,nb 重启 = 窗口失忆——用户视角是「聊着聊着忽然
装死」(窗口期内的跟帖不再被听,要重新 @)。本模块把窗口镜像进 SQLite,
启动时装回未过期窗口(含委托锚),重启从「失忆」变「续听」。

设计:
- 写路径挂在 ActiveSessionManager 各 mutator 尾部(bind_store 注入),镜像语义,
  内存永远是主、库只是影子——任何读都以内存为准,库只在启动时读一次;
- 装回时按 TTL + T5 双判据剔除死窗并顺手删行;
- 租约(begin_turn/end_turn 计数)是 turn 生命周期状态,turn 不跨重启,不持久化;
- last_bot_reply_text 截断 2000 字符落库(去重闸门只需近字面对比,全量没意义)。
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS active_windows (
    adapter      TEXT NOT NULL,
    group_id     TEXT NOT NULL,
    payload      TEXT NOT NULL,
    expires_at   INTEGER NOT NULL,
    PRIMARY KEY (adapter, group_id)
)
"""

# last_bot_reply_text 落库上限(见模块 docstring)
_REPLY_TEXT_CAP = 2000


class WindowStore:
    """active_windows 表薄封装:整行 JSON 镜像,字段演进不用 ALTER。"""

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False,与 MessageStore/SessionKeyStore
        # 的加固对齐——NoneBot driver 可能在多 thread 上访问 ASGI 应用;写入方只有
        # event loop 一个,本参数只为放宽 sqlite3 模块自身的线程检查。手动 commit
        # 语义保持不变。
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        # WAL 标配 synchronous=NORMAL(同 run_tracker,镜像库
        # 不需要每提交 fsync)。
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    def upsert(self, adapter: str, group_id: str, payload: dict[str, Any], expires_at: int) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO active_windows (adapter, group_id, payload, expires_at)"
            " VALUES (?,?,?,?)",
            (adapter, group_id, json.dumps(payload, ensure_ascii=False), expires_at),
        )
        self._conn.commit()

    def delete(self, adapter: str, group_id: str) -> None:
        self._conn.execute(
            "DELETE FROM active_windows WHERE adapter=? AND group_id=?", (adapter, group_id)
        )
        self._conn.commit()

    def load_all(self) -> list[tuple[str, str, dict[str, Any], int]]:
        rows = self._conn.execute(
            "SELECT adapter, group_id, payload, expires_at FROM active_windows"
        ).fetchall()
        out = []
        for adapter, group_id, payload, expires_at in rows:
            try:
                out.append((adapter, group_id, json.loads(payload), expires_at))
            except ValueError:
                continue
        return out

    def close(self) -> None:
        self._conn.close()


def session_to_payload(s) -> dict[str, Any]:
    """ActiveSession → JSON payload(锚递归序列化)。独立成函数供离线测试。"""
    def _anchor(rec):
        if rec is None:
            return None
        return {
            "participants": [
                {"user_id": p.user_id, "nickname": p.nickname, "joined_ts": p.joined_ts}
                for p in rec.participants
            ],
            "msg_id": rec.platform_message_id,
            "m_id": rec.m_id,
            "platform_message_id": rec.platform_message_id,
            "text": rec.text,
            "ts": rec.ts,
            "answered": rec.answered,
        }

    return {
        "triggered_by": s.triggered_by,
        "started_at": s.started_at,
        "last_active_at": s.last_active_at,
        "expires_at": s.expires_at,
        "topic_hint": s.topic_hint,
        "last_bot_reply_at": s.last_bot_reply_at,
        "last_bot_reply_media": s.last_bot_reply_media,
        "last_bot_reply_text": (s.last_bot_reply_text or "")[:_REPLY_TEXT_CAP],
        "last_strong_activation_at": s.last_strong_activation_at,
        "generation": s.generation,
        "anchor_master": _anchor(s.anchor_master),
        "anchor_guest": _anchor(s.anchor_guest),
    }


def session_from_payload(adapter: str, group_id: str, payload: dict[str, Any]):
    """JSON payload → ActiveSession。字段缺失按默认值容忍(老版本行也能装)。

    锚的双形态装载(任务化重构 ):新形态 participants 列表直接用;
    旧形态 {commissioner_id, commissioner_nickname} 迁移为单参与者。
    """
    from ..active_session import ActiveSession, AnchorRecord, Participant

    def _anchor(d):
        if not isinstance(d, dict):
            return None
        ts = int(d.get("ts") or 0)
        participants: list = []
        raw_parts = d.get("participants")
        if isinstance(raw_parts, list):
            for p in raw_parts:
                if isinstance(p, dict) and str(p.get("user_id") or ""):
                    participants.append(
                        Participant(
                            user_id=str(p["user_id"]),
                            nickname=p.get("nickname"),
                            joined_ts=int(p.get("joined_ts") or ts),
                        )
                    )
        if not participants:
            # 旧形态迁移:单 commissioner → 单参与者
            cid = str(d.get("commissioner_id") or "")
            if cid:
                participants = [Participant(user_id=cid, nickname=d.get("commissioner_nickname"), joined_ts=ts)]
        return AnchorRecord(
            participants=participants,
            # 旧行 msg_id 只迁为内部平台 ID(alive/103);插件 m_id 旧行没有,
            # 保持 None=无号降级,绝不把平台 id 倒进模型可见的 m_id。
            m_id=(int(d["m_id"]) if d.get("m_id") is not None else None),
            platform_message_id=(
                str(d.get("platform_message_id") or d.get("msg_id"))
                if (d.get("platform_message_id") or d.get("msg_id")) is not None
                else None
            ),
            text=str(d.get("text") or ""),
            ts=ts,
            answered=bool(d.get("answered")),
        )

    return ActiveSession(
        adapter=adapter,
        group_id=group_id,
        triggered_by=str(payload.get("triggered_by") or ""),
        started_at=int(payload.get("started_at") or 0),
        last_active_at=int(payload.get("last_active_at") or 0),
        expires_at=int(payload.get("expires_at") or 0),
        topic_hint=payload.get("topic_hint"),
        last_bot_reply_at=int(payload.get("last_bot_reply_at") or 0),
        last_bot_reply_media=int(payload.get("last_bot_reply_media") or 0),
        last_bot_reply_text=str(payload.get("last_bot_reply_text") or ""),
        last_strong_activation_at=int(payload.get("last_strong_activation_at") or 0),
        generation=int(payload.get("generation") or 0),
        anchor_master=_anchor(payload.get("anchor_master")),
        anchor_guest=_anchor(payload.get("anchor_guest")),
    )
