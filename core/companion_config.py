"""runs_companion.yaml：会话到 Runs Companion stream 的独立映射。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

from nonebot import logger

from ..config import resolve_custom_dir
from .config_templates import load_yaml


@dataclass(frozen=True, slots=True)
class CompanionStreamConfig:
    stream_id: str
    session_id: str
    generation: int


@dataclass(frozen=True, slots=True)
class CompanionConfig:
    enabled: bool = False
    base_url: str = ""
    token_env: str = "RUNS_COMPANION_NONEBOT_TOKEN"
    poll_interval_sec: float = 0.5
    request_timeout_sec: float = 10.0
    database_path: str = ""
    streams: dict[str, CompanionStreamConfig] = field(default_factory=dict)


def parse_companion_config(doc: dict) -> CompanionConfig:
    enabled = doc.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ValueError("enabled 必须是布尔值")
    if not enabled:
        return CompanionConfig()
    base_url = str(doc.get("base_url") or "").strip().rstrip("/")
    parsed = urlparse(base_url)
    if parsed.scheme == "http" and parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("远程 base_url 必须使用 HTTPS")
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("base_url 无效")
    token_env = str(doc.get("token_env") or "").strip()
    if not token_env or token_env == "AGENT_API_KEY":
        raise ValueError("token_env 必须指定独立凭据环境变量")
    poll_interval = float(doc.get("poll_interval_sec", 0.5))
    timeout = float(doc.get("request_timeout_sec", 10.0))
    if not 0.1 <= poll_interval <= 60 or not 1 <= timeout <= 120:
        raise ValueError("轮询间隔或请求超时越界")
    raw_streams = doc.get("streams")
    if not isinstance(raw_streams, list) or not raw_streams:
        raise ValueError("companion 至少需要一个 stream")
    streams: dict[str, CompanionStreamConfig] = {}
    stream_ids: set[str] = set()
    for raw in raw_streams:
        if not isinstance(raw, dict):
            raise ValueError("stream 必须是映射")
        stream_id = str(raw.get("stream_id") or "").strip()
        session_id = str(raw.get("session_id") or "").strip()
        generation = raw.get("generation")
        if (
            not stream_id or not session_id or stream_id in stream_ids
            or session_id in streams or type(generation) is not int or generation < 1
        ):
            raise ValueError("stream 映射或 generation 无效")
        stream_ids.add(stream_id)
        streams[session_id] = CompanionStreamConfig(stream_id, session_id, generation)
    return CompanionConfig(
        enabled=True,
        base_url=base_url,
        token_env=token_env,
        poll_interval_sec=poll_interval,
        request_timeout_sec=timeout,
        database_path=str(doc.get("database_path") or "").strip(),
        streams=streams,
    )


def _path() -> Path | None:
    base = resolve_custom_dir()
    return base / "runs_companion.yaml" if base else None


_snapshot = CompanionConfig()
_loaded = False
_load_error = ""


def load_companion_config() -> CompanionConfig:
    """缺省关闭；坏档留下进程级错误，绝不回退 legacy 形成第二出口。"""
    global _snapshot, _loaded, _load_error
    if _loaded:
        return _snapshot
    path = _path()
    if path is not None and path.is_file():
        try:
            doc = load_yaml(path.read_text(encoding="utf-8"))
            if doc is None:
                doc = {}
            if not isinstance(doc, dict):
                raise ValueError("顶层必须是 YAML 映射")
            _snapshot = parse_companion_config(doc)
        except Exception as exc:
            _load_error = type(exc).__name__
            _snapshot = CompanionConfig()
            logger.warning(f"[HERMES runs-companion] 配置加载失败，交付失败关闭：{_load_error}")
    _loaded = True
    return _snapshot


def companion_config_error() -> str:
    load_companion_config()
    return _load_error


def stream_for_session(session_id: str) -> CompanionStreamConfig | None:
    cfg = load_companion_config()
    return cfg.streams.get(session_id) if cfg.enabled else None


def database_file(cfg: CompanionConfig) -> Path:
    base = resolve_custom_dir()
    if cfg.database_path:
        path = Path(os.path.expanduser(cfg.database_path))
        if path.is_absolute() or base is None:
            return path
        return base / path
    if base is None:
        raise ValueError("无法确定 companion 台账目录")
    return base / "runs_companion_ledger.db"
