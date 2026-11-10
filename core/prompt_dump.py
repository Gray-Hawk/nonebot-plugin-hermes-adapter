"""生产提示词请求留痕（排障用，默认关闭）。

用途：排查「模型到底收到了什么」时，能拿到某轮真实上发请求的完整结构——
system 指令、messages/input 里的 user content（含 recent/current 行、锚块、
近因提醒）以及接口特有的外层字段。只存最终拼装文本不够：呈递/渲染改造要看
的就是 user content 里每一行的字节。Hermes 转录不存 system 消息、网关 dump
只在请求失败时生成，事后无据可查。

形态：
- 截留点在 hermes_client.chat 发送前（system_prompt 最终成形、请求体组装完成
  之后，通道分流之前）：runs 抓 instructions+input，completions/legacy 抓真实
  POST 的 messages，astrbot 抓本地侧请求视图（图片另走附件上传）。
- 只留最新一份，不攒历史版本：桶目录
  <custom_dir>/prompt_dumps/private|group/<平台>_<id>.json，
  SHA256（对完整请求结构算）变了才覆写、没变跳过；磁盘占用随会话数恒定。
- 内联二进制（`data:` URL）转储时换成长度占位，其余文本逐字节保留——一张
  内联图是 MB 级二进制，原样落盘会淹没真正要看的那几行。
- 开关 misc_config.yaml 的 prompt_dump_enabled（缺省 false），启动装载；
  管理员手动重载成功后同步刷新，坏档重载保留旧状态；custom_dir 不可用时静默跳过。
- 覆写时 nb 日志 INFO 一行（sha+长度+路径），排查时按日志找文件。
- 落盘内容含画像与主机路径，仅存本地目录，不进主日志刷屏；排查完记得关回。

契约：绝不抛——留痕失败只 WARN，不能打炸主链路。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime
from typing import Any

from nonebot import logger

from ..config import resolve_custom_dir
from .misc_config import load_misc_config

_enabled: bool = False


def init_prompt_dump() -> None:
    """启动装载（进程级缓存）。开关开启时启动期明示，防「配了忘了」。"""
    global _enabled
    _enabled = load_misc_config().prompt_dump_enabled
    if _enabled:
        logger.info("[HERMES] 提示词请求留痕已开启: 落盘 <custom_dir>/prompt_dumps/")


_SAFE_CHARS = re.compile(r"[^0-9A-Za-z_-]")
_CHECKSUM_RE = re.compile(r'"sha256":\s*"([0-9a-f]{64})"')
# 内联二进制占位阈值:比这短的 data: URL 不至于淹没文本,照原样留。
_DATA_URL_MIN_LEN = 512


def _sanitize(raw: str | None) -> str:
    """文件名消毒：只留数字字母下划线连字符，防空名与路径注入。"""
    cleaned = _SAFE_CHARS.sub("_", (raw or "").strip())
    return cleaned or "unknown"


def _shrink(value: Any) -> Any:
    """把内联二进制（`data:` URL）换成长度占位，其余结构与文本原样保留。"""
    if isinstance(value, dict):
        return {k: _shrink(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_shrink(v) for v in value]
    if isinstance(value, str) and value.startswith("data:") and len(value) > _DATA_URL_MIN_LEN:
        head, _, body = value.partition(",")
        return f"{head},<omitted {len(body)} chars base64>"
    return value


def maybe_dump_request(
    *,
    adapter_name: str,
    is_private: bool,
    user_id: str,
    group_id: str | None,
    interface: str,
    request: dict,
    note: str | None = None,
) -> None:
    """按完整请求结构的 SHA 覆写留痕。

    request 是本轮真实上发的请求结构（或接口侧等价视图）：指令/system 字段与
    messages/input 的 user content 都在里面，不再只存拼装后的 system 文本。
    开关关/目录不可用 → 零开销直返；任何异常只 WARN。
    """
    if not _enabled:
        return
    try:
        base = resolve_custom_dir()
        if base is None:
            return
        bucket = "private" if is_private else "group"
        scope_id = user_id if is_private else (group_id or "unknown")
        path = (
            base
            / "prompt_dumps"
            / bucket
            / f"{_sanitize(adapter_name)}_{_sanitize(scope_id)}.json"
        )
        view = {
            "interface": interface,
            "scope": {
                "adapter": adapter_name,
                "is_private": bool(is_private),
                "user_id": str(user_id),
                "group_id": None if group_id is None else str(group_id),
            },
            "request": _shrink(request),
        }
        if note:
            view["note"] = note
        canonical = json.dumps(view, ensure_ascii=False, sort_keys=True)
        sha = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        # 首行即 SHA 行：相同则跳过（重启后首行比对仍成立，不靠内存态）。
        # sha 必须写在最前——请求体可能有几十 KB，只读文件头比对。
        try:
            with path.open("r", encoding="utf-8") as f:
                head = f.read(256)
            found = _CHECKSUM_RE.search(head)
            if found is not None and found.group(1) == sha:
                return
        except FileNotFoundError:
            pass
        body: dict[str, Any] = {
            "sha256": sha,
            "length": len(canonical),
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        body.update(view)
        text = json.dumps(body, ensure_ascii=False, indent=2, sort_keys=False)
        # 先写临时文件再原子替换，防中途崩溃留半截
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(text + "\n", encoding="utf-8")
        os.replace(tmp, path)
        logger.info(
            f"[HERMES] 提示词请求已留痕: {path} "
            f"(sha256={sha[:12]}, len={len(canonical)}, interface={interface})"
        )
    except Exception:
        logger.warning("[HERMES] 提示词请求留痕失败（不影响主链路）", exc_info=True)
