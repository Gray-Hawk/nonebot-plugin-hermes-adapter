"""system_prompt 全文留痕（排障用，默认关闭）。

用途：排查「系统提示词里有脏东西」时，能拿到某轮真实上发的 system_prompt 字节。
Hermes 转录不存 system 消息，网关 dump 只在请求失败时生成，事后无据可查。

形态：
- 截留点在 hermes_client 发送前（system_prompt 最终成形之后、通道分流之前，
  passive/reactive 与 Completions/Runs 抓到的是同一份真实上发字节）。
- 只留最新一份，不攒历史版本：桶目录
  <custom_dir>/prompt_dumps/private|group/<平台>_<id>.txt，
  SHA256 变了才覆写、没变跳过；磁盘占用随会话数恒定。
- 开关 .env 的 HERMES_PROMPT_DUMP_ENABLED（缺省 false），启动时装载+缓存，
  改动需重启生效；custom_dir 不可用（回退 None）时静默跳过。
- 覆写时 nb 日志 INFO 一行（sha+长度+路径），排查时按日志找文件。
- 落盘内容含画像与主机路径，仅存本地目录，不进主日志刷屏；排查完记得关回。

契约：绝不抛——留痕失败只 WARN，不能打炸主链路。
"""

from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime

from nonebot import logger

from ..config import plugin_config, resolve_custom_dir

_enabled: bool = False


def init_prompt_dump() -> None:
    """启动装载（进程级缓存）。开关开启时启动期明示，防「配了忘了」。"""
    global _enabled
    _enabled = bool(getattr(plugin_config, "hermes_prompt_dump_enabled", False))
    if _enabled:
        logger.info("[HERMES] system_prompt 留痕已开启: 落盘 <custom_dir>/prompt_dumps/")


_SAFE_CHARS = re.compile(r"[^0-9A-Za-z_-]")


def _sanitize(raw: str | None) -> str:
    """文件名消毒：只留数字字母下划线连字符，防空名与路径注入。"""
    cleaned = _SAFE_CHARS.sub("_", (raw or "").strip())
    return cleaned or "unknown"


def maybe_dump_system_prompt(
    *,
    adapter_name: str,
    is_private: bool,
    user_id: str,
    group_id: str | None,
    system_prompt: str,
) -> None:
    """按 SHA 覆写留痕。开关关/目录不可用 → 零开销直返；任何异常只 WARN。"""
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
            / f"{_sanitize(adapter_name)}_{_sanitize(scope_id)}.txt"
        )
        sha = hashlib.sha256(system_prompt.encode("utf-8")).hexdigest()
        # 首行即 SHA 行：相同则跳过（重启后首行比对仍成立，不靠内存态）
        try:
            with path.open("r", encoding="utf-8") as f:
                first_line = f.readline()
            if sha in first_line:
                return
        except FileNotFoundError:
            pass
        header = (
            f"# sha256: {sha}\n"
            f"# length: {len(system_prompt)}\n"
            f"# time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"# scope: {adapter_name} {bucket} {scope_id}\n"
            "\n"
        )
        # 先写临时文件再原子替换，防中途崩溃留半截
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".txt.tmp")
        tmp.write_text(header + system_prompt, encoding="utf-8")
        os.replace(tmp, path)
        logger.info(
            f"[HERMES] system_prompt 已留痕: {path} (sha256={sha[:12]}, len={len(system_prompt)})"
        )
    except Exception:
        logger.warning("[HERMES] system_prompt 留痕失败（不影响主链路）", exc_info=True)
