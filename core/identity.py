"""用户身份表（跨群用户画像系统残项重启）。

存储：<hermes_custom_dir>/memory/<适配器>/identities.csv——人手可改
（CSV，表格类数据用 CSV，方便手动编辑/表格软件打开；
存储量无上限，纯文本随便长），助手软触发维护也用 write_file/patch 直改。
插件侧只读 + mtime 缓存（文件出现/消失/改动自然失效重建，与 group_profile
同套缓存语义）。

列格式（首行表头固定）：

    user_id,nickname,aliases,merged_into,note
    1000000004,测试友,粉毛、狐狸,,
    123456,,,1000000004,小号

- user_id：QQ 号（字符串，CSV 里数字会被表格软件吞前导零无所谓，QQ 无前导零）
- nickname：正主名（发言者标签注释用）
- aliases：已知别名/绰号，多个用中文顿号「、」分隔（半角逗号是列分隔符，禁用）
- merged_into：小号并入主档时填主档 user_id，该行其余列留空
- note：自由备注

与画像体系挂钩：merged_into 的号读 people/ 画像时跳主档
（resolve_people_id，group_profile 侧接线）；发言者标签注释走
resolve_display_name（prompt_builder 侧接线）。
"""
from __future__ import annotations

import csv
import io
from pathlib import Path

from nonebot import logger

from .config_bootstrap import ensure_file
from .misc_config import load_misc_config

# path → (签名, {user_id: entry})；签名失效重建，负缓存（文件不存在）也缓存
# 签名 = (mtime_ns, size)：本机文件系统 mtime 粒度很粗（同毫秒连写不变，
# 冒烟实证），单看 mtime 会漏掉快速连改；同长度同瞬间改写仍有
# 残余风险，生产场景人工编辑间隔远大于此，可接受。
_cache: dict[str, tuple[tuple[int | None, int], dict[str, dict]]] = {}

# 缺档自动建档写入的表头（严格固定，首行）。
_HEADER = "user_id,nickname,aliases,merged_into,note\n"


def _load_raw(path: Path) -> dict[str, dict]:
    key = str(path)
    try:
        st = path.stat()
        sig: tuple[int | None, int] = (st.st_mtime_ns, st.st_size)
    except OSError:
        sig = (None, 0)
    cached = _cache.get(key)
    if cached and cached[0] == sig:
        return cached[1]
    data: dict[str, dict] = {}
    if sig[0] is not None:
        try:
            rows = csv.DictReader(io.StringIO(path.read_text(encoding="utf-8")))
            for row in rows:
                uid = (row.get("user_id") or "").strip()
                if uid:
                    data[uid] = {k: (v or "").strip() for k, v in row.items() if k}
        except Exception as exc:
            logger.warning(f"[identity] {path} 读取/解析失败，按空表处理: {exc}")
            data = {}
    _cache[key] = (sig, data)
    return data


def _path(custom_dir: Path, adapter: str) -> Path:
    return custom_dir / "memory" / adapter / "identities.csv"


def _ensure_table(custom_dir: Path | None, adapter: str) -> None:
    """功能开启且缺档时自动建档（连带建父目录）；失败按空表容错，不抛异常。"""
    if custom_dir is None:
        return
    if not load_misc_config().group_profile_enabled:
        return
    try:
        ensure_file(_path(custom_dir, adapter), _HEADER, "identity", label="身份表")
    except Exception:
        return


def _resolve_entry(custom_dir: Path, adapter: str, user_id: str) -> tuple[str, dict] | None:
    """返回 (主档 user_id, 主档条目)。merged_into 只跟一跳，自环/断链按无主档。"""
    _ensure_table(custom_dir, adapter)
    entry = _load_raw(_path(custom_dir, adapter)).get(str(user_id))
    if entry is None:
        return None
    merged = entry.get("merged_into") or ""
    if merged:
        primary = _load_raw(_path(custom_dir, adapter)).get(merged)
        if primary is not None and not (primary.get("merged_into") or ""):
            return merged, primary
        return None
    return str(user_id), entry


def resolve_display_name(custom_dir: Path | None, adapter: str, user_id: str) -> str | None:
    """发言者标签注释用：返回正主名，查不到/异常返回 None（热路径静默）。"""
    if custom_dir is None:
        return None
    try:
        hit = _resolve_entry(custom_dir, adapter, user_id)
        if hit is None:
            return None
        return hit[1].get("nickname") or None
    except Exception:
        return None


def resolve_people_id(custom_dir: Path | None, adapter: str, user_id: str) -> str:
    """画像文件归属：merged 号返回主档 user_id，否则原样返回（画像读主档文件）。"""
    if custom_dir is None:
        return user_id
    try:
        hit = _resolve_entry(custom_dir, adapter, user_id)
        return hit[0] if hit else user_id
    except Exception:
        return user_id


def clear_cache() -> None:
    """测试/调试用。"""
    _cache.clear()
