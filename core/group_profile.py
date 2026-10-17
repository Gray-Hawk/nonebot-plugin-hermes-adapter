"""群档案（群记忆外挂）：按「适配器+群号」读取策展式纯文本档案，拼成字节稳定的注入块。

设计定稿：项目文档库（2026-10-14 详细定稿）
要点：
- 存储：<hermes_custom_dir>/memory/<适配器>/groups/<群号>.md、people/<QQ号>.md
- 群友行：群档案内单独一行 "群友: 1000000004, 1000000005"（冒号兼容全角，分隔兼容中文逗号）
- 群友排序：去重后按 QQ 号数值升序——字节稳定钉死在代码侧，与文件书写顺序无关
- 缓存：key=(adapter, group_id)，按涉及文件的 mtime 签名失效；缺失文件以 None 参与签名
- 上限：块超 _MAX_BLOCK_CHARS 只 WARN 不截断（防画像越写越肥悄悄吃提示词预算）
- 纪律：未配 custom_dir / 文件缺失 / 私聊(group_id=None) / 路径异常 一律静默返回 None
"""

from __future__ import annotations

import re
from pathlib import Path

from nonebot import logger

from ..config import plugin_config, resolve_custom_dir
from . import identity as _identity

# 路径段白名单：适配器名/群号/QQ 号只许这些字符，异常值按档案缺失处理（防穿出目录）。
_VALID_SEGMENT = re.compile(r"^[0-9A-Za-z_-]+$")

# 群友行：「群友」+ 冒号（兼容全角）+ QQ 号列表（分隔兼容中文逗号与空白）。
_BUDDY_LINE = re.compile(r"^群友\s*[:：]\s*(\S.*)$")

# 单群注入块体积上限（2026-10-14 维护者拍板）：超限只 WARN 不截断。
_MAX_BLOCK_CHARS = 4000

# key=(adapter, group_id) → (签名, 注入块|None)。签名=涉及文件 (路径, mtime) 元组，
# 缺失文件 mtime=None；None 块也缓存（负缓存），文件创建后 mtime 变化自然失效。
_cache: dict[tuple[str, str], tuple[tuple[tuple[str, "float | None"], ...], "str | None"]] = {}


def _valid_segment(value: str | None) -> bool:
    return bool(value) and bool(_VALID_SEGMENT.match(value))


def _mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except OSError:
        return None


def _parse_buddy_ids(group_text: str) -> list[str]:
    """抠群友清单：只认第一行匹配，去重后按 QQ 号数值升序（定稿三.2/三.3）。"""
    for line in group_text.splitlines():
        m = _BUDDY_LINE.match(line.strip())
        if m:
            tokens = re.split(r"[,，\s]+", m.group(1))
            ids = {t for t in tokens if t.isdigit()}
            return sorted(ids, key=int)
    return []


def sync_buddy_to_group(adapter: str | None, group_id: str | None, user_id: str | None) -> bool:
    """发言者已有画像但不在群友行时，自动补进群友行（2026-10-15 维护者定案）。

    每次群消息入站时检查：user_id 在 people/ 目录有画像，但不在当前群友行，
    则自动加入群友行。返回是否有实际更新。
    """
    if not (adapter and group_id and user_id and _valid_segment(adapter) and _valid_segment(group_id) and _valid_segment(user_id)):
        return False
    if not plugin_config.hermes_group_profile_enabled:
        return False
    try:
        custom_dir = resolve_custom_dir()
        if custom_dir is None:
            return False
        group_path, people_dir = _profile_paths(custom_dir, adapter, group_id)
        # 身份表挂钩（2026-10-15）：merged 小号的画像归主档文件
        people_file = people_dir / f"{_identity.resolve_people_id(custom_dir, adapter, user_id)}.md"
        if not people_file.exists():
            return False  # 无画像，不自动补
        group_text = _read_text(group_path)
        if group_text is None:
            return False  # 群档案不存在（应该已被自动创建，防御）
        buddy_ids = _parse_buddy_ids(group_text)
        if user_id in buddy_ids:
            return False  # 已在群友行
        # 补进群友行：保持数值升序，重写群友行
        buddy_ids.append(user_id)
        buddy_ids = sorted(set(buddy_ids), key=int)
        new_lines = []
        replaced = False
        for line in group_text.splitlines():
            if _BUDDY_LINE.match(line.strip()) and not replaced:
                new_lines.append(f"群友: {', '.join(buddy_ids)}")
                replaced = True
            else:
                new_lines.append(line)
        if not replaced:
            new_lines.append(f"群友: {', '.join(buddy_ids)}")
        group_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
        logger.info(f"[HERMES group-profile] 自动补群友: {user_id} → 群 {group_id}")
        # 清缓存，下次重建
        _cache.pop((adapter, group_id), None)
        return True
    except Exception as exc:
        logger.warning(f"[HERMES group-profile] 自动补群友失败 {user_id}→{group_id}: {exc}")
        return False


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return None


def _assemble(group_text: str, buddies: list[tuple[str, str]]) -> str:
    """钉死的包裹格式（定稿三.4）：段间单空行，文件原文去首尾空白后嵌入；
    无群友时「# 群友画像」段整段不出现。不塞任何动态值。"""
    sections = ["# 群档案\n" + group_text.strip()]
    if buddies:
        parts = ["# 群友画像"]
        for qq, text in buddies:
            parts.append(f"## {qq}\n{text.strip()}")
        sections.append("\n\n".join(parts))
    return "<group_profile>\n" + "\n\n".join(sections) + "\n</group_profile>"


def _profile_paths(custom_dir: Path, adapter: str, group_id: str) -> tuple[Path, Path]:
    """档案路径构造的唯一收口（2026-10-14 维护者注：文件结构后续可能调整，
    改结构只动这个函数）。返回 (群档案路径, 群友画像目录)。"""
    base = custom_dir / "memory" / adapter
    return base / "groups" / f"{group_id}.md", base / "people"


def _build_block(
    adapter: str, group_id: str
) -> tuple[tuple[tuple[str, "float | None"], ...], "str | None"]:
    """实际构建。返回 (签名, 块|None)；目录不可用/文件缺失都落 None 或负缓存。"""
    custom_dir = resolve_custom_dir()
    if custom_dir is None:
        return (), None  # 开关关闭或解析失败：不缓存，下轮重试
    group_path, people_dir = _profile_paths(custom_dir, adapter, group_id)
    # 2026-10-15 补丁：档案不存在时自动创建基础模板，避免"群档案丢失"漏洞
    if not group_path.exists():
        try:
            group_path.parent.mkdir(parents=True, exist_ok=True)
            group_path.write_text(f"群 {group_id}。\n群友: \n", encoding="utf-8")
            logger.info(f"[HERMES group-profile] 自动创建群档案: {group_path}")
        except OSError as exc:
            logger.warning(f"[HERMES group-profile] 自动创建失败 {group_path}: {exc}")
    signature: list[tuple[str, float | None]] = [(str(group_path), _mtime(group_path))]
    group_text = _read_text(group_path)
    if group_text is None:
        return tuple(signature), None
    buddies: list[tuple[str, str]] = []
    # 身份表挂钩（2026-10-15）：merged 小号读主档画像文件
    for qq in _parse_buddy_ids(group_text):
        p = people_dir / f"{_identity.resolve_people_id(custom_dir, adapter, qq)}.md"
        signature.append((str(p), _mtime(p)))
        text = _read_text(p)
        if text is not None:  # 群友文件缺失：跳过该人，不炸（定稿五.4）
            buddies.append((qq, text))
    block = _assemble(group_text, buddies)
    if len(block) > _MAX_BLOCK_CHARS:
        logger.warning(
            f"[HERMES group-profile] 注入块超限 ({len(block)}>{_MAX_BLOCK_CHARS}, "
            f"group={group_id})，按定稿不截断，请瘦身档案"
        )
    return tuple(signature), block


def build_group_profile_block(adapter: str | None, group_id: str | None) -> str | None:
    """返回该群的注入块；私聊/无档案/异常一律 None（静默跳过，定稿三.9）。

    每轮调用：mtime 签名命中直接返回缓存块，否则重建。绝不抛异常——
    档案是增强不是命门，挂了不能拖累消息处理。
    """
    try:
        if not plugin_config.hermes_group_profile_enabled:
            return None  # 功能开关关闭（缺省开，2026-10-14 维护者定稿）
        if not (adapter and group_id and _valid_segment(adapter) and _valid_segment(group_id)):
            return None
        key = (adapter, group_id)
        cached = _cache.get(key)
        if cached is not None:
            signature, block = cached
            if all(_mtime(Path(p)) == m for p, m in signature):
                return block
        signature, block = _build_block(adapter, group_id)
        if signature:  # 空签名=未配 custom_dir，不进缓存
            _cache[key] = (signature, block)
        return block
    except Exception as exc:
        logger.warning(f"[HERMES group-profile] 构建失败静默跳过: {exc}")
        return None
