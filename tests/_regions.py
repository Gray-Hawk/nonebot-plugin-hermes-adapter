"""测试共用判据:聊天消息区域块。

steer 与普通消息现在共用 `prompt_builder.render_chat_region` 一个构造点,所以测试
要么断言**整块字节**(`wrap_region`),要么只断言**区域内消息行**(`region_rows`,
框架另有断言,取出失败会直接抛)。两处都从 prompt_builder 的常量取字面量,不再
在测试里各抄一份标签文本。
"""
from __future__ import annotations


def _pb():
    from nonebot_plugin_hermes_adapter.core import prompt_builder

    return prompt_builder


def wrap_region(rows: str) -> str:
    """消息行正文 → 完整区域块(开标签、固定说明、行、闭标签,段落之间空一行)。"""
    pb = _pb()
    return "\n\n".join([pb._CHAT_REGION_OPEN, pb._CHAT_REGION_NOTE, rows, pb._CHAT_REGION_CLOSE])


def region_rows(text: str) -> str:
    """区域块 → 区域内消息行正文;不是区域块就抛 AssertionError。"""
    pb = _pb()
    lines = text.split("\n")
    if len(lines) < 7:
        raise AssertionError(f"不是区域块(行数不足): {text!r}")
    if lines[0] != pb._CHAT_REGION_OPEN:
        raise AssertionError(f"缺区域开标签: {lines[0]!r}")
    if lines[1] != "":
        raise AssertionError(f"开标签后缺空行: {text!r}")
    if lines[2] != pb._CHAT_REGION_NOTE:
        raise AssertionError(f"缺区域固定说明: {lines[2]!r}")
    if lines[3] != "":
        raise AssertionError(f"固定说明后缺空行: {text!r}")
    if lines[-1] != pb._CHAT_REGION_CLOSE:
        raise AssertionError(f"缺区域闭标签: {lines[-1]!r}")
    if lines[-2] != "":
        raise AssertionError(f"闭标签前缺空行: {text!r}")
    return "\n".join(lines[4:-2])


def is_region(text: str) -> bool:
    try:
        region_rows(text)
    except AssertionError:
        return False
    return True
