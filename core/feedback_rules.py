"""分级反馈规则引擎(2026-10-13,维护者定案:规则文件化,可自定义)。

规则来源:定制定居目录下的 `feedback_rules.json`,现读现用(保存即生效)。
目录缺省=localstore 插件数据目录(2026-10-14 定稿);文件缺失/解析失败/规则为空
→ 回退内置默认两条并 WARN(每进程只 WARN 一次)。

文件格式(带版本号,未来顶层加 key 不炸旧文件):
```json
{
  "version": 1,
  "rules": [
    {
      "name": "doc-quote",
      "pattern": "[「」]",
      "where": "reply",            // reply=正式回复 | interim=旁白 | all=两者
      "action": "note",            // note=下轮暗示便签 | confirm=追加离场确认轮
      "note": "小提示:……",          // action=note 必填,贴进下一轮的文案
      "cooldown_sec": 86400        // 同类提示去重窗口,缺省 24h;confirm 缺省 300s
    }
  ]
}
```

防呆:每条 pattern 加载即 compile,坏规则跳过并 WARN(其余规则不受影响);
正则别写灾难性回溯(嵌套量词),本文件不做运行时超时保护。

动作语义:
- note:task_notes.push_plugin_note(kind=rule.name)——24h 同类去重由便签层承载。
- confirm:命中即按规则的 cooldown 追加一发离场确认轮(确认轮提示词可用规则的
  "prompt" 字段覆盖内置)。confirm 只对 reply 生效——旁白是过程文本,不当真触发确认。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from nonebot import logger

from ..config import resolve_custom_dir


@dataclass
class FeedbackRule:
    name: str
    regex: "re.Pattern[str]"
    where: str  # reply / interim / all
    action: str  # note / confirm
    note: str = ""
    prompt: str = ""  # confirm 动作的确认轮提示词覆盖
    cooldown_sec: float = 0.0  # 0 = 用动作缺省(note 24h / confirm 300s)


# 内置默认规则(2026-10-13 维护者合成方案的两条首发)。pattern 用窄表,宁可漏不可滥。
_BUILTIN_RULES = (
    {
        "name": "doc-quote",
        "pattern": r"[「」]",
        "where": "all",
        "action": "note",
        "note": "群聊要求全角标点,你上一轮的消息里出现了直角引号「」,双引号请用“”。",
    },
    {
        "name": "departure-promise",
        "pattern": r"(稍后|回头|晚点|待会[儿]?|一会[儿]?|之后).{0,6}(汇报|告诉你|通知你|回复你|同步|更新)"
                   r"|(汇报|通知|告诉).{0,4}(稍后|回头|晚点|待会[儿]?|一会[儿]?)",
        "where": "reply",
        "action": "confirm",
    },
)

_warned = False


def _parse_rule(raw: dict) -> FeedbackRule | None:
    try:
        name = str(raw["name"]).strip()
        regex = re.compile(str(raw["pattern"]))
        where = str(raw.get("where") or "reply")
        action = str(raw.get("action") or "note")
        if not name or where not in {"reply", "interim", "all"} or action not in {"note", "confirm"}:
            raise ValueError("name/where/action 非法")
        note = str(raw.get("note") or "")
        if action == "note" and not note:
            raise ValueError("action=note 缺 note 文案")
        return FeedbackRule(
            name=name, regex=regex, where=where, action=action,
            note=note, prompt=str(raw.get("prompt") or ""),
            cooldown_sec=float(raw.get("cooldown_sec") or 0),
        )
    except Exception as exc:
        logger.warning(f"[HERMES feedback-rules] 规则解析失败跳过 ({raw.get('name')!r}): {exc}")
        return None


def load_rules() -> list[FeedbackRule]:
    """加载反馈规则。文件缺失/坏档/空规则 → 内置默认。现读现用,无缓存。"""
    global _warned
    custom_dir = resolve_custom_dir()
    path = custom_dir / "feedback_rules.json" if custom_dir else None
    if path is None or not path.is_file():
        return _load_builtin()

    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
        raws = doc.get("rules") if isinstance(doc, dict) else None
        if not isinstance(raws, list) or not raws:
            raise ValueError("rules 缺失或为空")
        rules = [r for r in (_parse_rule(x) for x in raws if isinstance(x, dict)) if r is not None]
        if not rules:
            raise ValueError("全部规则解析失败")
        return rules
    except Exception as exc:
        if not _warned:
            logger.warning(f"[HERMES feedback-rules] 规则文件不可用,回退内置默认: {exc}")
            _warned = True
        return _load_builtin()


def _load_builtin() -> list[FeedbackRule]:
    return [r for r in (_parse_rule(dict(x)) for x in _BUILTIN_RULES) if r is not None]
