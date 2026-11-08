"""分级反馈规则引擎(规则文件化,可自定义;schema v2)。

规则来源:定制目录下的 `feedback_rules.yaml`,现读现用(保存即生效)。
目录缺省=localstore 插件数据目录(定稿);文件缺失/解析失败/规则为空
→ 回退内置默认两条并 WARN(每进程只 WARN 一次)。

章三迁移:旧 `feedback_rules.json` 不再读取——存在时 WARN 提示迁移,
不双轨并行。人改的配置一律 YAML(可注释,媒体侧 media_config.yaml
先例)。模板见插件根目录 `feedback_rules.example.yaml`。

文件格式(schema v2,带版本号;顶层加 key 不炸旧文件):
```yaml
version: 2
rules:
  - name: doc-quote            # 规则名(便签去重键 + 日志标识)
    pattern: "[「」]"          # 正则检出(可选;自带锚,不受 position 约束)
    keywords: ["稍后汇报"]      # 关键词检出(可选;与 pattern 并存,任一命中即算)
    position: full             # 关键词命中位置:full 全文(缺省) | head 句首 | tail 句尾
                               # 只约束 keywords;pattern 正则自带锚不受影响
    where: reply               # reply=正式回复 | interim=旁白 | all=两者
    action: note               # note 便签(缺省) | confirm 离场确认轮
                               # relay 放行+接力 | bounce 拦截+重交
    note: "小提示:……"          # action=note 必填,贴进下一轮的文案
    prompt: "……"               # confirm/relay/bounce 的后续轮提示词覆盖(可选)
    cooldown_sec: 86400        # 同类事件去重窗口(note 缺省 24h;confirm 缺省 300s)
    match: envelope_json       # 特殊检出器(可选):内置「信封 JSON 残片」硬形态检测,
                               # 配 action=bounce 用;写了才拦截,缺省不拦截
```

防呆:每条 pattern 加载即 compile,坏规则跳过并 WARN(其余规则不受影响);
嵌套量词(灾难性回溯形态)加载时静态拒收;运行时仍无
超时保护,未覆盖形态见 _DANGEROUS_NESTED_QUANT_RE 注释与乙方案备选。

动作语义:
- note:task_notes.push_plugin_note(kind=rule.name)——24h 同类去重由便签层承载。
- confirm:命中即按规则的 cooldown 追加一发离场确认轮(提示词可用 rule.prompt 覆盖内置)。
- relay:回复照发(它就是「接任务回执」),随后追加一发「继续执行轮」;同 scope 连续
  接力硬计数,上限 2 次,超限照发并 WARN。
- bounce:回复不投递,追加重交轮;同 scope 连续计数,第二次检出起放行残片 + 贴便签。
- confirm/relay/bounce 只对 reply 生效——旁白是过程文本,不当真触发后续轮。
- 总闸:缺省不拦截——没有任何 relay/bounce 规则时,
  系统无任何拦截/接力行为。
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import yaml
from nonebot import logger

from ..config import resolve_custom_dir
from .decision_contract import DECISION_FIELDS

_RULES_FILENAME = "feedback_rules.yaml"
_LEGACY_JSON_FILENAME = "feedback_rules.json"

# 信封锁字段名集合(单一来源=decision_contract,加字段自动跟随)——类②硬形态检测用。
_ENVELOPE_FIELD_NAMES: tuple[str, ...] = tuple(f.name for f in DECISION_FIELDS)
# 带引号的「字段名 + 冒号」形态 = 信封残片的指纹(宁窄勿滥:不接受纯引号词)。
_QUOTED_ENVELOPE_FIELD_RE = re.compile(
    r'"(?:' + "|".join(re.escape(n) for n in _ENVELOPE_FIELD_NAMES) + r')"\s*:'
)

# 关键词位置切句:中英句末标点 + 分号 + 换行(句首/句尾的「句」按此拆)。
_SENTENCE_SPLIT_RE = re.compile(r"[。！？!?；;\n]+")

# 加载时静态预检灾难性回溯形态。
# 检测「组内含 +/* 量词、组外又紧跟 +/* 量词」的嵌套量词(如 (a+)+、(\w+)*),
# 命中即拒收该规则——这类写法在不匹配时指数爆炸,会同步卡死整个事件循环。
# 首版从窄:只抓括号不嵌套的两层形态;(x|x)* 重叠分支、(a{2,})+ 花括号嵌套、
# (a+(b+))+ 多层嵌套暂不抓(宁窄勿滥,误拒会让合法规则失效)。
# 备选(未实施,乙方案):换装第三方 regex 库做运行时 timeout 保护——能覆盖全部
# 回溯形态而非仅嵌套量词,代价是 venv 加依赖;需要时在 nb venv 装 regex 并改
# matches() 传 timeout 即可。
_DANGEROUS_NESTED_QUANT_RE = re.compile(r"\((?:\?[:=!]?)?[^()]*[+*][^()]*\)[+*]")

_POSITIONS = ("full", "head", "tail")
_MATCHES = ("", "envelope_json")
_ACTIONS = ("note", "confirm", "relay", "bounce")
_WHERES = ("reply", "interim", "all")


@dataclass
class FeedbackRule:
    name: str
    regex: "re.Pattern[str] | None"
    where: str  # reply / interim / all
    action: str  # note / confirm / relay / bounce
    note: str = ""
    prompt: str = ""  # confirm/relay/bounce 动作的后续轮提示词覆盖
    cooldown_sec: float = 0.0  # 0 = 用动作缺省(note 24h / confirm 300s)
    keywords: tuple[str, ...] = ()
    position: str = "full"  # full / head / tail(只约束 keywords)
    match: str = ""  # "" = 正则+关键词;envelope_json = 内置硬形态检测器

    def matches(self, text: str) -> bool:
        """统一命中判定(所有消费点只走这里,别各自 rule.regex.search)。"""
        if not text:
            return False
        if self.match == "envelope_json":
            return detect_envelope_residue(text)
        if self.regex is not None and self.regex.search(text):
            return True
        if self.keywords and _keyword_hit(text, self.keywords, self.position):
            return True
        return False


# 内置默认规则。pattern 用窄表,宁可漏不可滥。
# 落盘形态(=feedback_rules.example.yaml 的注释版)与这里逐字段同义。
_BUILTIN_RULES = (
    {
        "name": "doc-quote",
        "pattern": r"[「」]",
        "where": "all",
        "action": "note",
        "note": "群聊要求全角标点，你上一轮的消息里出现了直角引号「」，双引号请用“”。",
    },
    {
        "name": "departure-promise",
        "pattern": r"(稍后|回头|晚点|待会[儿]?|一会[儿]?|之后).{0,6}(汇报|告诉你|通知你|回复你|同步|更新)"
                   r"|(汇报|通知|告诉).{0,4}(稍后|回头|晚点|待会[儿]?|一会[儿]?)",
        "where": "reply",
        "action": "confirm",
    },
)


def _keyword_hit(text: str, keywords: tuple[str, ...], position: str) -> bool:
    """关键词命中判定。position 只约束关键词:full=全文;head/tail=首句/末句。

    句按 [。！？!?；;\\n] 拆;没有句末标点时整段算一句(startswith/endswith 的
    窄口径自然被覆盖)。
    """
    if position == "full":
        return any(k in text for k in keywords)
    segments = [s.strip() for s in _SENTENCE_SPLIT_RE.split(text) if s.strip()]
    if not segments:
        return False
    scope = segments[0] if position == "head" else segments[-1]
    return any(k in scope for k in keywords)


def detect_envelope_residue(text: str) -> bool:
    """类②检出:信封 JSON 残片硬形态(首版从窄,宁漏勿滥——规划书 §3.3 Q3)。

    形态 A:含 `"should_reply"` 字面值但 maybe_extract_decision_reply_text 提取失败
            (破信封被当正文送出);
    形态 B:提取后残余文本仍含带引号的信封字段名形态(如 `"reply_text":`)。
    豁免:含 ``` 代码块包裹的合法 JSON 讨论(首版不做语义判断,只对上面两种硬形态点火)。

    只报「有残片」,拦不拦由规则配置决定(总闸:缺省不拦截)。
    """
    if not text:
        return False
    if "```" in text:  # 代码块包裹:合法 JSON 讨论的容器,一律豁免
        return False
    has_should_reply = '"should_reply"' in text
    if not has_should_reply and not _QUOTED_ENVELOPE_FIELD_RE.search(text):
        return False
    from .hermes_client import maybe_extract_decision_reply_text

    extracted = maybe_extract_decision_reply_text(text)
    if extracted is None:
        return has_should_reply  # 形态 A:信封在场痕迹抠不出来
    # 抠得出来 → 把抠出的正文摘掉,残余里还有带引号的字段名才算形态 B
    residual = text.replace(extracted, "", 1) if extracted else text
    return bool(_QUOTED_ENVELOPE_FIELD_RE.search(residual))


_warned = False
_legacy_warned = False


def _parse_rule(raw: dict) -> FeedbackRule | None:
    try:
        name = str(raw["name"]).strip()
        action = str(raw.get("action") or "note")
        where = str(raw.get("where") or "reply")
        position = str(raw.get("position") or "full")
        match = str(raw.get("match") or "")
        if not name or where not in _WHERES or action not in _ACTIONS:
            raise ValueError("name/where/action 非法")
        if position not in _POSITIONS:
            raise ValueError(f"position 非法: {position!r}")
        if match not in _MATCHES:
            raise ValueError(f"match 非法: {match!r}")

        raw_kw = raw.get("keywords")
        keywords: tuple[str, ...] = ()
        if raw_kw is not None:
            if isinstance(raw_kw, str):  # 单串宽容成单元素表
                raw_kw = [raw_kw]
            if not isinstance(raw_kw, list):
                raise ValueError("keywords 必须是字符串列表")
            keywords = tuple(str(k).strip() for k in raw_kw if str(k).strip())

        raw_pattern = raw.get("pattern")
        regex = None
        if raw_pattern:
            pattern_str = str(raw_pattern)
            # 嵌套量词静态预检,灾难形态拒收(其余规则不受影响)。
            if _DANGEROUS_NESTED_QUANT_RE.search(pattern_str):
                raise ValueError("正则含嵌套量词,灾难性回溯风险,拒收该规则")
            regex = re.compile(pattern_str)
        if regex is None and not keywords and match != "envelope_json":
            raise ValueError("规则没有任何检出手段(pattern/keywords/match)")

        note = str(raw.get("note") or "")
        if action == "note" and not note:
            raise ValueError("action=note 缺 note 文案")
        return FeedbackRule(
            name=name, regex=regex, where=where, action=action,
            note=note, prompt=str(raw.get("prompt") or ""),
            cooldown_sec=float(raw.get("cooldown_sec") or 0),
            keywords=keywords, position=position, match=match,
        )
    except Exception as exc:
        logger.warning(f"[HERMES feedback-rules] 规则解析失败跳过 ({raw.get('name')!r}): {exc}")
        return None


def load_rules() -> list[FeedbackRule]:
    """加载反馈规则(feedback_rules.yaml)。文件缺失/坏档/空规则 → 内置默认。

    现读现用,无缓存。旧 feedback_rules.json 存在时 WARN 提示迁移(不双轨,已忽略)。
    """
    global _warned, _legacy_warned
    custom_dir = resolve_custom_dir()
    if custom_dir is None:
        return _load_builtin()
    path = custom_dir / _RULES_FILENAME
    if not path.is_file():
        legacy = custom_dir / _LEGACY_JSON_FILENAME
        if legacy.is_file() and not _legacy_warned:
            logger.warning(
                f"[HERMES feedback-rules] 检测到旧规则文件 {legacy.name}(JSON),已不再读取"
                f"——请迁移为 {_RULES_FILENAME}(YAML,支持注释),迁移后删除旧文件"
            )
            _legacy_warned = True
        return _load_builtin()

    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
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
