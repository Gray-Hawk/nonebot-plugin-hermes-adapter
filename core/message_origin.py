"""消息来源的单一事实模型。

本模块只描述文本由谁、经哪条通道产生。平台路由、事件行为人、数据库主键和
bot 账号身份分别由既有字段承担，不从本模块反推。
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Mapping


class AuthorKind(str, Enum):
    """文本作者类型；聚合行由 constituents 保留逐段身份。"""

    HUMAN = "human"
    BOT = "bot"
    SYSTEM = "system"
    MIXED = "mixed"


class MessageSource(str, Enum):
    """消息来源闭集。值用于日志与测试，不直接作为模型可见展示文案。"""

    HUMAN_INGRESS = "human.ingress"
    HUMAN_REPLAY = "human.replay"
    BOT_OUTPUT = "bot.output"
    NOTICE_POKE = "notice.poke"
    NOTICE_JOIN = "notice.join"
    FEEDBACK_CONFIRM = "feedback.confirm"
    FEEDBACK_RELAY = "feedback.relay"
    FEEDBACK_BOUNCE = "feedback.bounce"
    RECONCILE_TIMELINE = "reconcile.timeline"
    DELIVERY_REPORT = "delivery.report"
    WAKE_MANUAL = "wake.manual"
    PATROL = "patrol"
    MIXED = "mixed"


@dataclass(frozen=True, slots=True)
class SourcePolicy:
    """一种来源的治理策略；业务文案与队列生命周期不进入此表。"""

    label: str | None
    author_kind: AuthorKind
    suppresses_feedback: bool
    allows_steer: bool
    allows_store: bool
    allows_label_override: bool = False


_POLICIES = {
    MessageSource.HUMAN_INGRESS: SourcePolicy(None, AuthorKind.HUMAN, False, True, True),
    # 真人原文回投不重复入库:原行已在库里，再写一遍就是重复行（历史里只保留原行）。
    MessageSource.HUMAN_REPLAY: SourcePolicy(None, AuthorKind.HUMAN, False, True, False),
    MessageSource.BOT_OUTPUT: SourcePolicy(None, AuthorKind.BOT, False, False, True),
    # 系统来源的展示名就是模型可见的 `[sys=…]` 标签：用业务事实命名，
    # 不用「系统」这类泛称——泛称让模型分不清是哪一类系统输入。
    MessageSource.NOTICE_POKE: SourcePolicy("戳一戳", AuthorKind.SYSTEM, False, False, False),
    MessageSource.NOTICE_JOIN: SourcePolicy("入群", AuthorKind.SYSTEM, False, False, False),
    MessageSource.FEEDBACK_CONFIRM: SourcePolicy("离场确认", AuthorKind.SYSTEM, True, False, False),
    MessageSource.FEEDBACK_RELAY: SourcePolicy("继续执行", AuthorKind.SYSTEM, True, False, False),
    MessageSource.FEEDBACK_BOUNCE: SourcePolicy("重新提交", AuthorKind.SYSTEM, True, False, False),
    MessageSource.RECONCILE_TIMELINE: SourcePolicy("补投", AuthorKind.SYSTEM, True, False, False),
    MessageSource.DELIVERY_REPORT: SourcePolicy("任务投递", AuthorKind.SYSTEM, True, False, False),
    MessageSource.WAKE_MANUAL: SourcePolicy("主动唤起", AuthorKind.SYSTEM, True, False, False, True),
    MessageSource.PATROL: SourcePolicy("定时巡查", AuthorKind.SYSTEM, True, False, False),
}
SOURCE_POLICY: Mapping[MessageSource, SourcePolicy] = MappingProxyType(_POLICIES)
assert set(_POLICIES) == set(MessageSource) - {MessageSource.MIXED}, "来源策略表必须覆盖全部非混合来源"


@dataclass(frozen=True, slots=True)
class MessageOrigin:
    """不可变来源值对象。MIXED 只能由 merge 生成，成分保持原始顺序。"""

    source: MessageSource
    label: str | None = None
    constituents: tuple[MessageOrigin, ...] = ()

    def __post_init__(self) -> None:
        if self.source is MessageSource.MIXED:
            if self.label is not None or len(self.constituents) < 2:
                raise ValueError("混合来源必须保留至少两个成分，且不接受展示名覆盖")
            if any(item.source is MessageSource.MIXED for item in self.constituents):
                raise ValueError("混合来源的成分必须先展开")
            return
        if self.constituents:
            raise ValueError("普通来源不能携带混合成分")
        policy = SOURCE_POLICY[self.source]
        if self.label is not None:
            if not policy.allows_label_override or not self.label.strip() or "\n" in self.label or "\r" in self.label:
                raise ValueError("该来源不接受此展示名")

    @property
    def policy(self) -> SourcePolicy:
        if self.source is MessageSource.MIXED:
            raise AttributeError("混合来源没有单一策略，请读取成分或聚合谓词")
        return SOURCE_POLICY[self.source]

    @property
    def author_kind(self) -> AuthorKind:
        return AuthorKind.MIXED if self.source is MessageSource.MIXED else self.policy.author_kind

    @property
    def display_label(self) -> str | None:
        if self.source is MessageSource.MIXED:
            return "混合消息"
        return self.label or self.policy.label

    @property
    def is_pure_human(self) -> bool:
        return self.source in (MessageSource.HUMAN_INGRESS, MessageSource.HUMAN_REPLAY)

    @property
    def has_human_content(self) -> bool:
        if self.source is MessageSource.MIXED:
            return any(item.has_human_content for item in self.constituents)
        return self.author_kind is AuthorKind.HUMAN

    @property
    def has_system_content(self) -> bool:
        if self.source is MessageSource.MIXED:
            return any(item.has_system_content for item in self.constituents)
        return self.author_kind is AuthorKind.SYSTEM

    @property
    def suppresses_feedback(self) -> bool:
        if self.source is MessageSource.MIXED:
            return any(item.suppresses_feedback for item in self.constituents)
        return self.policy.suppresses_feedback

    @property
    def allows_anchor(self) -> bool:
        """只有单一人、单一真人原文可成为委托锚；m:id 仍由消费方另验。"""

        return self.is_pure_human

    @property
    def allows_steer(self) -> bool:
        return self.is_pure_human and self.policy.allows_steer

    @property
    def allows_store(self) -> bool:
        return self.source is not MessageSource.MIXED and self.policy.allows_store

    @property
    def id_required_none(self) -> bool:
        """当前合成与混合聚合体不持有可寻址 m:id；真人回投允许无号。"""

        return self.source is MessageSource.MIXED or self.author_kind is AuthorKind.SYSTEM

    @classmethod
    def human(cls) -> MessageOrigin:
        return cls(MessageSource.HUMAN_INGRESS)

    @classmethod
    def human_replay(cls) -> MessageOrigin:
        return cls(MessageSource.HUMAN_REPLAY)

    @classmethod
    def bot_output(cls) -> MessageOrigin:
        return cls(MessageSource.BOT_OUTPUT)

    @classmethod
    def synthetic(cls, source: MessageSource, *, label: str | None = None) -> MessageOrigin:
        if source not in SOURCE_POLICY or SOURCE_POLICY[source].author_kind is not AuthorKind.SYSTEM:
            raise ValueError("合成构造器只接受系统来源")
        return cls(source, label=label)

    @classmethod
    def from_stored(cls, *, is_bot: bool) -> MessageOrigin:
        """存储行显式水合：现库只含平台真人消息与 bot 回写，不使用缺省来源。"""

        return cls.bot_output() if is_bot else cls.human()

    @classmethod
    def merge(cls, old: MessageOrigin, new: MessageOrigin) -> MessageOrigin:
        """聚合唯一入口：同源返回原对象，异源按实际到达顺序拼接成分。

        只折叠「相邻且相同」的成分，不做跨位去重：同一来源在序列不同位置再次出现时
        时序与来路都必须原样保留（HUMAN→PATROL→HUMAN 是三段成分，不是两段）。
        成分对象本身不复制、不改写——来源事实不可变，聚合只持有引用。
        """
        # 同源按通道判定,展示名差异不制造伪混合;保留首条展示名。
        if old.source is new.source and old.source is not MessageSource.MIXED:
            return old
        parts: list[MessageOrigin] = []
        for origin in (old, new):
            incoming = origin.constituents if origin.source is MessageSource.MIXED else (origin,)
            for item in incoming:
                if parts and parts[-1] == item:
                    continue
                parts.append(item)
        if len(parts) == 1:
            return parts[0]
        return cls(MessageSource.MIXED, constituents=tuple(parts))
