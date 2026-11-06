"""(adapter, scope, scope_id) → (Bot, Target) 的内存路由表。

Target 永远不序列化:重启即清。首次 perception 事件重新填表;
重启恢复期由 Bot 连接钩子按交付台账补一份 synthetic 路由(见
``tasks.run_resume.restore_bot_routes``),真实感知 entry 一到就覆盖它。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

ScopeT = Literal["private", "group"]
"""push_message 路由表的 scope 维度。typo 会被 type checker 拦下,避免运行期静默 routing miss。"""


@dataclass
class BotEntry:
    bot_self_id: str
    target: Any  # nonebot_plugin_alconna.Target,只存内存
    last_seen_at: int  # ms;只写不读
    synthetic: bool = False  # 重启恢复补的路由;真实感知 entry 可覆盖它


class BotRegistry:
    def __init__(self) -> None:
        self._entries: dict[tuple[str, ScopeT, str], BotEntry] = {}

    def upsert(
        self,
        adapter: str,
        scope: ScopeT,
        scope_id: str,
        bot_self_id: str,
        target: Any,
        ts: int,
        synthetic: bool = False,
    ) -> None:
        self._entries[(adapter, scope, scope_id)] = BotEntry(
            bot_self_id=bot_self_id,
            target=target,
            last_seen_at=ts,
            synthetic=synthetic,
        )

    def get(self, adapter: str, scope: ScopeT, scope_id: str) -> BotEntry | None:
        return self._entries.get((adapter, scope, scope_id))

    def remove(self, adapter: str, scope: ScopeT, scope_id: str) -> None:
        self._entries.pop((adapter, scope, scope_id), None)

    def known(self, adapter: str | None = None) -> list[tuple[str, ScopeT, str]]:
        if adapter is None:
            return list(self._entries.keys())
        return [k for k in self._entries if k[0] == adapter]
