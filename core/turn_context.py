"""runs 回合的上下文（``hermes_context``）契约。

本模块只定义 NB 与 Hermes 用户插件 ``background-completion`` 之间的**传输契约**，
以及组包前的最小校验；不承载任何渲染（通知区仍由 ``core.prompt_builder`` 生成）。

契约（条目 145 §5）：

    {
      "schema": "hermes.turn.context/1",
      "request_context": "（当前时间：……）…",
      "persisted_notices": "2026-11-27 08:00",
      "notices": "<系统通知区域起始>…<系统通知区域结束>",
      "closing_reminder": "\n\n请直接输出正文……"   # 仅 reactive 可带
    }

- ``spec`` 字段名固定为 ``hermes_context``，只在 runs 回合且确有上下文时发送；
- ``request_context`` 是请求级动态事实；``persisted_notices`` 是当前小时的留史通知；
- ``notices`` 是 NB 已渲染好的完整通知区，非空字符串，不是数组；
- ``closing_reminder`` 与近因 reminder 逐字一致，passive 一律不带；
- 不增加 memory／capabilities／版本协商／TTL 等字段（本期不做向量检索器）。

Hermes 插件副本 ``background-completion/turn_context.py`` 有一份逐字同源的常量
定义，两侧用测试互锁（``tests/test_turn_context_injection.py`` 与
``background-completion/tests/test_turn_context.py``），改这里必须同批改那边。

降级契约（§4）：登记成功或同幂等键重复登记时 runs 受理响应带
``X-Hermes-Turn-Context: registered``；无法登记时为 ``rejected``，且不阻断 runs。
NB 只在「发了 ``hermes_context`` 却没看到 ``registered``」时留一条脱敏 WARNING，
不重发、不回写旧格式、不把通知塞回正文。
"""

from __future__ import annotations

from typing import Any

# 请求体字段名 / schema 版本 / 受理响应头取值：两侧逐字同源。
TURN_CONTEXT_FIELD = "hermes_context"
TURN_CONTEXT_SCHEMA = "hermes.turn.context/1"
TURN_CONTEXT_HEADER = "X-Hermes-Turn-Context"
TURN_CONTEXT_REGISTERED = "registered"
TURN_CONTEXT_REJECTED = "rejected"


def build_context_payload(
    notices: str, closing_reminder: str = "", request_context: str = "",
    persisted_notices: str = "",
) -> dict[str, Any] | None:
    """通知区 + 可选批尾 reminder + 本轮动态上下文 → 请求体里的 ``hermes_context``。

    各字段都空（空串或全空白）时返回 ``None``：无上下文回合不发字段、不创建登记项。
    ``request_context`` 是本轮动态事实（时间、消息索引、媒体寻址、任务锚），随本自然轮
    （含工具续跑）注入；``notices`` 仍按自然回合提醒 N 次；``closing_reminder`` 只有
    reactive 会带，空串表示省略该键（passive 不多出空段）。``persisted_notices``
    是当前小时的留史通知，由插件经 pre_llm_call 写入 api_content，不走请求级注入。
    """
    payload: dict[str, Any] = {"schema": TURN_CONTEXT_SCHEMA}
    if isinstance(notices, str) and notices.strip():
        payload["notices"] = notices
    if isinstance(closing_reminder, str) and closing_reminder:
        payload["closing_reminder"] = closing_reminder
    if isinstance(request_context, str) and request_context.strip():
        payload["request_context"] = request_context
    if isinstance(persisted_notices, str) and persisted_notices.strip():
        payload["persisted_notices"] = persisted_notices
    if len(payload) == 1:
        return None
    return payload
