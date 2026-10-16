"""Message Context 头的单一拼装处(2026-10-16 同源化收口批,审计确认7)。

同一段 `Platform / Chat Type / User ID / Group ID` 此前在 hermes_client.chat()
的默认分支与 prompt_builder.build_passive_system_prompt 各拼一份,靠测试互锁;
两处改调本函数后,字节一致由代码同源保证而非约定。独立文件的原因:hermes_client
与 prompt_builder 有依赖方向(prompt_builder → hermes_client,取 UserContent),
互相调用会成环;本模块零依赖,两边都能安全 import。
"""

from __future__ import annotations


def render_message_context_head(
    *, adapter: str, is_private: bool, user_id: str, group_id: str | None
) -> str:
    """渲染 `Message Context:` 头(与历史拼装逐字节一致)。

    - Platform 行:adapter 为空回落 'unknown';
    - Chat Type:Private / Group;
    - User ID 仅非空时输出;Group ID 仅群聊且非空时输出。
    """
    ctx_lines = [f"Platform: {adapter or 'unknown'}"]
    ctx_lines.append("Chat Type: " + ("Private" if is_private else "Group"))
    if user_id:
        ctx_lines.append(f"User ID: {user_id}")
    if not is_private and group_id:
        ctx_lines.append(f"Group ID: {group_id}")
    return "Message Context:\n" + "\n".join(ctx_lines)
