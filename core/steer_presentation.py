"""Runs steer 呈递标记——NB 与网关薄插件之间**唯一**的结构化契约。

背景：`POST /v1/runs/{id}/steer` 的上游实现在投递时会把文本包进
`[OUT-OF-BAND USER MESSAGE — …]` 外壳（自描述标记 + 注入防御）。本部署的 steer
已经是普通的聊天消息，NB 在 `/steer` 请求体里带上本模块的标记后，网关薄插件
（`gateway_plugins/steer-presentation-thin`）对**这一批**跳过上游外壳，按 NB 呈递
的区域块原样落成 user 行。

为什么不是「按文本前缀识别上游外壳」：识别上游包裹靠文本形状猜测，任何一次上游
文案调整都会静默失效，而且模型正文里出现同样字样时会被误剥。这里传的是结构化
字段（请求体 JSON 对象），插件只认本模块声明的 schema/format，别的一切走上游原样。

契约（改动必须两侧同批，且有测试比对字面量）：

    POST /v1/runs/{run_id}/steer
    {"input": "<区域块文本>",
     "presentation": {"schema": "hermes.steer.presentation/1",
                      "format": "chat_region"}}

插件缺失/版本不认识 → 请求体多出来的字段被上游丢弃，文本照旧被外壳包住：
翻滚回退是安全的（模型仍看到区域块，只是套着可信标记），不会丢内容。
"""
from __future__ import annotations

from typing import Any

# 契约版本。任何语义变化（例如换成别的块形态）都必须同时改插件侧字面量。
PRESENTATION_SCHEMA = "hermes.steer.presentation/1"
# 呈递形态：与普通消息共用的聊天消息区域块（prompt_builder.render_chat_region）。
PRESENTATION_FORMAT = "chat_region"


def presentation_marker() -> dict[str, str]:
    """`/steer` 请求体里要带的结构化呈递标记。"""
    return {"schema": PRESENTATION_SCHEMA, "format": PRESENTATION_FORMAT}


def declares_chat_region(marker: Any) -> bool:
    """标记是否声明了「本批文本已是聊天区区域块」。

    只认本模块声明的字段值：不是对象、缺字段、版本/形态不认识，一律 False（走上游
    原样包装）。这是与网关插件同一份判据，测试用同一批用例比对两侧。
    """
    if not isinstance(marker, dict):
        return False
    return (
        marker.get("schema") == PRESENTATION_SCHEMA
        and marker.get("format") == PRESENTATION_FORMAT
    )
