"""adapter 名归一化单一来源(本部署补丁)。

core/routing.py 与 utils.get_adapter_name 共用同一个归一化实现——此前两处
各写一份孪生实现，单侧漂移会让路由键永远匹配不上。单独成模块是因为
utils 依赖 alconna，core 层 import 它会把 alconna 拉进 core。
"""


def normalize_adapter_name(raw: str) -> str:
    """归一化 adapter 名：小写、去空格/点号，空串归一为 "unknown"。"""
    return (raw or "").lower().replace(" ", "").replace(".", "") or "unknown"
