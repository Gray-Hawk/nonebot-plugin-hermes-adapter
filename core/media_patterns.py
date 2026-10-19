"""出站媒体引用形态正则(单一来源 同源化收口批,审计确认8)。

上游内联收口 media_whitelist.inline_local_media_images 与下游提取
hermes_client.extract_response_media 必须对同一组形态匹配;任何一侧放宽/收紧
而另一侧不动,就会静默不一致(典型后果:整段 base64 当普通文本发进群)。
此前两文件各一份正则、靠注释口头对齐,现单点定义。

无依赖小模块:media_whitelist 与 hermes_client 之间有依赖方向
(hermes_client → media_whitelist),不能互引;两边都从本模块 import。
"""

from __future__ import annotations

import re

# `![alt](ref)` markdown 图片引用
_MD_IMAGE_PATTERN = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")
# `MEDIA:ref` 标签形态
_MEDIA_TAG_PATTERN = re.compile(r"MEDIA:(\S+)")
