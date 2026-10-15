"""群档案（群记忆外挂）smoke 测试 — 定稿五.4 清单。

跑法（不经过 nb 运行时，直接导入插件包）：
    cd work 副本
    ~/NeonBot/.venv/bin/python scripts/smoke_group_profile.py

覆盖：无目录跳过 / 无群友行只挂群档案 / 群友行容错 / 缺文件不炸 / 数值升序 /
字节稳定 + mtime 缓存 / 超限 warning 不截断 / 私聊跳过 / 路径白名单 /
reactive、passive 两个构造函数的尾巴拼接。
"""

import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_hermes import config as cfg  # noqa: E402
from nonebot_plugin_hermes.core import group_profile as gp  # noqa: E402
from nonebot_plugin_hermes.core import prompt_builder as pb  # noqa: E402

FAILS = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("PASS " if cond else "FAIL ") + name + (f" | {detail}" if detail else ""))
    if not cond:
        FAILS.append(name)


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def fresh_dir() -> Path:
    d = Path(tempfile.mkdtemp(prefix="gp_smoke_"))
    gp._cache.clear()
    # group_profile 用 from-import 绑定 plugin_config 对象,替换必须两边都打
    # (cfg 模块命名空间 + gp 模块命名空间),否则开关替换不生效。
    cfg.plugin_config = SimpleNamespace(hermes_custom_dir=str(d), hermes_group_profile_enabled=True)
    gp.plugin_config = cfg.plugin_config
    return d


# --- 1. 记忆开关关闭：静默 None；空配置回落 localstore；prompt 构造函数不变 ---
gp._cache.clear()
cfg.plugin_config = SimpleNamespace(hermes_custom_dir="", hermes_group_profile_enabled=False)
gp.plugin_config = cfg.plugin_config
check("记忆开关关闭返回 None", gp.build_group_profile_block("onebotv11", "2000000001") is None)
cfg.plugin_config = SimpleNamespace(hermes_custom_dir="", hermes_group_profile_enabled=True)
gp.plugin_config = cfg.plugin_config
p = cfg.resolve_custom_dir()
check("空配置回落 localstore 数据目录", p is not None and "nonebot_plugin_hermes" in str(p), str(p))
base_reactive = pb.build_reactive_system_prompt()
check("reactive 无块时输出不变", pb.build_reactive_system_prompt(profile_block=None) == base_reactive)
base_passive = pb.build_passive_system_prompt(adapter="onebotv11", is_private=False, user_id="1", group_id="2000000001")
check(
    "passive 无块时输出不变",
    pb.build_passive_system_prompt(
        adapter="onebotv11", is_private=False, user_id="1", group_id="2000000001", profile_block=None
    ) == base_passive,
)

# --- 2. 无群档案文件：自动创建基础模板(2026-10-15 补丁),返回模板块 ---
d = fresh_dir()
block = gp.build_group_profile_block("onebotv11", "2000000001")
check("无群档案自动创建模板", block is not None and "2000000001" in block, repr(block[:80]))
check("模板文件已落盘", (d / "memory/onebotv11/groups/2000000001.md").is_file())

# --- 3. 有群档案、无群友行：只挂群档案本体 ---
d = fresh_dir()
write(d / "memory/onebotv11/groups/2000000001.md", "示例群。群规：别吵架。\n")
block = gp.build_group_profile_block("onebotv11", "2000000001")
check("只挂群档案", block == "<group_profile>\n# 群档案\n示例群。群规：别吵架。\n</group_profile>", repr(block))

# --- 4. 群友行容错 + 排序 + 缺文件跳过 ---
d = fresh_dir()
write(
    d / "memory/onebotv11/groups/2000000001.md",
    "示例群。\n群友： 1000000005，1000000004  abc  1000000005\n群友: 999999999\n",
)
write(d / "memory/onebotv11/people/1000000004.md", "测试友：粉毛狐耳。\n")
# 1000000005 的 people 文件故意不写 → 跳过不炸；999999999 在第二行 → 不认
block = gp.build_group_profile_block("onebotv11", "2000000001")
expect = (
    "<group_profile>\n# 群档案\n示例群。\n群友： 1000000005，1000000004  abc  1000000005\n群友: 999999999"
    "\n\n# 群友画像\n\n## 1000000004\n测试友：粉毛狐耳。\n</group_profile>"
)
check("容错+排序+缺文件跳过+只认首行", block == expect, repr(block))

# --- 5. 字节稳定 + mtime 缓存命中 ---
calls = {"n": 0}
orig_build = gp._build_block
def counting_build(*a, **kw):
    calls["n"] += 1
    return orig_build(*a, **kw)
gp._build_block = counting_build
gp._cache.clear()  # 上一个用例已填过同 key 缓存,清掉才能观察重建计数
b1 = gp.build_group_profile_block("onebotv11", "2000000001")
b2 = gp.build_group_profile_block("onebotv11", "2000000001")
gp._build_block = orig_build
check("连调两次字节相同", b1 == b2)
check("第二次命中缓存未重建", calls["n"] == 1, f"builds={calls['n']}")

# --- 6. mtime 失效后重建 ---
import time
p = d / "memory/onebotv11/groups/2000000001.md"
write(p, "示例群v2。\n")
t = time.time() + 5
os.utime(p, (t, t))
block2 = gp.build_group_profile_block("onebotv11", "2000000001")
check("档案改动后重建", block2 is not None and "示例群v2" in block2 and "群友画像" not in block2, repr(block2))

# --- 7. 超限不截断 ---
d = fresh_dir()
write(d / "memory/onebotv11/groups/2000000001.md", "长" * 5000)
block3 = gp.build_group_profile_block("onebotv11", "2000000001")
check("超 4000 不截断", block3 is not None and len(block3) > 4000, f"len={len(block3) if block3 else 0}")

# --- 8. 私聊 / 白名单 ---
check("私聊 group_id=None → None", gp.build_group_profile_block("onebotv11", None) is None)
check("群号带路径穿越 → None", gp.build_group_profile_block("onebotv11", "../etc") is None)
check("适配器带斜杠 → None", gp.build_group_profile_block("a/b", "2000000001") is None)

# --- 9. 构造函数尾巴拼接 ---
blk = "<group_profile>\n# 群档案\nX\n</group_profile>"
check("reactive 尾巴拼接", pb.build_reactive_system_prompt(profile_block=blk) == base_reactive + "\n\n" + blk)
check(
    "passive 尾巴拼接",
    pb.build_passive_system_prompt(
        adapter="onebotv11", is_private=False, user_id="1", group_id="2000000001", profile_block=blk
    ) == base_passive + "\n\n" + blk,
)

# --- 10. 会话上下文注入(2026-10-15 补丁) ---
# reactive: 传 adapter/self_id 时注入 session_context
ctx_reactive = pb.build_reactive_system_prompt(
    adapter="onebotv11", group_id="2000000001", self_id="1000000001", self_nickname="Normi"
)
check(
    "reactive 会话上下文注入",
    "<session_context>" in ctx_reactive
    and "adapter: onebotv11" in ctx_reactive
    and "group_id: 2000000001" in ctx_reactive
    and "you: [user=Normi id=1000000001]" in ctx_reactive,
    repr(ctx_reactive[:500]),
)
# reactive: 私聊省略 group_id 行
ctx_private = pb.build_reactive_system_prompt(
    adapter="onebotv11", group_id=None, self_id="1000000001", self_nickname="Normi"
)
check(
    "reactive 私聊省略 group_id",
    "<session_context>" in ctx_private
    and "adapter: onebotv11" in ctx_private
    and "group_id:" not in ctx_private
    and "you: [user=Normi id=1000000001]" in ctx_private,
    repr(ctx_private[:500]),
)
# reactive: 不传 adapter/self_id 时不注入
check("reactive 无会话上下文参数时不注入", pb.build_reactive_system_prompt() == base_reactive)

print()
if FAILS:
    print(f"共 {len(FAILS)} 项失败: {FAILS}")
    sys.exit(1)
print("全部通过")
