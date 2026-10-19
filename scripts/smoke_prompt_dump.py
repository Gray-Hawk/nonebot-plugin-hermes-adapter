"""system_prompt 留痕 smoke 测试。

跑法（shim 挂 work 副本，中立目录执行）：
    ln -s <work 副本路径> <shim>/nonebot_plugin_hermes
    cd 技能 scripts/ && PYTHONPATH=<shim> ~/NeonBot/.venv/bin/python smoke_prompt_dump.py

覆盖：开关关零 IO / init 回读开启 / 首开写文件 / 同 SHA 跳过 / 变 SHA 覆写 /
私群两桶分流 / 文件名消毒 / 群 id 缺失兜底 / 关回后零留痕。
"""

import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_hermes import config as cfg  # noqa: E402
from nonebot_plugin_hermes.core import prompt_dump as pd  # noqa: E402

FAILS = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("PASS " if cond else "FAIL ") + name + (f" | {detail}" if detail else ""))
    if not cond:
        FAILS.append(name)


def fresh_dir(enabled: bool = False) -> Path:
    d = Path(tempfile.mkdtemp(prefix="pd_smoke_"))
    # plugin_config 是 from-import 绑定：定义方与消费方两边都要替换才生效
    cfg.plugin_config = SimpleNamespace(
        hermes_custom_dir=str(d), hermes_prompt_dump_enabled=enabled
    )
    pd.plugin_config = cfg.plugin_config
    return d


def dump(adapt: str = "onebotv11", priv: bool = True, uid: str = "10001",
         gid: str | None = None, text: str = "sys v1") -> None:
    pd.maybe_dump_system_prompt(
        adapter_name=adapt, is_private=priv, user_id=uid,
        group_id=gid, system_prompt=text,
    )


# --- 1. 开关关：零 IO（连目录都不建） ---
d = fresh_dir(enabled=False)
pd.init_prompt_dump()
dump()
check("开关关零 IO", not (d / "prompt_dumps").exists())

# --- 2. 开关开：init 回读 + 首开写文件，首行含 SHA ---
d = fresh_dir(enabled=True)
pd.init_prompt_dump()
check("init 回读开启", pd._enabled is True)
dump(text="sys v1")
f = d / "prompt_dumps" / "private" / "onebotv11_10001.txt"
check("首开写文件", f.is_file())
if f.is_file():
    body = f.read_text(encoding="utf-8")
    first = body.splitlines()[0]
    check("首行 SHA 标记", first.startswith("# sha256: "), first[:30])
    check("全文落盘", body.endswith("sys v1"))
    check("头部长度字段", "# length: 6" in body)

# --- 3. 同 SHA 跳过（篡改中段内容做探针，同内容再调不覆写） ---
if f.is_file():
    probe = f.read_text(encoding="utf-8").replace("# length: 6", "# length: PROBE")
    f.write_text(probe, encoding="utf-8")
    dump(text="sys v1")
    check("同 SHA 跳过覆写", "PROBE" in f.read_text(encoding="utf-8"))

# --- 4. 变 SHA 覆写 ---
dump(text="sys v2 changed")
if f.is_file():
    body = f.read_text(encoding="utf-8")
    check("变 SHA 覆写", body.endswith("sys v2 changed") and "PROBE" not in body)

# --- 5. 群桶分流 ---
dump(priv=False, uid="10001", gid="20002", text="group sys")
g = d / "prompt_dumps" / "group" / "onebotv11_20002.txt"
check("群聊落 group 桶", g.is_file())
check("私聊桶不被群写", (d / "prompt_dumps" / "private" / "onebotv11_20002.txt").is_file() is False)

# --- 6. 文件名消毒（斜杠/点全消毒成下划线，不穿出桶目录） ---
dump(uid="../../etc/passwd", text="evil")
files = list((d / "prompt_dumps" / "private").glob("*.txt"))
check("消毒不穿出目录", all(p.parent.name == "private" for p in files), str([p.name for p in files]))
expect_name = "onebotv11_" + "_" * 6 + "etc_passwd.txt"
check("消毒形态", (d / "prompt_dumps" / "private" / expect_name).is_file(), expect_name)
dump(priv=False, gid=None, text="gid none")
check("群 id 缺失兜底 unknown", (d / "prompt_dumps" / "group" / "onebotv11_unknown.txt").is_file())

# --- 7. 关回后零留痕（重新 init 为关，新内容不覆写旧文件） ---
cfg.plugin_config = SimpleNamespace(hermes_custom_dir=str(d), hermes_prompt_dump_enabled=False)
pd.plugin_config = cfg.plugin_config
pd.init_prompt_dump()
check("init 回读关闭", pd._enabled is False)
dump(text="sys v3 should not land")
if f.is_file():
    check("关回后零留痕", f.read_text(encoding="utf-8").endswith("sys v2 changed"))

print()
if FAILS:
    print(f"FAILED {len(FAILS)}: {FAILS}")
    sys.exit(1)
print("ALL PASS")
