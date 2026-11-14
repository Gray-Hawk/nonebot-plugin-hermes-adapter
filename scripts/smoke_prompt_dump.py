"""提示词请求留痕 smoke 测试。

跑法（shim 挂 work 副本，中立目录执行）：
    ln -s <work 副本路径> <shim>/nonebot_plugin_agent_adapter
    cd <中立目录> && PYTHONPATH=<shim> python <work>/scripts/smoke_prompt_dump.py

覆盖：开关关零 IO / init 回读开启 / 首开写文件 / 完整请求结构落盘（system 字段
与 messages、input 的 user content 都在）/ 同请求 SHA 跳过 / 变 SHA 覆写 /
只改 user content 也覆写（不再只看 system）/ 内联二进制占位且文本逐字节保留 /
runs 与 astrbot 视图外形 / 私群两桶分流 / 文件名消毒 / 群 id 缺失兜底 /
关回后零留痕。
"""

import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_agent_adapter import config as cfg  # noqa: E402
from nonebot_plugin_agent_adapter.core import prompt_dump as pd  # noqa: E402
from nonebot_plugin_agent_adapter.core import misc_config  # noqa: E402

FAILS = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("PASS " if cond else "FAIL ") + name + (f" | {detail}" if detail else ""))
    if not cond:
        FAILS.append(name)


def fresh_dir(enabled: bool = False) -> Path:
    d = Path(tempfile.mkdtemp(prefix="pd_smoke_"))
    # 启动路径保留在 plugin_config；动态开关已迁入 misc 快照，不能再测废弃键。
    cfg.plugin_config = SimpleNamespace(hermes_custom_dir=str(d))
    misc_config._store.commit(misc_config.MiscConfig(prompt_dump_enabled=enabled))
    return d


def request(system: str = "sys v1", user: str = "user v1",
            interface: str = "completions") -> dict:
    if interface == "runs":
        return {
            "model": "hermes-agent",
            "session_id": "sess-1",
            "instructions": system,
            "input": [{"role": "user", "content": user}],
        }
    if interface == "astrbot":
        return {
            "model": "hermes-agent",
            "system_prompt": system,
            "user_content": user,
        }
    return {
        "model": "hermes-agent",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
    }


def dump(adapt: str = "onebotv11", priv: bool = True, uid: str = "10001",
         gid: str | None = None, system: str = "sys v1", user: str = "user v1",
         interface: str = "completions", note: str | None = None) -> None:
    pd.maybe_dump_request(
        adapter_name=adapt, is_private=priv, user_id=uid, group_id=gid,
        interface=interface,
        request=request(system=system, user=user, interface=interface),
        note=note,
    )


# --- 1. 开关关：零 IO（连目录都不建） ---
d = fresh_dir(enabled=False)
pd.init_prompt_dump()
dump()
check("开关关零 IO", not (d / "prompt_dumps").exists())

# --- 2. 开关开：init 回读 + 首开写文件，完整请求结构落盘 ---
d = fresh_dir(enabled=True)
pd.init_prompt_dump()
check("init 回读开启", pd._enabled is True)
dump(system="sys v1", user="[m:401] [主人] 在吗")
f = d / "prompt_dumps" / "private" / "onebotv11_10001.json"
check("首开写文件", f.is_file())
if f.is_file():
    body = json.loads(f.read_text(encoding="utf-8"))
    check("落盘是 JSON 结构", isinstance(body, dict))
    check("sha256 字段", len(str(body.get("sha256") or "")) == 64)
    check("interface 字段", body.get("interface") == "completions")
    req = body.get("request") or {}
    msgs = req.get("messages") or []
    check("messages 结构完整",
          [m.get("role") for m in msgs] == ["system", "user"], str(msgs))
    check("system 字段落盘", (msgs[0] if msgs else {}).get("content") == "sys v1")
    check("user content 逐字节保留",
          (msgs[1] if len(msgs) > 1 else {}).get("content") == "[m:401] [主人] 在吗")
    check("接口外层字段保留", req.get("stream") is False and req.get("model") == "hermes-agent")

# --- 3. 同请求 SHA 跳过（篡改中段做探针，同内容再调不覆写） ---
if f.is_file():
    probe = f.read_text(encoding="utf-8").replace('"length":', '"length": PROBE_')
    f.write_text(probe, encoding="utf-8")
    dump(system="sys v1", user="[m:401] [主人] 在吗")
    check("同 SHA 跳过覆写", "PROBE_" in f.read_text(encoding="utf-8"))

# --- 3b. 大请求同样按 SHA 跳过（sha 必须写在文件头，不能落在几十 KB 的请求体之后） ---
big_system = "系统提示词" * 4000  # >40KB，模拟真实 system
dump(system=big_system, user="[m:410] [主人] 大请求")
if f.is_file():
    body = json.loads(f.read_text(encoding="utf-8"))
    check("大请求结构完整", len((body.get("request") or {}).get("messages") or []) == 2)
    text = f.read_text(encoding="utf-8")
    check("大请求 sha 落在文件头可读区", body.get("sha256") in text[:256],
          f"len={len(text)}")
    f.write_text(text.replace('"length":', '"length": PROBE_BIG_'), encoding="utf-8")
    dump(system=big_system, user="[m:410] [主人] 大请求")
    check("大请求同 SHA 跳过覆写", "PROBE_BIG_" in f.read_text(encoding="utf-8"))
    # 复原为小请求,后续步骤按小样继续
    dump(system="sys v1", user="[m:401] [主人] 在吗")

# --- 4. 只改 user content 也要覆写（旧版只看 system，会漏掉呈递变化） ---
dump(system="sys v1", user="[m:402] [主人] 再来一句")
if f.is_file():
    body = json.loads(f.read_text(encoding="utf-8"))
    msgs = (body.get("request") or {}).get("messages") or []
    check("user content 变化触发覆写",
          (msgs[1] if len(msgs) > 1 else {}).get("content") == "[m:402] [主人] 再来一句")
    check("覆写后探针已消失", "PROBE_" not in f.read_text(encoding="utf-8"))

# --- 5. system 变化同样覆写 ---
dump(system="sys v2 changed", user="[m:402] [主人] 再来一句")
if f.is_file():
    body = json.loads(f.read_text(encoding="utf-8"))
    msgs = (body.get("request") or {}).get("messages") or []
    check("system 变化触发覆写", (msgs[0] if msgs else {}).get("content") == "sys v2 changed")

# --- 6. 内联二进制占位：结构在、文本不动 ---
big = "data:image/png;base64," + "A" * 4000
parts_user = [
    {"type": "text", "text": "（当前时间：2026-11-10 周二 08:00）\n\n[m:403] [主人] 看图"},
    {"type": "image_url", "image_url": {"url": big}},
]
pd.maybe_dump_request(
    adapter_name="onebotv11", is_private=True, user_id="10001", group_id=None,
    interface="completions",
    request={"model": "hermes-agent", "messages": [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": parts_user},
    ], "stream": False},
)
if f.is_file():
    body = json.loads(f.read_text(encoding="utf-8"))
    msgs = (body.get("request") or {}).get("messages") or [{}, {}]
    parts = msgs[1].get("content")
    parts = parts if isinstance(parts, list) else []
    parts_ok = len(parts) == 2 and isinstance(parts[0], dict)
    check("多模态 parts 结构在", parts_ok)
    if parts_ok:
        check("文本 part 逐字节保留",
              parts[0]["text"] == "（当前时间：2026-11-10 周二 08:00）\n\n[m:403] [主人] 看图")
        url = parts[1]["image_url"]["url"]
        check("data URL 换成长度占位",
              url.startswith("data:image/png;base64,") and "omitted" in url, url[:60])
    check("转储体积远小于内联体",
          len(f.read_text(encoding="utf-8")) < 4000, str(f.stat().st_size))

# --- 7. runs / astrbot 视图外形 ---
dump(interface="runs", system="run sys", user="[m:404] [主人] 跑")
if f.is_file():
    body = json.loads(f.read_text(encoding="utf-8"))
    req = body.get("request") or {}
    check("runs 视图:instructions 即 system", req.get("instructions") == "run sys")
    check("runs 视图:input user content 落盘",
          (req.get("input") or [{}])[0].get("content") == "[m:404] [主人] 跑")
    check("runs interface 标记", body.get("interface") == "runs")
dump(interface="astrbot", system="ab sys", user="[m:405] [主人] ab",
     note="astrbot 后端:图片另经附件上传,本转储为本地侧请求视图")
if f.is_file():
    body = json.loads(f.read_text(encoding="utf-8"))
    req = body.get("request") or {}
    check("astrbot 视图:system_prompt 字段", req.get("system_prompt") == "ab sys")
    check("astrbot 视图:user_content 字段", req.get("user_content") == "[m:405] [主人] ab")
    check("astrbot 视图:附注说明是本地侧视图", "本地侧请求视图" in str(body.get("note")))

# --- 8. 群桶分流 ---
dump(priv=False, uid="10001", gid="20002", system="group sys")
g = d / "prompt_dumps" / "group" / "onebotv11_20002.json"
check("群聊落 group 桶", g.is_file())
check("私聊桶不被群写",
      (d / "prompt_dumps" / "private" / "onebotv11_20002.json").is_file() is False)

# --- 9. 文件名消毒（斜杠/点全消毒成下划线，不穿出桶目录） ---
dump(uid="../../etc/passwd", system="evil")
files = list((d / "prompt_dumps" / "private").glob("*.json"))
check("消毒不穿出目录", all(p.parent.name == "private" for p in files), str([p.name for p in files]))
expect_name = "onebotv11_" + "_" * 6 + "etc_passwd.json"
check("消毒形态", (d / "prompt_dumps" / "private" / expect_name).is_file(), expect_name)
dump(priv=False, gid=None, system="gid none")
check("群 id 缺失兜底 unknown",
      (d / "prompt_dumps" / "group" / "onebotv11_unknown.json").is_file())

# --- 10. 关回后零留痕（重新 init 为关，新内容不覆写旧文件） ---
before = json.loads(f.read_text(encoding="utf-8")) if f.is_file() else {}
misc_config._store.commit(misc_config.MiscConfig(prompt_dump_enabled=False))
pd.init_prompt_dump()
check("init 回读关闭", pd._enabled is False)
dump(system="sys v3 should not land", user="[m:999] [主人] 不该落盘")
if f.is_file():
    check("关回后零留痕",
          json.loads(f.read_text(encoding="utf-8")).get("sha256") == before.get("sha256"))

print()
if FAILS:
    print(f"FAILED {len(FAILS)}: {FAILS}")
    sys.exit(1)
print("ALL PASS")
