# 返修轮证据（R1/R2/R3，父卡 t_aab0f31a）

候选 `835b08a` 被 reviewer_gpt 退回三条 P2（评论 301）。本轮只做局部修复，未扩框架、未动已通过的
Runs `not_final` 链，未部署/重启/推送/合并，未做真实自戳。

## 文件

| 文件 | 内容 | 复跑命令 |
| --- | --- | --- |
| `three_small_updates_20261116_FIX_RED.txt` | 基线 `835b08a` + 仅拷入本轮两个测试文件的失败原文：`Ran 31, failures=8, errors=2` | 见下「RED」 |
| `three_small_updates_20261116_FIX_GREEN.txt` | HEAD `565359f` 的定向测试、全量 tests 末尾摘要、turn_tests、`verify.py`、reviewer 探针输出 | 见下「GREEN」 |

## RED（生产代码保持基线，只换测试）

```
git worktree add --detach /tmp/red2-835b08a 835b08a
cp tests/test_video_file_ref.py tests/test_outbound_poke.py /tmp/red2-835b08a/tests/
cd /tmp/red2-835b08a
PYTHONDONTWRITEBYTECODE=1 /home/neonsky/NeonBot/.venv/bin/python -B -m unittest \
    tests.test_video_file_ref tests.test_outbound_poke
```

失败口径：R1 块内标签错位 3 条；R2 真入口（`handle_perception` → `MessageBuffer` → 临时 SQLite）
预算/占位 4 条；R3 前导零自戳 1 条；2 条 `file_slots` 参数不存在（TypeError）。通过的是控制项：
无 url 视频普通占位、前导零他人目标、普通文件登记、@ 回归。

## GREEN（HEAD 565359f，干净树）

```
cd <repo>
PYTHONDONTWRITEBYTECODE=1 /home/neonsky/NeonBot/.venv/bin/python -B -m unittest \
    tests.test_video_file_ref tests.test_outbound_poke tests.test_runs_not_final      # 42 OK
PYTHONDONTWRITEBYTECODE=1 /home/neonsky/NeonBot/.venv/bin/python -B -m unittest discover -s tests -t .   # 786 OK (skipped=1)
PYTHONDONTWRITEBYTECODE=1 /home/neonsky/NeonBot/.venv/bin/python -B -m unittest discover -s turn_tests -p 'test_*.py'  # 6 OK
PYTHONDONTWRITEBYTECODE=1 /home/neonsky/NeonBot/.venv/bin/python -B verify.py      # ALL PASS: 22/22
```

reviewer 探针（attachment 59 `review-835b08a-probes.zip`，未改一行）在同一 HEAD 上重跑：
`FINDINGS []`；三条缺陷对应的原始打印现在是

```
Sender: [视频][视频 m:1#f0]                       # R1 逐段保位
[文件 m:1#f0:a.pdf] [视频]                        # R2 cap=1，File 先占、视频无 ref
[视频 m:1#f0] [视频]                              # R2 cap=1，两个普通视频只登记 1 行
<forwarded_messages .../> [视频 m:1#f0]          # R2 转发视频共用剩余预算
[CQ:poke,id=000999] -> 纯文本                     # R3 前导零自戳不导出
```

所有探针路径 `automatic_downloads == 0`（不自动下载、不抽帧）。
