# t_b611baca 交付：段内重复吞掉 → 群聊触发消息表情观察

基于 `feat/plain-text-protocol` HEAD `7138d0b`（确认 `a27ca76` 为祖先）的增量补丁，
不重开纯文本协议架构、不新增配置/数据库/持久状态/账号群号硬编码，仅本地提交与测试。
**未部署、未重启、未上传。**

## 行为

Runs 同一 assistant 段的 interim／delta 完整单元全文匹配被吞（`segment_duplicate`）时，
给「本轮应该回复的用户触发消息」贴一次表情（`set_msg_emoji_like`, emoji_id=32）。

- 只接 onebotv11 群聊 reactive 且有 `trigger_msg_id` 的路径；私聊（passive）路径不接线、
  不调用——NapCat 私聊外层 `status=ok/retcode=0` 但 `data.result=65011`，是能力边界。
- 不贴 bot 自己的消息、不另发表情消息；`unsealed_delta_tail`、跨段同文、终态重复都不贴。
- 每轮最多贴一次（首次命中即置位，失败也不重试）；后台任务吞异常，失败不影响正文。

## 改动

- `core/hermes_client.py`：`chat()`／`_chat_via_runs()` 新增可选 `on_suppressed`，
  透传给 `RunsEventParser.on_suppressed`（默认 None，未接线零开销）；新增
  `SuppressedCallback` 类型别名。
- `handlers/message_feedback.py`：`_emoji_like_quiet` 返回 `bool`，检查返回的
  `data.result`／`errMsg`：内部非 0 或 errMsg 非空记 warning 并返回 False，缺失 result
  的旧适配器按成功（保留 429「丢了就贴」原调用兼容，调用方忽略返回值）。
- `handlers/message_turns.py`：新增 `_segment_duplicate_emoji_hook(...)`（不适用返回 None）
  ＋ `_SEGMENT_DUPLICATE_EMOJI_ID = 32`；`_run_reactive_turn` 唯一接线点把 hook 传给
  `_chat_with_overflow_recovery(on_suppressed=…)` → `chat()`。后台任务走仓库统一出口
  `core.bg_tasks.spawn_bg_task`（内部即 `asyncio.create_task`，另持引用防 GC 并把异常
  打进 ERROR 日志），卡面建议的裸 create_task 换成它不是新观测系统，是既有惯例。
- `tests/test_segment_duplicate_emoji.py`（新增 18 项）：多个重复只贴 1 次、终态尾丢弃
  不贴、跨段同文不贴、私聊/passive 不接线、内部 result 非 0 判失败且不重试、
  发平台异常不外泄、429 原路径仍用 `rate_limit_emoji_id`、真实 `chat()→_chat_via_runs`
  →parser 链按帧语义触发。
- `tests/test_runs_terminal.py`：共享 harness `_run` 增加可选 `trigger_msg_id` 透传
  （不传时行为不变）。
- `tests/test_runs_stream.py`：仅更新一行注释（parser 默认 None，生产由群聊 reactive 按需接线）。

## 真实测试输出

解释器：`/home/neonsky/NeonBot/.venv/bin/python`（nonebot 2.5.0 / Python 3.13.5），
全程 `PYTHONDONTWRITEBYTECODE=1` + `-B`。

```
$ python -B -m unittest discover -s tests -t . -q
Ran 474 tests in 32.420s
OK

$ python -B -m unittest discover -s turn_tests -p 'test_*.py' -q
Ran 6 tests in 0.054s
OK

$ python -B -m unittest tests.test_segment_duplicate_emoji
Ran 18 tests in 0.080s
OK
```

`verify.py` 输出见提交后的运行记录（干净工作树、清缓存后复跑）。

## 未做

- 未部署到任何实例；生产 site-packages 仍是旧版，实测表情行为需部署后在受控群观察。
- 未改私聊行为；私聊 `set_msg_emoji_like` 的 65011 只作为能力边界记录在注释里。
