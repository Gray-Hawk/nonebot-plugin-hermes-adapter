"""真实 Runs HTTP/SSE 消费链上的连发节奏；平台与 HTTP 边界离线替代。"""
from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401
from tests.test_runs_stream import _ClientChain, sse_lines, unit
from tests.test_runs_stream import real_chat
from tests import test_runs_terminal as e2
from nonebot_plugin_hermes_adapter.config import plugin_config
from nonebot_plugin_hermes_adapter.core import hermes_client as hc
from nonebot_plugin_hermes_adapter import mcp
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from pathlib import Path
import tempfile
import time
from dataclasses import replace
from nonebot_plugin_hermes_adapter.core.inflight import InflightRegistry, make_inflight_key
from nonebot_plugin_hermes_adapter.core.run_tracker import RunTracker
from nonebot_plugin_hermes_adapter.core.message_origin import MessageOrigin
from nonebot_plugin_hermes_adapter.core.message_buffer import BufferedMessage, MessageBuffer
from nonebot_plugin_hermes_adapter.core.storage.message_store import MessageStore
from nonebot_plugin_hermes_adapter.handlers import message_turns as mt, message_relay as relay


class RunsReplyPacingTests(unittest.IsolatedAsyncioTestCase):
    async def chat(self, events, deliver, after_frame=None, tracker=None, private=False):
        fake = _ClientChain('paced-run', sse_lines(events))
        async def lines(response):
            for index, line in enumerate(response._lines):
                yield line
                if after_frame is not None:
                    await after_frame(index)
        with patch.object(plugin_config, 'hermes_api_interface', 'runs'), \
                patch.object(hc, '_get_http_client', return_value=fake), \
                patch.object(_ClientChain._Resp, 'aiter_lines', lines), \
                patch.object(mcp, 'run_tracker', tracker):
            return await hc.hermes_client.chat(
                text='hi', session_key='s', user_id='u1', group_id=None if private else 'g1',
                adapter_name='onebotv11', is_private=private, mode='passive' if private else 'reactive',
                expect_structured=True, structured_tool_name='submit_decision',
                system_prompt='SYS', decision_callback=deliver,
                bot_self_id='999' if tracker is not None else None,
                target=SimpleNamespace(base_url='http://test', api_key='', timeout=5, label='test'),
            )

    async def test_three_consecutive_units_first_immediate_then_three_seconds(self):
        now = [0.0]
        sleeps, sends, busy, drained = [], [], [], []
        async def sleep(delay):
            sleeps.append(delay)
            now[0] += delay
            await asyncio.sleep(0)
        async def deliver(decision, run_id):
            if decision['body']:
                sends.append((decision['body'], now[0]))
        async def on_drained():
            drained.append(len(sends))
        deliver.set_busy = busy.append
        deliver.on_drained = on_drained
        output = '\n'.join(unit({}, x) for x in ['甲', '乙', '丙'])
        with patch.object(hc, '_runs_reply_sleep', sleep, create=True), \
                patch.object(hc, '_runs_reply_clock', lambda: now[0], create=True):
            await self.chat([
                {'event': 'message.delta', 'delta': output, 'seq': 0},
                {'event': 'run.completed', 'output': output, 'seq': 1},
            ], deliver)
        self.assertEqual(sends, [('甲', 0.0), ('乙', 3.0), ('丙', 6.0)])
        self.assertEqual(sleeps, [3.0, 3.0])
        self.assertEqual(busy[0], True)
        self.assertEqual(busy[-1], False)
        self.assertEqual(drained, [3])

    async def test_streaming_batch_does_not_release_between_delta_frames(self):
        busy = [False]
        sends, releases = [], []
        async def deliver(decision, run_id):
            sends.append(decision['body'])
        async def drained():
            releases.append(list(sends))
        deliver.set_busy = lambda value: busy.__setitem__(0, value)
        deliver.on_drained = drained
        async def after_frame(index):
            if index == 0:
                self.assertEqual(sends, ['甲'])
                self.assertTrue(busy[0])
                self.assertEqual(releases, [])
        async def sleep(delay):
            await asyncio.sleep(0)
        with patch.object(hc, '_runs_reply_sleep', sleep, create=True):
            await self.chat([
                {'event': 'message.delta', 'delta': unit({}, '甲'), 'seq': 0},
                {'event': 'message.delta', 'delta': '\n' + unit({}, '乙'), 'seq': 1},
                {'event': 'tool.started', 'tool': 'lookup', 'seq': 2},
                {'event': 'run.completed', 'output': unit({}, '乙'), 'seq': 3},
            ], deliver, after_frame)
        self.assertFalse(busy[0])
        self.assertEqual(releases, [['甲', '乙']])

    async def test_empty_single_tool_separated_and_terminal_only_paths(self):
        for bodies, terminal_only in [([], True), ([''], True), (['甲'], True), (['甲', '', '乙', '丙'], True), (['甲', '', '乙', '丙'], False)]:
            with self.subTest(bodies=bodies, terminal_only=terminal_only):
                clock = [0.0]
                sends, sleeps = [], []
                async def sleep(delay):
                    sleeps.append(delay)
                    clock[0] += delay
                    await asyncio.sleep(0)
                async def deliver(decision, run_id):
                    if decision['body']:
                        sends.append((decision['body'], clock[0]))
                output = '\n'.join(unit({}, body) for body in bodies)
                events = [] if terminal_only else [{'event': 'message.delta', 'delta': output, 'seq': 0}]
                events.append({'event': 'run.completed', 'output': output, 'seq': 1})
                with patch.object(hc, '_runs_reply_sleep', sleep, create=True), patch.object(hc, '_runs_reply_clock', lambda: clock[0], create=True):
                    await self.chat(events, deliver)
                nonempty = [body for body in bodies if body]
                self.assertEqual(sends, [(body, index * 3.0) for index, body in enumerate(nonempty)])
                self.assertEqual(sleeps, [3.0] * max(0, len(nonempty)-1))
        sends = []
        async def deliver(decision, run_id):
            sends.append(decision['body'])
        with patch.object(hc, '_runs_reply_sleep', AsyncMock(), create=True) as sleep:
            await self.chat([
                {'event': 'message.delta', 'delta': unit({}, '进度'), 'seq': 0},
                {'event': 'tool.started', 'tool': 'lookup', 'seq': 1},
                {'event': 'tool.completed', 'tool': 'lookup', 'seq': 2},
                {'event': 'message.delta', 'delta': '\n' + unit({}, '结果'), 'seq': 3},
                {'event': 'run.completed', 'output': unit({}, '结果'), 'seq': 4},
            ], deliver)
        self.assertEqual(sends, ['进度', '结果'])
        sleep.assert_not_awaited()

    async def test_failed_send_releases_and_model_can_append_during_delay(self):
        busy, calls, releases = [], [], []
        blocked, resume = asyncio.Event(), asyncio.Event()
        async def sleep(delay):
            blocked.set()
            await resume.wait()
        async def deliver(decision, run_id):
            calls.append(decision['body'])
            if decision['body'] == '乙':
                raise RuntimeError('offline send failure')
        async def drained():
            releases.append(list(calls))
        deliver.set_busy, deliver.on_drained = busy.append, drained
        async def after_frame(index):
            if index == 1:
                await blocked.wait()
                self.assertEqual(calls, ['甲'])
            if index == 2:
                # 第三份已被 reader 接收，但发送器仍在第二份前等待。
                self.assertEqual(calls, ['甲'])
                resume.set()
        events = [
            {'event': 'message.delta', 'delta': unit({}, '甲'), 'seq': 0},
            {'event': 'message.delta', 'delta': '\n' + unit({}, '乙'), 'seq': 1},
            {'event': 'message.delta', 'delta': '\n' + unit({}, '丙'), 'seq': 2},
            {'event': 'run.completed', 'output': unit({}, '丙'), 'seq': 3},
        ]
        with patch.object(hc, '_runs_reply_sleep', sleep, create=True):
            try:
                result = await asyncio.wait_for(self.chat(events, deliver, after_frame), 2)
            except TimeoutError:
                self.fail('发送延迟阻塞了 SSE 继续产出，连发批未能排空。')
        self.assertEqual(calls, ['甲', '乙', '丙'])
        self.assertEqual(releases, [['甲', '乙', '丙']])
        self.assertFalse(busy[-1])
        self.assertTrue(result.streamed_decisions[1]['_live_delivery_failed'])

    async def test_real_clock_three_units(self):
        timestamps = []
        start = time.monotonic()
        async def deliver(decision, run_id):
            timestamps.append((decision['body'], time.monotonic() - start))
        output = '\n'.join(unit({}, body) for body in ['甲', '乙', '丙'])
        await self.chat([
            {'event': 'message.delta', 'delta': output, 'seq': 0},
            {'event': 'run.completed', 'output': output, 'seq': 1},
        ], deliver)
        self.assertEqual([body for body, _ in timestamps], ['甲', '乙', '丙'])
        self.assertLess(timestamps[0][1], 0.5)
        for left, right in zip(timestamps, timestamps[1:]):
            self.assertGreaterEqual(right[1] - left[1], 2.95)
            self.assertLess(right[1] - left[1], 3.8)
        print('REAL_CLOCK_SEND_SECONDS', timestamps)

    async def test_cancelled_queue_releases_gate(self):
        busy = []
        started = asyncio.Event()
        async def deliver(decision, run_id):
            started.set()
            await asyncio.Event().wait()
        deliver.set_busy = busy.append
        task = asyncio.create_task(self.chat([{'event': 'message.delta', 'delta': unit({}, '甲'), 'seq': 0}], deliver))
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(busy)
        self.assertFalse(busy[-1])

    async def test_sse_cancel_terminal_stops_unsent_batch_and_releases_gate(self):
        for terminal in ('run.cancelled', 'run.interrupted'):
            with self.subTest(terminal=terminal):
                sends, busy = [], []
                async def sleep(delay):
                    await asyncio.Event().wait()
                async def deliver(decision, run_id):
                    sends.append(decision['body'])
                deliver.set_busy = busy.append
                output = '\n'.join(unit({}, body) for body in ['甲', '乙', '丙'])
                with patch.object(hc, '_runs_reply_sleep', sleep, create=True):
                    try:
                        result = await asyncio.wait_for(self.chat([
                            {'event': 'message.delta', 'delta': output, 'seq': 0},
                            {'event': terminal, 'seq': 1},
                        ], deliver), 0.3)
                    except TimeoutError:
                        self.fail('上游取消后仍在等待发送未发出的连发正文。')
                self.assertEqual(sends, ['甲'])
                self.assertFalse(busy[-1])
                self.assertTrue(result.is_interrupted)

    async def test_pending_batch_blocks_direct_steer_then_releases_queued_private_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            tracker = RunTracker(Path(tmp) / 'runs.db')
            store = MessageStore(Path(tmp) / 'messages.db')
            self.addCleanup(tracker.close)
            self.addCleanup(store.close)
            inflight = InflightRegistry()
            key = make_inflight_key('onebotv11', True, 'u1', None)
            row = BufferedMessage(ts=10, adapter='onebotv11', group_id=None,
                                  user_id='u1', nickname='用户', content='新的补充', origin=MessageOrigin.human())
            inflight.try_enter(key, row, is_direct=True, original_msg_id=None, now_ms=1)
            tracker.upsert(run_id='r1', scope_key='onebotv11:private:u1', adapter='onebotv11',
                           is_private=True, user_id='u1', group_id=None, bot_self_id='999',
                           session_key='s', mode='passive', expect_structured=True)
            runtime = SimpleNamespace(run_tracker=tracker, inflight=inflight,
                                      message_buffer=MessageBuffer(store=store, fetcher=MagicMock()),
                                      active_sessions=MagicMock())
            with patch.object(mt, '_mcp', runtime), patch.object(relay, '_mcp', runtime), \
                    patch.object(relay.hermes_client, 'steer_run', AsyncMock(return_value=True)) as steer, \
                    patch.object(relay, 'resolve_target', return_value=object()):
                callback, state = mt._make_runs_live_decisions(
                    bot=MagicMock(self_id=999), target=SimpleNamespace(private=True),
                    adapter_name='onebotv11', user_id='u1', group_id=None, is_private=True,
                    fb_key=key, nickname='bot',
                )
                getattr(callback, 'set_busy', lambda busy: None)(True)
                runtime.message_buffer.append(row)
                accepted = await relay._maybe_steer_direct(
                    target=SimpleNamespace(private=True), adapter_name='onebotv11', user_id='u1',
                    nickname='用户', group_id=None, msg_text=row.content, is_direct=True,
                    now_ms=10, origin=row.origin, rows=[row],
                )
                self.assertFalse(accepted)
                steer.assert_not_awaited()
                self.assertIsNotNone(row.id)
                self.assertEqual(store.get_recent(adapter='onebotv11', group_id=None, limit=10, owner_user_id='u1')[0].content, row.content)
                self.assertEqual(inflight.try_enter(key, row, is_direct=True, original_msg_id=None,
                                                   now_ms=10, commission=True, fragments=[row]), 'queued')
                getattr(callback, 'set_busy', lambda busy: None)(False)
                await getattr(callback, 'on_drained', AsyncMock())()
                self.assertEqual(steer.await_count, 1)
                self.assertIn(row.content, steer.call_args.kwargs['text'])
                self.assertIsNone(inflight.peek_next_pending(key, after_ts=1))
                # 真实 HTTP 拒绝原语：认领撤销，原条目仍留槽，后续接力不会丢内容。
                later = replace(row, id=None, ts=20, content='拒绝后保留')
                runtime.message_buffer.append(later)
                inflight.try_enter(key, later, is_direct=True, original_msg_id=None,
                                   now_ms=20, commission=True, fragments=[later])
                steer.return_value = False
                await getattr(callback, 'on_drained', AsyncMock())()
                self.assertEqual(steer.await_count, 2)
                self.assertEqual(inflight.peek_next_pending(key, after_ts=1).fragments[0].content, '拒绝后保留')

    async def test_arrival_during_delay_uses_real_store_queue_and_midrun_release(self):
        for private, fail_last in [(True, False), (False, False), (True, True)]:
            with self.subTest(private=private, fail_last=fail_last), tempfile.TemporaryDirectory() as tmp:
                tracker = RunTracker(Path(tmp) / 'runs.db')
                store = MessageStore(Path(tmp) / 'messages.db')
                self.addCleanup(tracker.close)
                self.addCleanup(store.close)
                inflight = InflightRegistry()
                group = None if private else 'g1'
                key = make_inflight_key('onebotv11', private, 'u1', group)
                trigger = BufferedMessage(ts=1, adapter='onebotv11', group_id=group,
                                          user_id='u1', nickname='用户', content='触发', origin=MessageOrigin.human())
                inflight.try_enter(key, trigger, is_direct=True, original_msg_id=None, now_ms=1)
                runtime = SimpleNamespace(run_tracker=tracker, inflight=inflight,
                                          message_buffer=MessageBuffer(store=store, fetcher=MagicMock()),
                                          active_sessions=MagicMock())
                sends, steers, clock = [], [], [0.0]
                released = asyncio.Event()
                incoming = BufferedMessage(ts=10, adapter='onebotv11', group_id=group,
                                           user_id='u1', nickname='用户', content='延迟期间的新消息', origin=MessageOrigin.human())
                async def steer(**kwargs):
                    self.assertEqual([text for text, _ in sends], ['甲', '乙', '丙'])
                    self.assertFalse(inflight.reply_batch_busy(key))
                    self.assertIsNotNone(tracker.get_by_run_id('paced-run'))
                    steers.append(kwargs['text'])
                    released.set()
                    return True
                async def send(**kwargs):
                    sends.append((kwargs['text'], clock[0]))
                    if fail_last and kwargs['text'] == '丙':
                        return None
                    ctx = kwargs['delivery']
                    tracker.mark_component_sent(run_id=ctx.run_id, component_id=ctx.component_id, msg_ids=['ack'])
                    return SimpleNamespace(ok=True, msg_ids=['ack'], primary_qq_message_id='ack')
                async def sleep(delay):
                    if len(sends) == 1:
                        self.assertTrue(inflight.reply_batch_busy(key))
                        runtime.message_buffer.append(incoming)
                        accepted = await relay._maybe_steer_direct(
                            target=SimpleNamespace(private=private), adapter_name='onebotv11',
                            user_id='u1', nickname='用户', group_id=group, msg_text=incoming.content,
                            is_direct=True, now_ms=10, origin=incoming.origin, rows=[incoming],
                        )
                        self.assertFalse(accepted)
                        self.assertEqual(steers, [])
                        if private:
                            await relay._handle_passive_path(
                                bot=MagicMock(self_id=999), target=SimpleNamespace(private=True),
                                adapter_name='onebotv11', user_id='u1', group_id=None,
                                text=incoming.content, image_urls=[], is_private=True,
                                now_ms=10, origin=incoming.origin, rows=[incoming],
                            )
                        else:
                            await relay._handle_reactive_path(
                                bot=MagicMock(self_id=999), target=SimpleNamespace(private=False),
                                adapter_name='onebotv11', user_id='u1', group_id='g1',
                                text=incoming.content, image_urls=[], is_direct=True,
                                addressed_to_bot=True, now_ms=10, origin=incoming.origin, rows=[incoming],
                            )
                        self.assertIsNotNone(inflight.peek_next_pending(key, after_ts=1))
                        self.assertEqual(len(tracker.list_components('paced-run')), 6)
                    clock[0] += delay
                    await asyncio.sleep(0)
                async def after_frame(index):
                    if index == 1:
                        await asyncio.wait_for(released.wait(), 1)
                with patch.object(mt, '_mcp', runtime), patch.object(relay, '_mcp', runtime), \
                        patch.object(mt, '_maybe_intercept_bounce', AsyncMock(return_value=False)), \
                        patch.object(mt, 'resolve_reply', return_value=None), \
                        patch.object(mt, 'send_text_with_media', send), \
                        patch.object(mt, 'send_local_files', AsyncMock(return_value=0)), \
                        patch.object(mt, '_post_send_feedback', AsyncMock()), \
                        patch.object(relay, 'resolve_target', return_value=object()), \
                        patch.object(relay.hermes_client, 'steer_run', steer), \
                        patch.object(hc, '_runs_reply_clock', lambda: clock[0], create=True), \
                        patch.object(hc, '_runs_reply_sleep', sleep, create=True):
                    callback, _ = mt._make_runs_live_decisions(
                        bot=MagicMock(self_id=999), target=SimpleNamespace(private=private),
                        adapter_name='onebotv11', user_id='u1', group_id=group, is_private=private,
                        fb_key=key, nickname='bot',
                    )
                    output = '\n'.join(unit({}, body) for body in ['甲', '乙', '丙'])
                    await asyncio.wait_for(self.chat([
                        {'event': 'message.delta', 'delta': output, 'seq': 0},
                        {'event': 'tool.started', 'tool': 'lookup', 'seq': 1},
                        {'event': 'tool.completed', 'tool': 'lookup', 'seq': 2},
                        {'event': 'run.completed', 'output': output, 'seq': 3},
                    ], callback, after_frame, tracker=tracker, private=private), 2)
                self.assertEqual(sends, [('甲', 0.0), ('乙', 3.0), ('丙', 6.0)])
                self.assertEqual(len(steers), 1)
                self.assertIn(incoming.content, steers[0])
                self.assertIsNone(inflight.peek_next_pending(key, after_ts=1))
                self.assertFalse(inflight.reply_batch_busy(key))
                stored = store.get_recent(adapter='onebotv11', group_id=group, limit=10,
                                          owner_user_id='u1' if private else None)
                self.assertEqual(sum(row.content == incoming.content for row in stored), 1)

    async def test_batch_overflow_is_preserved_and_failed_steer_stays_pending(self):
        inflight = InflightRegistry()
        self.assertTrue(hasattr(inflight, 'set_reply_batch_busy'))
        key = make_inflight_key('onebotv11', False, 'u1', 'g1')
        trigger = BufferedMessage(ts=1, adapter='onebotv11', group_id='g1',
                                  user_id='u1', nickname='u1', content='触发', origin=MessageOrigin.human())
        inflight.try_enter(key, trigger, is_direct=True, original_msg_id=None, now_ms=1)
        inflight.set_reply_batch_busy(key, True)
        for index, user in enumerate(['u2', 'u3', 'u4'], 2):
            row = BufferedMessage(ts=index, adapter='onebotv11', group_id='g1',
                                  user_id=user, nickname=user, content=user, origin=MessageOrigin.human())
            self.assertEqual(inflight.try_enter(key, row, is_direct=True, original_msg_id=None,
                                               now_ms=index, commission=True), 'queued')
        self.assertEqual([fragment.user_id for entry in inflight.steer_pending(key) for fragment in entry.fragments], ['u2', 'u3', 'u4'])
        inflight.set_reply_batch_busy(key, False)
        runtime = SimpleNamespace(inflight=inflight, run_tracker=None, active_sessions=None)
        with patch.object(relay, '_mcp', runtime):
            await relay._release_runs_batch_inputs(key, 'onebotv11', False, 'u1', 'g1')
        self.assertEqual(len(inflight.steer_pending(key)), 3)

    async def test_slow_input_release_does_not_block_next_tool_stage(self):
        releasing, unblock = asyncio.Event(), asyncio.Event()
        sends, drains = [], []
        async def deliver(decision, run_id):
            sends.append(decision['body'])
        async def on_drained():
            drains.append(list(sends))
            releasing.set()
            await unblock.wait()
        deliver.on_drained = on_drained
        async def after_frame(index):
            if index == 1:
                await asyncio.wait_for(releasing.wait(), 1)
            if index == 3:
                self.assertEqual(sends, ['进度', '结果'])
                async def later():
                    await asyncio.sleep(0)
                    await asyncio.sleep(0)
                    unblock.set()
                asyncio.create_task(later())
        await asyncio.wait_for(self.chat([
            {'event': 'message.delta', 'delta': unit({}, '进度'), 'seq': 0},
            {'event': 'tool.started', 'tool': 'lookup', 'seq': 1},
            {'event': 'tool.completed', 'tool': 'lookup', 'seq': 2},
            {'event': 'message.delta', 'delta': '\n' + unit({}, '结果'), 'seq': 3},
            {'event': 'run.completed', 'output': unit({}, '结果'), 'seq': 4},
        ], deliver, after_frame), 2)
        self.assertEqual(drains, [['进度'], ['进度', '结果']])


class _FailedSendRetry:
    def setUp(self):
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, 'hermes_api_interface', 'runs')
        self.addCleanup(object.__setattr__, plugin_config, 'hermes_api_interface', previous)

    async def test_failed_live_send_is_not_mistaken_for_success_at_terminal(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / 'runs.db')
        self.addCleanup(tracker.close)
        output = '\n'.join(unit({}, body) for body in ['甲', '乙'])
        transport = real_chat(tracker, [
            {'event': 'message.delta', 'delta': output, 'seq': 0},
            {'event': 'run.completed', 'output': output, 'seq': 1},
        ], self.RUN_ID)
        failed = []
        async def chat(*args, **kwargs):
            sender = mt.send_text_with_media
            async def send(**kw):
                if kw['text'] == '甲' and not failed:
                    failed.append('甲')
                    return None
                return await sender(**kw)
            with patch.object(mt, 'send_text_with_media', send):
                return await transport(*args, **kwargs)
        with patch.object(hc, '_runs_reply_sleep', AsyncMock(), create=True):
            calls = await self._run(None, tracker=tracker, chat=chat)
        self.assertEqual(failed, ['甲'])
        self.assertCountEqual([call['text'] for call in calls['send']], ['甲', '乙'])
        self.assertEqual(len(tracker.list_components(self.RUN_ID)), 4)


    async def test_terminal_only_uses_existing_ready_delivery_order_gate(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tracker = RunTracker(Path(tmp.name) / 'runs.db')
        self.addCleanup(tracker.close)
        output = unit({}, '甲')
        chat = real_chat(tracker, [{'event': 'run.completed', 'output': output, 'seq': 0}], self.RUN_ID)
        events = []
        async def gate(run_id):
            self.assertEqual(tracker.get_delivery(run_id)['expected_state'], 'ready')
            events.append('gate')
        async def sender(**kwargs):
            self.assertTrue(events, '最终正文发送绕过原有前序交付顺序门。')
            events.append('send')
            return SimpleNamespace(msg_ids=['m1'], primary_qq_message_id='m1')
        async def transport(*args, **kwargs):
            with patch.object(mt, 'send_text_with_media', sender):
                return await chat(*args, **kwargs)
        with patch.object(mt, 'order_gate', gate):
            await self._run(None, tracker=tracker, chat=transport)
        self.assertEqual(events, ['gate', 'send'])


class ReactiveFailureRetryTests(_FailedSendRetry, e2._ReactiveHarness):
    RUN_ID = 'run-e2-h'


class PrivateFailureRetryTests(_FailedSendRetry, e2._PassiveHarness):
    RUN_ID = 'run-e2-p'


if __name__ == '__main__':
    unittest.main()
