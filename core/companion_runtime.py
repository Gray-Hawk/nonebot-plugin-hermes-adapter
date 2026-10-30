"""Runs Companion 每流运行时：恢复闸、持续轮询与退出清理。"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

from nonebot import logger

from .. import mcp as _mcp
from .companion_config import (
    CompanionConfig,
    CompanionStreamConfig,
    companion_config_error,
    database_file,
    load_companion_config,
    stream_for_session,
)
from .companion_delivery import (
    CompanionFeedback,
    CompanionHistoryWriter,
    OneBotComponentSender,
)
from .runs_companion import (
    CompanionConsumer,
    CompanionHttpClient,
    CompanionLedger,
    CompanionPoller,
)


@dataclass
class _Runtime:
    config: CompanionConfig
    stream: CompanionStreamConfig
    ledger: CompanionLedger
    client: CompanionHttpClient
    sender: OneBotComponentSender
    history: CompanionHistoryWriter
    consumer: CompanionConsumer
    poller: CompanionPoller
    lock: asyncio.Lock
    task: asyncio.Task | None = None
    identity: tuple = ()
    closing: bool = False

    def update_context(self, *, bot, target, adapter_name, user_id, group_id, is_private):
        target_id = str(user_id) if is_private else str(group_id)
        identity = (adapter_name, target_id, bool(is_private))
        if self.identity and self.identity != identity:
            raise ValueError("companion stream 目标身份发生变化")
        self.identity = identity
        self.sender.bot = bot
        self.sender.target = target
        self.history.bot = bot

    async def poll_once(self) -> int:
        async with self.lock:
            return await self.poller.poll_once()

    def start(self) -> None:
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        last_error = ""
        while not self.closing:
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                name = type(exc).__name__
                if name != last_error:
                    logger.warning(f"[HERMES runs-companion] 轮询暂停于 {name}，保持 companion 停投")
                    last_error = name
            else:
                last_error = ""
            await asyncio.sleep(self.config.poll_interval_sec)

    async def close(self) -> None:
        # 等当前发送与本地事实提交完成，不在平台返回后强行 cancel 制造未知回执。
        self.closing = True
        if self.task is not None:
            await self.task
        await self.client.close()
        self.ledger.close()


_runtimes: dict[str, _Runtime] = {}


def _get_runtime(stream: CompanionStreamConfig, *, bot, target, adapter_name,
                 user_id, group_id, is_private) -> _Runtime:
    cfg = load_companion_config()
    runtime = _runtimes.get(stream.stream_id)
    if runtime is None:
        token = os.environ.get(cfg.token_env, "")
        token_length = len(token.encode("utf-8"))
        if token_length < 32:
            raise ValueError(
                f"companion 独立凭据缺失或过短（env={cfg.token_env}, length={token_length}）"
            )
        db_path = database_file(cfg)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        ledger = CompanionLedger(db_path)
        client = CompanionHttpClient(
            cfg.base_url,
            token=token,
            timeout=cfg.request_timeout_sec,
        )
        sender = OneBotComponentSender(
            bot=bot, target=target, adapter_name=adapter_name,
            user_id=user_id, group_id=group_id, is_private=is_private,
        )
        history = CompanionHistoryWriter(
            ledger=ledger, bot=bot, adapter_name=adapter_name,
            user_id=user_id, group_id=group_id, is_private=is_private,
            message_buffer=_mcp.message_buffer, active_sessions=_mcp.active_sessions,
        )
        feedback = CompanionFeedback(
            bot=bot, target=target, adapter_name=adapter_name,
            user_id=user_id, group_id=group_id, is_private=is_private,
        )
        consumer = CompanionConsumer(
            ledger,
            stream_id=stream.stream_id,
            generation=stream.generation,
            sender=sender,
            history=history,
            ack=client,
            feedback=feedback,
        )
        runtime = _Runtime(
            config=cfg, stream=stream, ledger=ledger, client=client,
            sender=sender, history=history, consumer=consumer,
            poller=CompanionPoller(client, consumer), lock=asyncio.Lock(),
        )
        _runtimes[stream.stream_id] = runtime
    runtime.update_context(
        bot=bot, target=target, adapter_name=adapter_name,
        user_id=user_id, group_id=group_id, is_private=is_private,
    )
    return runtime


async def prepare_companion_run(*, session_id: str, bot, target, adapter_name: str,
                                user_id: str, group_id: str | None,
                                is_private: bool) -> bool:
    """legacy 恒放行；companion 先恢复旧事件，失败时禁止新 run。"""
    if companion_config_error():
        logger.warning("[HERMES runs-companion] 配置错误，禁止新 run")
        return False
    stream = stream_for_session(session_id)
    if stream is None:
        return True
    try:
        runtime = _get_runtime(
            stream, bot=bot, target=target, adapter_name=adapter_name,
            user_id=user_id, group_id=group_id, is_private=is_private,
        )
        await runtime.poll_once()
    except Exception as exc:
        logger.exception(f"[HERMES runs-companion] 恢复闸未通过：{type(exc).__name__}: {exc}")
        return False
    runtime.start()
    return not runtime.ledger.has_blocking_event(stream.stream_id)


async def deliver_companion_run_output(*, session_id: str, bot, target,
                                       adapter_name: str, user_id: str,
                                       group_id: str | None, is_private: bool) -> bool:
    """companion 会话总是吞掉 run output；轮询失败也绝不回 legacy 双发。"""
    if companion_config_error():
        return True
    if stream_for_session(session_id) is None:
        return False
    await prepare_companion_run(
        session_id=session_id, bot=bot, target=target, adapter_name=adapter_name,
        user_id=user_id, group_id=group_id, is_private=is_private,
    )
    return True


async def close_companion_runtimes() -> None:
    runtimes = list(_runtimes.values())
    _runtimes.clear()
    for runtime in runtimes:
        try:
            await runtime.close()
        except Exception as exc:
            logger.warning(f"[HERMES runs-companion] 关闭清理失败：{type(exc).__name__}")
