"""Antigravity CLI 的 ACP stdio 客户端。

只经官方 `agy acp`：一个插件实例一个懒启动进程（`--process-per-session`），
stdin/stdout 走 NDJSON JSON-RPC。本模块集中放进程、会话生命周期与事件归一化，
不引入后端抽象体系；HermesClient 只调用 `prompt()` 拿最终正文。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Sequence

from nonebot import logger

from ...core.storage.session_key_store import SessionKeyStore

DEFAULT_ACP_ARGS: tuple[str, ...] = ("acp", "--process-per-session")
_CLIENT_INFO = {"name": "nonebot-plugin-hermes-adapter", "version": "0.1.0"}


def build_acp_args(cli_args: Sequence[str], agent: str) -> tuple[str, ...]:
    """命令行尾部的 `--agent <name>`：空串保持官方默认 agent。

    只拼 argv 列表、不拼 shell 字符串；agent 名原样作为单独一项传递，
    空格与特殊字符不会被 shell 或进程层拆开。
    """

    args = tuple(cli_args)
    name = (agent or "").strip()
    if not name:
        return args
    return (*args, "--agent", name)


class ACPError(Exception):
    """ACP 调用失败（协议错误、进程退出、超时等）的统一异常基类。"""

    def __init__(self, message: str, *, code: int | None = None) -> None:
        super().__init__(message)
        self.code = code


class ACPProcessError(ACPError):
    """`agy acp` 进程无法启动或提前退出。"""


class ACPTimeoutError(ACPError):
    """单次 JSON-RPC 或整轮 prompt 超时。"""


def system_prompt_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def prompt_blocks(user_text: str) -> list[dict[str, Any]]:
    """首片只发纯文本内容块；图片/文件不进入 ACP。"""
    return [{"type": "text", "text": user_text}]


@dataclass
class ACPTurn:
    """一次 `session/prompt` 的接管状态（最小形态，不是第二套交付系统）。

    一条轮 = 一次 prompt RPC；steer 产生的新轮在 `user_message_chunk` 边界前不接管
    正文归属，旧轮正文仍写进旧轮。`sent` 只承担**段内**全文去重：跨轮同文是两条
    独立消息。
    """

    session_key: str
    session_id: str
    seq: int
    on_unit: Callable[[dict[str, Any]], None] | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    request_id: int | None = None
    future: asyncio.Future | None = None
    text: list[str] = field(default_factory=list)
    pending: str = ""
    sent: list[str] = field(default_factory=list)
    superseded: bool = False


class AntigravityACPClient:
    """一个懒启动的 `agy acp --process-per-session` 子进程。"""

    def __init__(
        self,
        *,
        cli_path: str = "agy",
        cli_args: Sequence[str] | None = None,
        model: str = "",
        agent: str = "",
        workspace: Path,
        prompt_timeout: float = 300.0,
        rpc_timeout: float = 60.0,
    ) -> None:
        self._cli_path = cli_path or "agy"
        self._cli_args = tuple(DEFAULT_ACP_ARGS if cli_args is None else cli_args)
        self._model = (model or "").strip()
        # 交互 /agents 的选择不跨进程生效，官方 settings 也没有默认 agent 键；
        # 只能用官方命令行参数显式指定，空串保持官方默认 agent。
        self._agent = (agent or "").strip()
        self._workspace = Path(workspace)
        self._prompt_timeout = float(prompt_timeout)
        self._rpc_timeout = float(rpc_timeout)

        self._proc: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task | None = None
        self._stderr_task: asyncio.Task | None = None
        self._pending: dict[int, asyncio.Future] = {}
        self._next_id = 0
        self._write_lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        self._session_locks: dict[str, asyncio.Lock] = {}
        # 阶段2A：本进程内每 scope 的 prompt 轮接管状态（steer 只复用这条链）。
        self._turns: dict[str, list[ACPTurn]] = {}
        self._latest_turn: dict[str, ACPTurn] = {}
        self._streaming_turn: dict[str, ACPTurn] = {}
        self._session_key_by_id: dict[str, str] = {}
        self._finishing: set[str] = set()
        self._turn_seq = 0
        self._text_errors: dict[str, str] = {}
        # 没有 session_keys.db 时的纯内存降级（单测/初始化前）；生产走持久映射。
        self._memory_sessions: dict[str, tuple[str, str, str]] = {}
        self._closed = False
        self._closing = False

    # --- 进程与握手 -------------------------------------------------------

    async def _ensure_started(self) -> None:
        async with self._start_lock:
            if self._proc is not None and self._proc.returncode is None:
                return
            self._workspace.mkdir(parents=True, exist_ok=True)
            argv = [self._cli_path, *build_acp_args(self._cli_args, self._agent)]
            try:
                self._proc = await asyncio.create_subprocess_exec(
                    *argv,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=str(self._workspace),
                )
            except Exception as exc:
                self._proc = None
                raise ACPProcessError(f"无法启动 {self._cli_path}: {exc}") from exc
            self._closed = False
            self._closing = False
            self._reader_task = asyncio.create_task(self._reader_loop())
            self._stderr_task = asyncio.create_task(self._stderr_loop())
            logger.info(
                f"[HERMES antigravity] ACP 进程已启动 pid={self._proc.pid} "
                f"workspace={self._workspace}"
            )
            try:
                await self._rpc(
                    "initialize",
                    {
                        "protocolVersion": 1,
                        "clientCapabilities": {
                            "fs": {"readTextFile": False, "writeTextFile": False},
                            "terminal": False,
                        },
                        "clientInfo": dict(_CLIENT_INFO),
                    },
                    ensure=False,
                )
            except Exception:
                await self._terminate_process()
                raise

    def _reader_loop_started(self) -> bool:
        return self._reader_task is not None and not self._reader_task.done()

    async def _stderr_loop(self) -> None:
        proc = self._proc
        if proc is None or proc.stderr is None:
            return
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    return
                # 只排空管道防阻塞；stderr 可能含工具/会话细节，不写日志也不留内存。
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def _reader_loop(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            return
        try:
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                try:
                    message = json.loads(line)
                except ValueError:
                    logger.debug("[HERMES antigravity] 忽略非 JSON stdout 行")
                    continue
                if not isinstance(message, dict):
                    continue
                await self._dispatch(message)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("[HERMES antigravity] ACP stdout 读取异常", exc_info=True)
        finally:
            code = proc.returncode
            self._fail_pending(
                ACPProcessError(f"agy acp 进程已退出 (code={code})")
            )
            if self._proc is proc:
                self._proc = None
            if self._closing:
                logger.debug(f"[HERMES antigravity] ACP 进程已关闭 code={code}")
            else:
                logger.warning(f"[HERMES antigravity] ACP 进程结束 code={code}")

    async def _dispatch(self, message: dict) -> None:
        if "method" in message:
            request_id = message.get("id")
            if request_id is not None:
                await self._answer_agent_request(message)
                return
            self._on_notification(message)
            return
        request_id = message.get("id")
        future = self._pending.pop(request_id, None) if isinstance(request_id, int) else None
        if future is None or future.done():
            return
        error = message.get("error")
        if isinstance(error, dict):
            future.set_exception(
                ACPError(
                    str(error.get("message") or "ACP 调用失败"),
                    code=error.get("code") if isinstance(error.get("code"), int) else None,
                )
            )
        else:
            future.set_result(message.get("result"))

    async def _answer_agent_request(self, message: dict) -> None:
        method = message.get("method")
        request_id = message.get("id")
        if method == "session/request_permission":
            params = message.get("params") or {}
            options = params.get("options") or []
            chosen = self._reject_option(options)
            if chosen is None:
                outcome = {"outcome": "cancelled"}
            else:
                outcome = {"outcome": "selected", "optionId": chosen}
            # 必须在原 RPC id 回复，否则 agent 侧会一直挂起等权限。
            await self._send(
                {"jsonrpc": "2.0", "id": request_id, "result": {"outcome": outcome}}
            )
            return
        # 首片没有开放任何工具；未知 agent 请求一律按方法不存在拒绝，防止卡死。
        await self._send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": f"unsupported client method: {method}"},
            }
        )

    @staticmethod
    def _reject_option(options: list) -> str | None:
        """优先 reject_once；没有时取任一 reject_*；再没有则整体取消。"""
        for option in options:
            if not isinstance(option, dict):
                continue
            kind = str(option.get("kind") or "")
            option_id = option.get("optionId")
            if kind == "reject_once" and isinstance(option_id, str):
                return option_id
        for option in options:
            if not isinstance(option, dict):
                continue
            kind = str(option.get("kind") or "")
            option_id = option.get("optionId")
            if kind.startswith("reject") and isinstance(option_id, str):
                return option_id
        return None

    def _on_notification(self, message: dict) -> None:
        if message.get("method") != "session/update":
            return
        params = message.get("params") or {}
        session_id = params.get("sessionId")
        update = params.get("update") or {}
        if not isinstance(session_id, str) or not isinstance(update, dict):
            return
        kind = update.get("sessionUpdate")
        if kind == "user_message_chunk":
            # steer 的实测语义：新 prompt 到达后旧正文先继续流出，随后一条
            # user_message_chunk 才是轮次边界。边界之后的新 chunk 归最新 prompt，
            # 边界之前的未闭合尾部随旧轮丢弃（不能拼进下一轮）。
            key = self._session_key_by_id.get(session_id)
            if key is not None and key in self._latest_turn:
                self._streaming_turn[session_id] = self._latest_turn[key]
            return
        # 只消费 agent_message_chunk 的正文本；thought/plan/工具状态不进 QQ。
        if kind != "agent_message_chunk":
            return
        content = update.get("content")
        if not isinstance(content, dict) or content.get("type") != "text":
            return
        text = content.get("text")
        if not isinstance(text, str):
            return
        turn = self._streaming_turn.get(session_id)
        if turn is None or self._text_errors.get(turn.session_key):
            # 轮次已收尾（或已 fail-closed）：游离 chunk 不进入任何一轮。
            return
        turn.text.append(text)
        self._ingest_chunk(turn, text)

    def _ingest_chunk(self, turn: ACPTurn, text: str) -> None:
        """增量拆闭合 § 单位；未闭合碎片留在本轮，终态只补剩余单位。"""
        from ...core.runs_text import TextUnitsError, take_closed_units

        try:
            units, turn.pending = take_closed_units(turn.pending + text)
        except TextUnitsError as exc:
            reason = str(getattr(exc, "reason", "") or "invalid")
            self._text_errors[turn.session_key] = reason
            logger.error(f"[HERMES antigravity] 流式正文单位不合法 reason={reason}")
            return
        self._emit_units(turn, units)

    def _emit_units(self, turn: ACPTurn, units: list[str]) -> None:
        """已闭合单位按顺序交给上层队列；段内全文去重（跨轮同文照发）。"""
        from ...core.runs_text import TEXT_UNIT_KIND

        for unit in units:
            if unit in turn.sent:
                continue
            turn.sent.append(unit)
            hook = turn.on_unit
            if hook is not None:
                hook(
                    {
                        "body": unit,
                        "kind": TEXT_UNIT_KIND,
                        "should_reply": True,
                        "_turn_seq": turn.seq,
                    }
                )

    def _new_turn(
        self,
        *,
        session_key: str,
        session_id: str,
        on_unit: Callable[[dict[str, Any]], None] | None,
        meta: dict[str, Any] | None,
        stream_here: bool,
    ) -> ACPTurn:
        self._turn_seq += 1
        turn = ACPTurn(
            session_key=session_key,
            session_id=session_id,
            seq=self._turn_seq,
            on_unit=on_unit,
            meta=dict(meta or {}),
        )
        self._turns.setdefault(session_key, []).append(turn)
        self._latest_turn[session_key] = turn
        self._session_key_by_id[session_id] = session_key
        if stream_here:
            self._streaming_turn[session_id] = turn
        return turn

    def _finalize_turns(self, session_key: str, *, emit: bool = True) -> str:
        """收尾本 scope 的全部轮：只补发各轮尚未投递的闭合单位，返回完整正文。"""
        turns = self._turns.pop(session_key, [])
        self._latest_turn.pop(session_key, None)
        for session_id, turn in list(self._streaming_turn.items()):
            if turn.session_key == session_key:
                self._streaming_turn.pop(session_id, None)
                if self._session_key_by_id.get(session_id) == session_key:
                    self._session_key_by_id.pop(session_id, None)
        for turn in turns:
            if turn.future is not None:
                if turn.request_id is not None:
                    self._pending.pop(turn.request_id, None)
                if not turn.future.done():
                    turn.future.cancel()
        full = "".join(part for turn in turns for part in turn.text)
        if not emit or not turns:
            return full
        from ...core.runs_text import TextUnitsError, split_text_units

        for turn in turns:
            try:
                units = split_text_units("".join(turn.text))
            except TextUnitsError as exc:
                reason = str(getattr(exc, "reason", "") or "invalid")
                self._text_errors[turn.session_key] = reason
                logger.error(f"[HERMES antigravity] 终态正文单位不合法 reason={reason}")
                break
            self._emit_units(turn, units)
        return full

    def active_turn(self, session_key: str) -> ACPTurn | None:
        """本 scope 正在跑的 ACP 轮（steer 分发用）；没有则 None。"""
        if session_key in self._finishing or not self._reader_loop_started():
            return None
        return self._latest_turn.get(session_key)

    async def _submit_prompt(self, turn: ACPTurn, user_text: str) -> None:
        self._next_id += 1
        request_id = self._next_id
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        turn.request_id = request_id
        turn.future = future
        self._pending[request_id] = future
        await self._send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "session/prompt",
                "params": {
                    "sessionId": turn.session_id,
                    "prompt": prompt_blocks(user_text),
                },
            }
        )

    async def _await_turn(self, turn: ACPTurn) -> None:
        future = turn.future
        if future is None:
            raise ACPError("ACP prompt 未提交")
        try:
            await asyncio.wait_for(asyncio.shield(future), timeout=self._prompt_timeout)
        except asyncio.CancelledError:
            # steer 提交失败回退时该轮已被撤销：交给 prompt 循环重新看 newest 轮，
            # 不能把内部撤销误当成整个 chat 被取消。
            if self._latest_turn.get(turn.session_key) is not turn or turn.future is not future:
                return
            raise
        except TimeoutError as exc:
            try:
                await self._send(
                    {
                        "jsonrpc": "2.0",
                        "method": "session/cancel",
                        "params": {"sessionId": turn.session_id},
                    }
                )
            except ACPError:
                pass
            raise ACPTimeoutError("ACP session/prompt 超时") from exc

    def _fail_pending(self, exc: Exception) -> None:
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(exc)
        self._pending.clear()

    async def _send(self, payload: dict) -> None:
        proc = self._proc
        if proc is None or proc.stdin is None or proc.returncode is not None:
            raise ACPProcessError("agy acp 进程不可用")
        data = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
        async with self._write_lock:
            try:
                proc.stdin.write(data)
                await proc.stdin.drain()
            except (BrokenPipeError, ConnectionResetError) as exc:
                raise ACPProcessError("agy acp stdin 已关闭") from exc

    async def _rpc(
        self,
        method: str,
        params: dict,
        *,
        timeout: float | None = None,
        ensure: bool = True,
    ) -> Any:
        if ensure and not self._reader_loop_started():
            await self._ensure_started()
        self._next_id += 1
        request_id = self._next_id
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending[request_id] = future
        try:
            await self._send(
                {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
            )
            return await asyncio.wait_for(
                future, timeout=self._rpc_timeout if timeout is None else timeout
            )
        except TimeoutError as exc:
            self._pending.pop(request_id, None)
            raise ACPTimeoutError(f"ACP {method} 超时") from exc
        except BaseException:
            self._pending.pop(request_id, None)
            raise

    # --- 会话与 prompt ----------------------------------------------------

    def _session_lock(self, session_key: str) -> asyncio.Lock:
        lock = self._session_locks.get(session_key)
        if lock is None:
            lock = asyncio.Lock()
            self._session_locks[session_key] = lock
        return lock

    def _load_session(
        self, store: SessionKeyStore | None, session_key: str
    ) -> tuple[str, str, str] | None:
        if store is not None:
            return store.load_acp_session(session_key)
        return self._memory_sessions.get(session_key)

    def _save_session(
        self,
        store: SessionKeyStore | None,
        session_key: str,
        session_id: str,
        system_hash: str,
        model: str,
    ) -> None:
        if store is not None:
            store.put_acp_session(session_key, session_id, system_hash, model, now=time.time())
        else:
            self._memory_sessions[session_key] = (session_id, system_hash, model)

    async def _create_session(
        self,
        *,
        store: SessionKeyStore | None,
        session_key: str,
        system_prompt: str,
        system_hash: str,
        model: str,
        reason: str = "",
    ) -> str:
        result = await self._rpc(
            "session/new",
            {
                "cwd": str(self._workspace),
                "mcpServers": [],
                "_meta": {
                    "antigravity": {"systemPrompt": {"append": system_prompt}}
                },
            },
        )
        session_id = str((result or {}).get("sessionId") or "")
        if not session_id:
            raise ACPError("session/new 未返回 sessionId")
        if model:
            # 真实 ACP 模型选择：session/set_model，不把模型名塞进提示词伪造。
            await self._rpc(
                "session/set_model", {"sessionId": session_id, "modelId": model}
            )
        if reason:
            logger.warning(f"[HERMES antigravity] {reason}，已新建 ACP 会话")
        self._save_session(store, session_key, session_id, system_hash, model)
        return session_id

    async def prompt(
        self,
        *,
        session_key: str,
        system_prompt: str,
        user_text: str,
        store: SessionKeyStore | None = None,
        on_unit: Callable[[dict[str, Any]], None] | None = None,
        turn_meta: dict[str, Any] | None = None,
    ) -> str:
        """跑一轮文本 prompt，返回最终 assistant 正文（含模型自己写的 §）。

        on_unit：每形成一份完整闭合的 § 正文单位就同步回调一次（流式投递）。
        终态只补发这一轮尚未回调过的单位；steer 接管的后续 prompt 也共用同一回调。
        返回的正文是这一轮所有 prompt 段（含被 steer 接管的旧段）的完整拼接。
        """
        async with self._session_lock(session_key):
            await self._ensure_started()
            record = self._load_session(store, session_key)
            system_hash = system_prompt_hash(system_prompt)
            if record is None:
                session_id = await self._create_session(
                    store=store,
                    session_key=session_key,
                    system_prompt=system_prompt,
                    system_hash=system_hash,
                    model=self._model,
                )
            else:
                session_id, stored_hash, stored_model = record
                params: dict[str, Any] = {
                    "sessionId": session_id,
                    "cwd": str(self._workspace),
                    "mcpServers": [],
                }
                changed = stored_hash != system_hash
                if changed:
                    # 标准 ACP systemPrompt 字段不生效；扩展只在变化时追加，
                    # 避免同一段系统上下文在会话里叠加多份。
                    params["_meta"] = {
                        "antigravity": {"systemPrompt": {"append": system_prompt}}
                    }
                try:
                    await self._rpc("session/resume", params)
                except ACPError as exc:
                    # 只对「会话不存在」重建；其它错误如实上抛，不掩盖鉴权/超时等问题。
                    if exc.code != -32602:
                        raise
                    # 本轮 prompt 尚未提交，直接新建会话不构成有副作用重放。
                    logger.warning(
                        f"[HERMES antigravity] session/resume 失败 code={exc.code}，重建会话"
                    )
                    session_id = await self._create_session(
                        store=store,
                        session_key=session_key,
                        system_prompt=system_prompt,
                        system_hash=system_hash,
                        model=self._model,
                    )
                else:
                    effective_model = stored_model
                    if self._model and self._model != stored_model:
                        await self._rpc(
                            "session/set_model",
                            {"sessionId": session_id, "modelId": self._model},
                        )
                        effective_model = self._model
                    if changed or effective_model != stored_model:
                        self._save_session(
                            store,
                            session_key,
                            session_id,
                            system_hash,
                            effective_model,
                        )

            self._text_errors.pop(session_key, None)
            turn = self._new_turn(
                session_key=session_key,
                session_id=session_id,
                on_unit=on_unit,
                meta=turn_meta,
                stream_here=True,
            )
            failed = True
            try:
                await self._submit_prompt(turn, user_text)
                while True:
                    latest = self._latest_turn.get(session_key)
                    if latest is None:
                        break
                    # 第一条 RPC 可能因 steer 提前 end_turn；终态归 newest prompt。
                    await self._await_turn(latest)
                    if self._latest_turn.get(session_key) is not latest:
                        continue
                    self._finishing.add(session_key)
                    break
                failed = False
            finally:
                self._finishing.discard(session_key)
                text = self._finalize_turns(session_key, emit=not failed)
            error = self._text_errors.pop(session_key, "")
            if error:
                raise ACPError(f"正文单位不合法: {error}")
            return text

    async def steer(self, *, session_key: str, text: str) -> bool:
        """在同 ACP session 上发后续 `session/prompt`（运行中 steer 接管）。

        新轮只登记接管身份；正文归属在 `user_message_chunk` 边界后才切到新轮，
        旧轮已流出的正文继续按旧轮投递。没有在跑的轮、进程已退出或本 scope 正在
        收尾时返回 False，调用方按既有排队语义投递（零丢失，不另造状态机）。
        """
        if not text or not text.strip():
            return False
        if session_key in self._finishing or not self._reader_loop_started():
            return False
        latest = self._latest_turn.get(session_key)
        if latest is None:
            return False
        turn = self._new_turn(
            session_key=session_key,
            session_id=latest.session_id,
            on_unit=latest.on_unit,
            meta=latest.meta,
            stream_here=False,
        )
        # 先接管再提交：prompt 循环在下一次醒来时必须看到 newest 轮，不能把旧 RPC
        # 的 end_turn 当作整轮结束。
        latest.superseded = True
        try:
            await self._submit_prompt(turn, text)
        except Exception:
            if turn.request_id is not None:
                self._pending.pop(turn.request_id, None)
            if turn.future is not None and not turn.future.done():
                turn.future.cancel()
            turns = self._turns.get(session_key) or []
            if turn in turns:
                turns.remove(turn)
            if self._latest_turn.get(session_key) is turn:
                self._latest_turn[session_key] = latest
            latest.superseded = False
            logger.warning("[HERMES antigravity] steer 提交失败，回退到既有排队路径")
            return False
        logger.info(
            f"[HERMES antigravity] steer 已接管在跑轮 session={latest.session_id} "
            f"len={len(text)}"
        )
        return True

    # --- 收尾 -------------------------------------------------------------

    async def _terminate_process(self) -> None:
        proc = self._proc
        self._proc = None
        self._closing = True
        if proc is not None and proc.returncode is None:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except TimeoutError:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                await proc.wait()
        for task in (self._reader_task, self._stderr_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._reader_task = None
        self._stderr_task = None
        self._fail_pending(ACPProcessError("agy acp 进程已关闭"))

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._terminate_process()

    async def healthy(self) -> bool:
        """轻量探活：能启动并完成 ACP 握手即可；不调用任何模型。"""
        try:
            await self._ensure_started()
            return True
        except ACPError:
            return False


_client: AntigravityACPClient | None = None


def _acp_workspace() -> Path:
    from ...config import resolve_custom_dir

    base = resolve_custom_dir()
    if base is None:
        base = Path(tempfile.gettempdir()) / "nonebot_plugin_hermes_adapter"
    return Path(base) / "antigravity-workspace"


def get_client() -> AntigravityACPClient:
    """插件实例级单例；配置在启动/重载时定型，不在每次调用重读。"""
    global _client
    if _client is None:
        from ...config import plugin_config

        _client = AntigravityACPClient(
            cli_path=plugin_config.hermes_antigravity_cli,
            model=plugin_config.hermes_antigravity_model,
            agent=plugin_config.hermes_antigravity_persona,
            workspace=_acp_workspace(),
            prompt_timeout=float(plugin_config.hermes_api_timeout),
        )
    return _client


async def close_client() -> None:
    global _client
    client = _client
    _client = None
    if client is not None:
        await client.close()
