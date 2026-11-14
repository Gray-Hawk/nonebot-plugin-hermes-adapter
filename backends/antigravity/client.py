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

from ...config import plugin_config
from ...core.routing import CallerScope, resolve_caller_scope
from ...core.storage.session_key_store import SessionKeyStore

DEFAULT_ACP_ARGS: tuple[str, ...] = ("acp", "--process-per-session")
_CLIENT_INFO = {"name": "nonebot-plugin-hermes-adapter", "version": "0.1.0"}

# 阶段2C1：ACP 反向 MCP 只暴露两个只读工具（同一个 HTTP 服务；其它后端仍全量）。
ANTIGRAVITY_MCP_TOOL_NAMES: tuple[str, ...] = (
    "get_recent_messages",
    "get_message_images",
)
MCP_SERVER_NAME = "nonebot-bridge"
_MCP_TOOL_PREFIX = "mcp_"


def caller_scope_for_mcp() -> CallerScope | None:
    """本进程反向 MCP 调用方的范围——与 `mcp/auth` 同一个判定函数。

    配置了 AGENT_API_KEY 就用这把凭据走真实 `resolve_caller_scope`（uniform）；
    未配置就是既有 dev 语义。认不出（None）时调用方不得挂 MCP。

    单上游部署里 `CallerScope` 只有 dev/uniform 两态（没有 per-group token），
    所以范围由凭据决定；本轮消息上下文再用它的 `allows()` 与私聊可识别闸校验目标，
    不为 ACP 另造第二套 token 表或权限模型。
    """
    key = str(getattr(plugin_config, "hermes_api_key", "") or "").strip()
    if not key:
        return CallerScope.dev()
    return resolve_caller_scope(f"Bearer {key}")


def mcp_servers_for_turn(turn_meta: dict[str, Any] | None) -> list[dict[str, Any]]:
    """按当前消息上下文构造 ACP 的 HTTP mcpServers（new/resume 共用这一份）。

    - 只在 AGENT_MCP_ENABLED 时挂载；URL 复用既有 host/port，不新增端口或服务。
    - Bearer 取现有 AGENT_API_KEY，只放进本次 ACP 配置，不写日志/异常/错误文案。
    - 群/私都要求 scope 可识别且允许本轮目标；不成立就返回 []（安全拒绝）。
    """
    if not plugin_config.hermes_mcp_enabled:
        return []
    scope = caller_scope_for_mcp()
    if scope is None:
        return []
    meta = turn_meta or {}
    adapter = str(meta.get("adapter") or "")
    if bool(meta.get("is_private")):
        if not str(meta.get("user_id") or ""):
            return []
    else:
        group_id = str(meta.get("group_id") or "")
        if not group_id or not scope.allows(adapter, group_id):
            return []
    server: dict[str, Any] = {
        "type": "http",
        "name": MCP_SERVER_NAME,
        "url": f"http://{plugin_config.hermes_mcp_host}:{plugin_config.hermes_mcp_port}/mcp",
    }
    key = str(getattr(plugin_config, "hermes_api_key", "") or "").strip()
    if key:
        server["headers"] = [{"name": "Authorization", "value": f"Bearer {key}"}]
    return [server]


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


class ACPCancelledError(ACPError):
    """本轮被 `/run-stop` 取消：原 prompt 以 `stopReason=cancelled` 收尾。

    单独一类异常让 HermesClient 能映射 `ChatResult.is_interrupted=True`（被停 turn 走
    既有硬静默），不落成“调用失败”错误面。进程本身仍可继续复用。
    """


def system_prompt_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def prompt_blocks(
    user_text: str | Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """本轮用户内容 → `session/prompt` 的 content blocks。

    - `str` 仍是单个 text 块（纯文本轮的 wire 字节与 2B 首片一致）；
    - 已转换好的块列表（`text` / `image`）按原顺序透传：2C2 的图片块形如
      `{"type":"image","data":<base64>,"mimeType":"image/png"}`，来自既有内联媒体链，
      本函数不再下载、压缩或重新寻址。
    """
    if isinstance(user_text, str):
        return [{"type": "text", "text": user_text}]
    return [dict(block) for block in user_text]


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
    # 权限请求只认本轮的 approval_callback（steer 轮继承同轮回调）。
    approval_callback: Callable[[dict[str, Any]], Any] | None = None
    cancelled: bool = False


@dataclass
class ACPPendingPermission:
    """一条等待管理员应答的 `session/request_permission`。

    `request_id` 保留原始 RPC id 对象（真实 agy 是字符串 `jetski-acp-front:2`）：
    应答必须逐字节回同一个 id，不能转成整数或重新编号。
    """

    session_key: str
    session_id: str
    request_id: Any
    options: list[dict[str, Any]]
    expires_at: float
    timer: asyncio.Task | None = None


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
        permission_timeout: float | None = None,
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
        # None = 现读既有审批过期秒数（与 /approve 同源），测试可注入更短窗口。
        self._permission_timeout = None if permission_timeout is None else float(permission_timeout)

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
        # 阶段2B：等待管理员应答的权限请求（内存态，key=(session_key, str(原 RPC id))）。
        # 旧 ACP 请求随子进程失效；这里不持久化、不跨重启恢复。
        self._permissions: dict[tuple[str, str], ACPPendingPermission] = {}
        self._permission_tasks: set[asyncio.Task] = set()
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
            return
        result = message.get("result")
        future.set_result(result)
        if isinstance(result, dict) and str(result.get("stopReason") or "") == "cancelled":
            # /run-stop 的真实协议证据：原 prompt 以 stopReason=cancelled 收尾。
            # 在派发点识别，steer 的并行 prompt（B/C）未在 await 时也能一并结束等待。
            turn = self._turn_for_request(request_id)
            if turn is not None:
                self._mark_cancelled(turn)

    def _turn_for_request(self, request_id: Any) -> "ACPTurn | None":
        for turns in self._turns.values():
            for turn in turns:
                if turn.request_id == request_id:
                    return turn
        return None

    async def _answer_agent_request(self, message: dict) -> None:
        method = message.get("method")
        request_id = message.get("id")
        if method == "session/request_permission":
            params = message.get("params") or {}
            options = [
                item for item in (params.get("options") or []) if isinstance(item, dict)
            ]
            tool_call = params.get("toolCall")
            tool_name = str(tool_call.get("name") or "") if isinstance(tool_call, dict) else ""
            if tool_name.startswith(_MCP_TOOL_PREFIX):
                # 阶段2C1：MCP 工具一律自动 allow_once——不进人工审批 FIFO、不发通知。
                await self._auto_allow_mcp_permission(request_id, tool_name, options)
                return
            if not self._register_permission(message, options):
                # 无 callback／未知会话：安全自动拒绝（不做永久授权），绝不挂起 reader。
                await self._reply_permission(request_id, options, allow=False)
            return
        # 首片没有开放任何工具；未知 agent 请求一律按方法不存在拒绝，防止卡死。
        await self._send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": f"unsupported client method: {method}"},
            }
        )

    # --- 权限审批（阶段2B） ------------------------------------------------

    @staticmethod
    def _permission_key(session_key: str, request_id: Any) -> tuple[str, str]:
        return (session_key, str(request_id))

    @staticmethod
    def _allow_once_option(options: list[dict]) -> str | None:
        """按真实 options 的 kind 取 allow_once 的 optionId（不猜 id 字面量）。"""
        for option in options:
            option_id = option.get("optionId")
            if str(option.get("kind") or "") == "allow_once" and isinstance(option_id, str) and option_id:
                return option_id
        return None

    def _permission_expiry_sec(self) -> float:
        """审批过期秒数：与 /approve 同源（misc_config），测试可注入更短窗口。"""
        if self._permission_timeout is not None:
            return self._permission_timeout
        try:
            from ...core.misc_config import load_misc_config

            return float(load_misc_config().approval_expiry_sec)
        except Exception:
            return 300.0

    async def _reply_permission(
        self, request_id: Any, options: list[dict], *, allow: bool
    ) -> bool:
        """在原 RPC id 上回答权限请求；写管道失败只留日志（调用方决定是否留单）。"""
        option_id = (
            self._allow_once_option(options) if allow else self._reject_option(options)
        )
        outcome = (
            {"outcome": "selected", "optionId": option_id}
            if option_id
            else {"outcome": "cancelled"}
        )
        try:
            await self._send(
                {"jsonrpc": "2.0", "id": request_id, "result": {"outcome": outcome}}
            )
        except ACPError:
            logger.warning("[HERMES antigravity] 权限应答写入失败（请求保持挂起）")
            return False
        return True

    async def _auto_allow_mcp_permission(
        self, request_id: Any, tool_name: str, options: list[dict]
    ) -> None:
        """MCP 工具权限自动 allow_once（不登记挂起、不发人工通知）。

        没有 allow_once 时 `_reply_permission(allow=True)` 会退到 `cancelled`，仍是
        安全拒绝而不是放行。写帧失败只留日志：自动应答不得转成人工审批单。
        """
        if await self._reply_permission(request_id, options, allow=True):
            logger.info(f"[HERMES antigravity] MCP 工具自动放行 tool={tool_name}")
        else:
            logger.warning(
                f"[HERMES antigravity] MCP 工具权限自动应答写帧失败 tool={tool_name}"
                "（不登记人工审批）"
            )

    def _register_permission(self, message: dict, options: list[dict]) -> bool:
        """登记 pending 并把规范化后的请求交给本轮 approval_callback。

        返回 False 表示本轮没有可用的审批通道（无 callback／未知会话／无 id），
        调用方按安全拒绝收尾。通知在独立任务里跑，reader 继续读取（取消能立即生效）。
        """
        params = message.get("params") or {}
        session_id = params.get("sessionId")
        request_id = message.get("id")
        if request_id is None or not isinstance(session_id, str):
            return False
        session_key = self._session_key_by_id.get(session_id)
        if session_key is None:
            return False
        turn = self._streaming_turn.get(session_id) or self._latest_turn.get(session_key)
        callback = getattr(turn, "approval_callback", None) if turn is not None else None
        if callback is None:
            return False

        pending = ACPPendingPermission(
            session_key=session_key,
            session_id=session_id,
            request_id=request_id,
            options=options,
            expires_at=time.monotonic() + self._permission_expiry_sec(),
        )
        self._permissions[self._permission_key(session_key, request_id)] = pending
        pending.timer = asyncio.create_task(
            self._expire_permission(self._permission_key(session_key, request_id), pending)
        )
        self._spawn_permission_notify(callback, pending, params)
        return True

    def _spawn_permission_notify(self, callback, pending: ACPPendingPermission, params: dict) -> None:
        tool_call = params.get("toolCall")
        tool_call = tool_call if isinstance(tool_call, dict) else {}
        raw_input = tool_call.get("rawInput")
        payload = {
            "backend": "antigravity",
            "run_id": "",
            "session_key": pending.session_key,
            "session_id": pending.session_id,
            "request_id": str(pending.request_id),
            "title": str(tool_call.get("title") or ""),
            "raw_input": raw_input if isinstance(raw_input, dict) else {},
            "options": [
                {
                    "optionId": str(item.get("optionId") or ""),
                    "kind": str(item.get("kind") or ""),
                    "name": str(item.get("name") or ""),
                }
                for item in pending.options
            ],
        }

        async def _run() -> None:
            try:
                await callback(payload)
            except Exception:
                # 审批是附属通道：通知/登记失败不拖垮主 turn，管理员仍可重试或等超时。
                logger.warning("[HERMES antigravity] 权限审批通知失败", exc_info=True)

        task = asyncio.create_task(_run())
        self._permission_tasks.add(task)
        task.add_done_callback(self._permission_tasks.discard)

    async def _expire_permission(
        self, key: tuple[str, str], pending: ACPPendingPermission
    ) -> None:
        """超时安全拒绝：管理员一直不答时 agent 不能永久挂起。"""
        try:
            await asyncio.sleep(max(0.0, pending.expires_at - time.monotonic()))
        except asyncio.CancelledError:
            return
        if self._permissions.get(key) is not pending:
            return
        if await self._settle_permission(key, pending):
            logger.info("[HERMES antigravity] 权限请求超时，已安全拒绝")

    @staticmethod
    def _cancel_permission_timer(pending: ACPPendingPermission) -> None:
        task = pending.timer
        pending.timer = None
        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()

    @staticmethod
    def _settle_shared_approval(pending: ACPPendingPermission) -> None:
        """原 RPC 上已拒绝成功 → 同步删掉共享审批条目。

        取消/超时结清后旧条目若留在 FIFO 队首，后面所有 scope 的新审批都会被
        `/approve` 打到这条死单上，直到懒过期。只在写帧成功后调用：写失败要留单。
        """
        from ...core.approvals import settle_permission

        settle_permission(
            session_key=pending.session_key, request_id=str(pending.request_id)
        )

    async def _settle_permission(
        self, key: tuple[str, str], pending: ACPPendingPermission
    ) -> bool:
        """取消/超时语义：先按 reject_once 关掉挂起权限，再让轮次收尾。

        返回原 RPC 是否真的写出了应答帧；True 才算「已确认结清」，此时共享审批
        条目一并删除。写失败保持条目留单（管理员可重试或等懒过期）。
        """
        self._permissions.pop(key, None)
        self._cancel_permission_timer(pending)
        if not await self._reply_permission(
            pending.request_id, pending.options, allow=False
        ):
            return False
        self._settle_shared_approval(pending)
        return True

    async def _reject_permissions(self, session_key: str) -> None:
        for key, pending in [
            (k, p) for k, p in self._permissions.items() if p.session_key == session_key
        ]:
            await self._settle_permission(key, pending)

    def _drop_permissions(self, session_key: str | None = None) -> None:
        """丢弃等待中的权限（轮次收尾/进程关闭）；不写帧，仅清理内存与定时器。"""
        for key, pending in list(self._permissions.items()):
            if session_key is not None and pending.session_key != session_key:
                continue
            self._permissions.pop(key, None)
            self._cancel_permission_timer(pending)

    async def answer_permission(
        self, *, session_key: str, request_id: Any, choice: str
    ) -> bool:
        """管理员应答一条挂起权限请求；True 只在原 RPC id 写帧成功后返回。

        未知 request／已过期／重复应答／真实 options 没有对应 kind／写管道失败都返回
        False（失败关闭），调用方据此保留审批条目供重试。
        """
        key = self._permission_key(session_key, request_id)
        pending = self._permissions.get(key)
        if pending is None:
            return False
        if time.monotonic() >= pending.expires_at:
            await self._settle_permission(key, pending)
            return False
        if choice == "once":
            option_id = self._allow_once_option(pending.options)
        elif choice == "deny":
            option_id = self._reject_option(pending.options)
        else:
            return False
        if option_id is None:
            # 真实 options 里没有该 kind：不硬猜 optionId，条目继续挂着。
            return False

        self._permissions.pop(key, None)
        self._cancel_permission_timer(pending)
        try:
            await self._send(
                {
                    "jsonrpc": "2.0",
                    "id": pending.request_id,
                    "result": {"outcome": {"outcome": "selected", "optionId": option_id}},
                }
            )
        except ACPError:
            self._permissions[key] = pending
            logger.warning("[HERMES antigravity] 权限应答写入失败，条目保留可重试")
            return False
        logger.info(
            f"[HERMES antigravity] 权限已应答 choice={choice} session={pending.session_id}"
        )
        return True

    async def cancel(self, session_key: str) -> bool:
        """`/run-stop`：对真实在跑轮发无 id `session/cancel`，不伪造 run_id。

        没有在跑轮（进程未起／已收尾／正在收尾）返回 False，调用方只回执不重试。
        权限等待中的轮先按 reject_once 关掉挂起请求，否则 agent 可能读不到取消。
        """
        if session_key in self._finishing or not self._reader_loop_started():
            return False
        turn = self._latest_turn.get(session_key)
        if turn is None:
            return False
        try:
            await self._send(
                {
                    "jsonrpc": "2.0",
                    "method": "session/cancel",
                    "params": {"sessionId": turn.session_id},
                }
            )
        except ACPError:
            logger.warning("[HERMES antigravity] session/cancel 写入失败")
            return False
        # 先发取消（无 id 通知），再按 reject_once 关掉挂起的权限 RPC：
        # agent 可能正卡在权限应答上，不关掉这条 RPC 就不能处理取消。
        await self._reject_permissions(session_key)
        logger.info(f"[HERMES antigravity] 已发送 session/cancel session={turn.session_id}")
        return True

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
            # user_message_chunk 才是轮次边界。边界之前旧轮已流出的正文（含帧尾
            # 无末尾换行的合法 `§`）必须先在边界封出，边界之后的新 chunk 才归新 prompt；
            # 旧轮真正缺闭合的碎片仍按协议丢弃（不能拼进下一轮）。
            key = self._session_key_by_id.get(session_id)
            if key is not None and key in self._latest_turn:
                current = self._streaming_turn.get(session_id)
                target = self._boundary_turn(key, session_id)
                if target is not None and target is not current:
                    if current is not None:
                        self._seal_turn_remainder(current)
                    self._streaming_turn[session_id] = target
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

    def _boundary_turn(self, session_key: str, session_id: str) -> ACPTurn | None:
        """本次 `user_message_chunk` 归属哪一轮：按已提交 prompt 的轮序取最早待接管轮。

        steer 允许多条 prompt 同时在跑，回显可能晚到：B 的回显到来前 C 已提交时，
        `_latest_turn` 是 C，但边界属于 B。轮次列表本身按提交顺序追加，因此「当前正文轮
        之后的第一轮」就是此次开始的轮；只有它之后没有待接管轮时（首条 prompt 自己的
        回显）才沿用最新轮。不按正文哈希或时间猜身份，也不另建身份台账。
        """
        turns = self._turns.get(session_key) or []
        current = self._streaming_turn.get(session_id)
        start = 0
        if current is not None:
            try:
                start = turns.index(current) + 1
            except ValueError:
                start = 0
        for turn in turns[start:]:
            if turn.session_id == session_id:
                return turn
        return self._latest_turn.get(session_key)

    def _seal_turn_remainder(self, turn: ACPTurn) -> None:
        """轮次边界封段：把本轮尚存尾部按完整单位发出，不足一份的仍丢弃。

        边界由 `user_message_chunk` 确定，因此帧尾那个没有换行终结的独立整行 `§`
        此刻就是旧轮正文的合法结尾（同一 `split_text_units` 协议，不补造缺标记正文）。
        """
        if not turn.pending or self._text_errors.get(turn.session_key):
            return
        from ...core.runs_text import TextUnitsError, split_text_units

        try:
            units = split_text_units(turn.pending)
        except TextUnitsError as exc:
            reason = str(getattr(exc, "reason", "") or "invalid")
            self._text_errors[turn.session_key] = reason
            logger.error(f"[HERMES antigravity] 边界正文单位不合法 reason={reason}")
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
        approval_callback: Callable[[dict[str, Any]], Any] | None = None,
    ) -> ACPTurn:
        self._turn_seq += 1
        turn = ACPTurn(
            session_key=session_key,
            session_id=session_id,
            seq=self._turn_seq,
            on_unit=on_unit,
            meta=dict(meta or {}),
            approval_callback=approval_callback,
        )
        self._turns.setdefault(session_key, []).append(turn)
        self._latest_turn[session_key] = turn
        self._session_key_by_id[session_id] = session_key
        if stream_here:
            self._streaming_turn[session_id] = turn
        return turn

    def _finalize_turns(self, session_key: str, *, emit: bool = True) -> str:
        """收尾本 scope 的全部轮：只补发各轮尚未投递的闭合单位，返回完整正文。"""
        # 轮次结束（正常/取消/异常）都不再等待权限应答：请求已随本轮结束失效。
        self._drop_permissions(session_key)
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

    async def _submit_prompt(
        self, turn: ACPTurn, user_text: str | Sequence[dict[str, Any]]
    ) -> None:
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
            # 不能把内部撤销误当成整个 chat 被取消。取消传播的轮（cancelled=True）
            # 同样直接返回，由 prompt 循环按 cancelled 语义收尾。
            if turn.cancelled:
                return
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

    def _mark_cancelled(self, turn: ACPTurn) -> None:
        for item in self._turns.get(turn.session_key) or []:
            item.cancelled = True
            if item.future is not None and not item.future.done():
                item.future.cancel()

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
        mcp_servers: Sequence[dict[str, Any]] | None = None,
    ) -> str:
        result = await self._rpc(
            "session/new",
            {
                "cwd": str(self._workspace),
                "mcpServers": list(mcp_servers or []),
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
        user_text: str | Sequence[dict[str, Any]],
        store: SessionKeyStore | None = None,
        on_unit: Callable[[dict[str, Any]], None] | None = None,
        turn_meta: dict[str, Any] | None = None,
        approval_callback: Callable[[dict[str, Any]], Any] | None = None,
    ) -> str:
        """跑一轮 prompt，返回最终 assistant 正文（含模型自己写的 §）。

        user_text：文本（str）或已转换好的 ACP 内容块（text／image，按原顺序发出）。

        on_unit：每形成一份完整闭合的 § 正文单位就同步回调一次（流式投递）。
        终态只补发这一轮尚未回调过的单位；steer 接管的后续 prompt 也共用同一回调。
        approval_callback：本轮 `session/request_permission` 的登记/通知通道；空则不登记、
        按 reject_once 安全拒绝（无管理员可答时不扩大授权）。
        返回的正文是这一轮所有 prompt 段（含被 steer 接管的旧段）的完整拼接；
        被 `/run-stop` 取消时抛 `ACPCancelledError`，不返回半截正文。
        """
        async with self._session_lock(session_key):
            await self._ensure_started()
            record = self._load_session(store, session_key)
            system_hash = system_prompt_hash(system_prompt)
            # 阶段2C1：本轮消息上下文对应的 MCP 定义；new/resume 传同一份。
            mcp_servers = mcp_servers_for_turn(turn_meta)
            if mcp_servers:
                server = mcp_servers[0]
                logger.info(
                    f"[HERMES antigravity] MCP tools={','.join(ANTIGRAVITY_MCP_TOOL_NAMES)}"
                    f" server={server.get('name')} url={server.get('url')}"
                    f" bearer={'yes' if server.get('headers') else 'no'}"
                )
            if record is None:
                session_id = await self._create_session(
                    store=store,
                    session_key=session_key,
                    system_prompt=system_prompt,
                    system_hash=system_hash,
                    model=self._model,
                    mcp_servers=mcp_servers,
                )
            else:
                session_id, stored_hash, stored_model = record
                params: dict[str, Any] = {
                    "sessionId": session_id,
                    "cwd": str(self._workspace),
                    "mcpServers": mcp_servers,
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
                        mcp_servers=mcp_servers,
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
                approval_callback=approval_callback,
            )
            failed = True
            cancelled = False
            settled = False
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
                cancelled = any(
                    item.cancelled for item in self._turns.get(session_key) or []
                )
                # 取消语义：已生成但尚未出站的余量按 interrupted 丢弃，不等终态补发。
                failed = cancelled
                settled = not cancelled
            finally:
                if not settled:
                    # 超时/取消/异常收尾：原权限 RPC 必须先用既有安全 reject 结清，
                    # 再清内存与 timer。只撤 timer 等于把请求丢给无人应答的 agent
                    # （prompt 超时与权限到期默认同秒时，prompt 先到就会走到这里）。
                    try:
                        await self._reject_permissions(session_key)
                    except Exception:  # noqa: BLE001 - 收尾不能掩盖原始异常
                        logger.warning(
                            "[HERMES antigravity] 收尾拒绝挂起权限失败", exc_info=True
                        )
                self._finishing.discard(session_key)
                text = self._finalize_turns(session_key, emit=not failed)
            error = self._text_errors.pop(session_key, "")
            if cancelled:
                # 取消结果优先：正文错误已经记过日志，不能覆盖 interrupted 语义
                # （否则被停 turn 会落到 transport-fallback，并把队列余量继续发出）。
                if error:
                    logger.error(
                        f"[HERMES antigravity] 取消收尾时忽略正文单位错误 reason={error}"
                    )
                raise ACPCancelledError("ACP turn 已被 /run-stop 取消")
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
            approval_callback=latest.approval_callback,
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
        # 权限等待随子进程失效：清内存与定时器，不跨重启/不重通知。
        self._drop_permissions()
        for task in list(self._permission_tasks):
            if not task.done():
                task.cancel()

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
