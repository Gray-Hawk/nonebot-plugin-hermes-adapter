"""completions 的显式续跑请求（``request_continuation`` 工具的进程内登记）。

语义与旧 ``not_final`` 一致：请求「结束当前轮并追加继续执行轮」，执行链复用
``handlers.message_feedback`` 的 relay 续跑。工具只登记、不执行；一轮消费一次，
轮末未消费即丢弃——不持久化、不建表、不跨轮，也不做静默退窗之类的新语义。
"""

from __future__ import annotations

import threading

# 键与 inflight / 控制队列同形：make_inflight_key(adapter, is_private, user_id, group_id)。
ContinuationKey = tuple[str, str]


class ContinuationQueue:
    """按会话键记录「本轮请求续跑」；只登记、只取一次。"""

    def __init__(self) -> None:
        self._keys: set[ContinuationKey] = set()
        self._lock = threading.Lock()

    def request(self, key: ContinuationKey) -> None:
        with self._lock:
            self._keys.add(key)

    def pending(self, key: ContinuationKey) -> bool:
        with self._lock:
            return key in self._keys

    def take(self, key: ContinuationKey) -> bool:
        """取走该会话的续跑请求；没有则 False。"""
        with self._lock:
            if key not in self._keys:
                return False
            self._keys.discard(key)
            return True

    def discard(self, key: ContinuationKey) -> bool:
        with self._lock:
            if key not in self._keys:
                return False
            self._keys.remove(key)
            return True


CONTINUATION_QUEUE = ContinuationQueue()


def request_continuation(key: ContinuationKey) -> None:
    CONTINUATION_QUEUE.request(key)


def take_continuation(key: ContinuationKey) -> bool:
    return CONTINUATION_QUEUE.take(key)


def pending_continuation(key: ContinuationKey) -> bool:
    return CONTINUATION_QUEUE.pending(key)
