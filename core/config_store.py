"""YAML 配置的进程内快照：启动加载，只有显式重载才更换。

本模块只解决缓存与换快照，不认识具体配置键。解析器负责字段级容错或抛错；
启动时坏档回默认，运行中重载坏档则由调用方保留上一份有效快照。
"""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Generic, TypeVar

from nonebot import logger

from .config_bootstrap import ensure_file
from .config_templates import load_yaml

T = TypeVar("T")


class ConfigLoadError(ValueError):
    """配置不可安全加载；不泄露文件正文或凭据。"""


class ConfigSnapshotStore(Generic[T]):
    """一份不可变配置快照的持有者。

    ``prepare_reload`` 与 ``commit`` 分开，便于上层先读完所有文件再统一提交，
    避免多域配置重载到一半。单域可直接调用 ``reload``。
    """

    def __init__(
        self,
        *,
        label: str,
        path_provider: Callable[[], Path | None],
        parser: Callable[[dict], T],
        default_factory: Callable[[], T],
        default_content: str | None = None,
    ) -> None:
        self.label = label
        self._path_provider = path_provider
        self._parser = parser
        self._default_factory = default_factory
        self._default_content = default_content
        self._snapshot = default_factory()
        self._loaded = False

    def _read_candidate(self, *, create: bool) -> T:
        path = self._path_provider()
        if path is None:
            return self._default_factory()
        if create and self._default_content is not None and not ensure_file(
            path,
            self._default_content,
            log_tag=f"HERMES config:{self.label}",
            label="配置文件",
        ):
            raise ConfigLoadError(f"{self.label}: 缺档建档失败")
        if not path.is_file():
            return self._default_factory()
        try:
            doc = load_yaml(path.read_text(encoding="utf-8"))
            if doc is None:
                doc = {}
            if not isinstance(doc, dict):
                raise ValueError("顶层必须是 YAML 映射")
            return self._parser(doc)
        except ConfigLoadError:
            raise
        except Exception as exc:
            raise ConfigLoadError(f"{self.label}: {exc}") from exc

    def initialize(self, *, create: bool = True) -> T:
        """加载配置；启动/显式初始化可建档，导入期懒读禁止写盘。"""
        try:
            self._snapshot = self._read_candidate(create=create)
        except ConfigLoadError as exc:
            logger.warning(f"[HERMES config] 首次加载失败，使用默认值: {exc}")
            self._snapshot = self._default_factory()
        self._loaded = True
        return self._snapshot

    def get(self) -> T:
        if not self._loaded:
            return self.initialize(create=False)
        return self._snapshot

    def prepare_reload(self) -> T:
        """只读并校验候选配置，不改变当前快照；缺档时允许补默认模板。"""
        return self._read_candidate(create=True)

    def commit(self, candidate: T) -> None:
        self._snapshot = candidate
        self._loaded = True

    def reload(self) -> T:
        candidate = self.prepare_reload()
        self.commit(candidate)
        return candidate
