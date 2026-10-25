"""YAML 配置的启动加载与管理员手动重载协调器。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .config_store import ConfigLoadError, ConfigSnapshotStore


@dataclass(frozen=True)
class ConfigReloadResult:
    ok: bool
    message: str
    reloaded: tuple[str, ...] = ()


def _stores() -> tuple[ConfigSnapshotStore, ...]:
    # 延迟导入，避免配置模块与插件启动序列互依赖。
    from . import chat_config, media_config, misc_config, user_lists

    return (
        chat_config._store,
        misc_config._store,
        media_config._store,
        user_lists._store,
    )


def initialize_runtime_config() -> None:
    for store in _stores():
        store.initialize()


def reload_stores(stores: Iterable[ConfigSnapshotStore]) -> ConfigReloadResult:
    """全部候选档读验成功后才统一提交，失败则一个域也不换。"""
    store_tuple = tuple(stores)
    candidates = []
    try:
        for store in store_tuple:
            candidates.append(store.prepare_reload())
    except ConfigLoadError as exc:
        return ConfigReloadResult(False, f"配置未重载，已保留原值: {exc}")
    for store, candidate in zip(store_tuple, candidates):
        store.commit(candidate)
    names = tuple(store.label for store in store_tuple)
    return ConfigReloadResult(True, "配置重载完成: " + "、".join(names), names)


def reload_runtime_config() -> ConfigReloadResult:
    result = reload_stores(_stores())
    if result.ok:
        # prompt dump 在执行层另有一份零开销开关缓存；快照提交后同步刷新，
        # 否则命令报告成功但仍沿用启动值。失败时两边都保持旧状态。
        from .prompt_dump import init_prompt_dump

        init_prompt_dump()
    return result
