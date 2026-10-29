"""runs 半接管对账配置（本部署补丁，alive/108 提交 C）。

custom_dir/runs_delivery.yaml，全部键可缺省：
  mode: observe                # off=停用对账与顺序门；observe=只记录；repair=允许补发
  repair_scopes: []            # 灰度：仅这些稳定 scope 提升为 repair
                               # 形态 adapter:private:<QQ> 或 adapter:group:<群号>
  grace_sec: 5.0               # legacy 主发宽限，超过才允许补发器接管
  order_gate_timeout_sec: 3.0  # 顺序门等待前序的上限，超时放行并记录
  unknown_backoff_sec: 60.0    # unknown 结果不明组件的重试退避
  unknown_max_attempts: 2      # unknown 最多认领次数（含首次），耗尽转人工
  retention_hours: 168.0       # 已完整结算台账保留期，最短 24 小时

缺省 observe：事实记录齐全但不自动补发、不实际延迟，与灰度第一步一致。
"""

from __future__ import annotations

from dataclasses import dataclass

import yaml
from nonebot import logger

from ..config import resolve_custom_dir

_MODES = ("off", "observe", "repair")


@dataclass(frozen=True)
class RunsDeliveryConfig:
    mode: str = "observe"
    repair_scopes: tuple[str, ...] = ()
    grace_sec: float = 5.0
    order_gate_timeout_sec: float = 3.0
    unknown_backoff_sec: float = 60.0
    unknown_max_attempts: int = 2
    retention_hours: float = 168.0


def load_runs_delivery_config() -> RunsDeliveryConfig:
    path = resolve_custom_dir() / "runs_delivery.yaml"
    if not path.exists():
        return RunsDeliveryConfig()
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            raise ValueError("runs_delivery.yaml 顶层必须是 mapping")
        mode = str(data.get("mode", "observe")).strip() or "observe"
        if mode not in _MODES:
            logger.warning(f"[runs-delivery] 未知 mode={mode!r},回退 observe")
            mode = "observe"
        scopes = data.get("repair_scopes") or []
        if not isinstance(scopes, list):
            raise ValueError("repair_scopes 必须是列表")
        try:
            retention_hours = max(24.0, float(data.get("retention_hours", 168.0)))
        except (TypeError, ValueError) as exc:
            logger.warning(f"[runs-delivery] retention_hours 无效,回退 168 小时: {exc}")
            retention_hours = 168.0
        return RunsDeliveryConfig(
            mode=mode,
            repair_scopes=tuple(str(s) for s in scopes),
            grace_sec=max(0.0, float(data.get("grace_sec", 5.0))),
            order_gate_timeout_sec=max(0.05, float(data.get("order_gate_timeout_sec", 3.0))),
            unknown_backoff_sec=max(0.0, float(data.get("unknown_backoff_sec", 60.0))),
            unknown_max_attempts=max(1, int(data.get("unknown_max_attempts", 2))),
            retention_hours=retention_hours,
        )
    except Exception as exc:
        logger.warning(f"[runs-delivery] 配置读取失败,回退 observe: {exc}")
        return RunsDeliveryConfig()


def stable_scope(adapter: str, is_private: bool, user_id, group_id) -> str:
    """灰度用的稳定聊天目标 scope(与 hermes scope_key 无关)。"""
    if is_private:
        return f"{adapter}:private:{user_id}"
    return f"{adapter}:group:{group_id}"


def effective_mode(cfg: RunsDeliveryConfig, *, adapter: str | None = None,
                   is_private: bool = False, user_id=None, group_id=None) -> str:
    """全局 mode＋灰度 scope 提升;off 不接受提升。"""
    if cfg.mode == "off":
        return "off"
    if adapter is not None and stable_scope(adapter, is_private, user_id, group_id) in cfg.repair_scopes:
        return "repair"
    return cfg.mode
