#!/usr/bin/env python3
"""配置离线迁移工具（只读旧配置，只写新目录，绝不原地覆盖、绝不回显 secret）。

把旧 ``HERMES_*`` 环境变量（.env）与既有 media/user YAML 迁移成：
  - 一份 ``AGENT_*`` 新 .env（8 键，写到 ``--env-output``）
  - 四份带注释 YAML（chat / misc / media / user_lists，写到 ``--output-dir``）

安全纪律：
  - 默认只写“不存在”的目标文件；目标已存在则跳过，绝不覆盖、绝不改输入文件。
  - 目标文件以 O_EXCL 原子独占创建；含凭据的新 ENV 从创建起权限即为 0600。
  - 任何日志/报告都不打印 secret（AGENT_API_KEY 值永不回显）。
  - 旧键删除 / 固定 / 合并严格按 v6 计划（见 core/config_templates.OLD_KEYS）。
  - 三条历史条数上限（perception_buffer / keyword_soft_context /
    proactive_max_messages）存在互相冲突的非默认值时直接报错退出，不静默乱选。
  - perception_text_length 非默认值会与旧硬编码渲染上限 800 合并（取 max）并显式 WARN。

本脚本只依赖标准库 + PyYAML + python-dotenv，不 import nonebot，可离线运行：

    python scripts/migrate_config_v6.py \
        --env-file .env --output-dir ./v6 --env-output ./v6/.env

可选输入（保留既有媒体/名单文件里的自定义值）：
    --media-yaml <path> --user-yaml <path>
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from dotenv import dotenv_values  # noqa: E402

from core import config_templates as ct  # noqa: E402

_EXIT_OK = 0
_EXIT_ERR = 1

_YAML_FILES = ("chat_config.yaml", "misc_config.yaml", "media_config.yaml", "user_lists.yaml")

_SECRET_KEYS = ("AGENT_API_KEY", "HERMES_API_KEY")


# --------------------------------------------------------------------------- #
# 值解析 / 类型收敛
# --------------------------------------------------------------------------- #


def _parse_bool(raw: str) -> bool:
    s = raw.strip().lower()
    if s in ("true", "1", "yes", "on"):
        return True
    if s in ("false", "0", "no", "off"):
        return False
    raise ValueError(f"无法解析为布尔: {raw!r}")


def _parse_json_list(raw: str) -> list[str]:
    try:
        val = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"无法解析为 JSON 数组: {raw!r} ({exc})") from exc
    if not isinstance(val, list):
        raise ValueError(f"期望 JSON 数组,实际: {raw!r}")
    return [str(x) for x in val]


def coerce(raw: str, type_: str):
    """把 .env 字符串值按目标类型收敛；失败抛 ValueError（不静默丢值）。"""
    if type_ == "str":
        return raw
    if type_ == "int":
        return int(raw.strip())
    if type_ == "float":
        return float(raw.strip())
    if type_ == "bool":
        return _parse_bool(raw)
    if type_ == "list":
        return _parse_json_list(raw)
    raise ValueError(f"未知类型: {type_}")


def _env_quote(val) -> str:
    """把值渲染成 dotenv 安全片段（空值裸写，特殊字符双引号包裹）。"""
    if isinstance(val, bool):
        s = "true" if val else "false"
    else:
        s = str(val)
    if s == "":
        return ""
    if re.fullmatch(r"[A-Za-z0-9_./:@+-]+", s):
        return s
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


# --------------------------------------------------------------------------- #
# 合并解析（三条历史条数上限 / 文本上限）
# --------------------------------------------------------------------------- #


def resolve_history_limit(old_env: dict[str, str | None]):
    """三条历史条数上限 → history_message_limit。

    返回 (value, warnings, errors)。互相冲突的非默认值报错，不静默乱选。
    """
    non_defaults: dict[str, int] = {}
    for key in ct.HISTORY_LIMIT_SOURCES:
        raw = old_env.get(f"HERMES_{key.upper()}")
        if raw is None:
            continue
        try:
            val = int(raw.strip())
        except ValueError:
            return (None, [], [f"{key} 值无法解析为整数: {raw!r}"])
        if val < 1:
            return (None, [], [f"{key}={val} 无法迁移：history_message_limit 必须 >= 1"])
        if val != ct.HISTORY_LIMIT_DEFAULT:
            non_defaults[key] = val

    distinct = set(non_defaults.values())
    if len(distinct) >= 2:
        detail = ", ".join(f"{k}={v}" for k, v in sorted(non_defaults.items()))
        return (
            None,
            [],
            [
                "三个历史条数上限键存在互相冲突的非默认值，无法确定 history_message_limit："
                f"{detail}；请人工指定唯一值后重跑"
            ],
        )
    if len(distinct) == 1:
        val = next(iter(distinct))
        src = next(iter(non_defaults))
        return (val, [f"history_message_limit 取 {src}={val}（其余两条为默认 10）"], [])
    return (ct.HISTORY_LIMIT_DEFAULT, [], [])


def resolve_text_max(old_env: dict[str, str | None]):
    """perception_text_length + 旧硬编码 800 → history_text_max_chars。

    非默认值会与旧渲染上限 800 合并（取 max）并显式 WARN，不静默吞。
    """
    raw = old_env.get("HERMES_PERCEPTION_TEXT_LENGTH")
    if raw is None:
        return (ct.TEXT_MAX_DEFAULT, [], [])
    try:
        val = int(raw.strip())
    except ValueError:
        return (None, [], [f"perception_text_length 值无法解析为整数: {raw!r}"])
    if val == ct.TEXT_MAX_INPUT_DEFAULT:
        return (ct.TEXT_MAX_DEFAULT, [], [])
    merged = max(val, ct.TEXT_MAX_RENDER_LEGACY)
    warn = (
        f"perception_text_length 非默认值 {val} 与旧硬编码渲染上限 "
        f"{ct.TEXT_MAX_RENDER_LEGACY} 合并：入库与渲染现共用同一上限，"
        f"取 max 得 history_text_max_chars = {merged}"
    )
    return (merged, [warn], [])


# --------------------------------------------------------------------------- #
# 迁移引擎
# --------------------------------------------------------------------------- #


class Migration:
    def __init__(self) -> None:
        self.env_values: dict[str, object] = {}
        self.yaml_values: dict[str, dict] = {f: {} for f in ct.YAML_SPECS}
        self.media_doc: dict = {}
        self.user_doc: dict = {}
        self.deleted: list[tuple[str, str]] = []
        self.fixed: list[tuple[str, str]] = []
        self.merged: dict[str, object] = {}
        self.warnings: list[str] = []
        self.errors: list[str] = []


def build_migration(old_env: dict[str, str | None], media_doc=None, user_doc=None) -> Migration:
    m = Migration()
    m.media_doc = dict(media_doc or {})
    m.user_doc = dict(user_doc or {})
    if old_env.get("HERMES_TRANSPORT_ERROR_FALLBACK_TEXT") is None:
        m.warnings.append(
            "transport_error_fallback_text 未显式设置：旧版非空默认不保留，"
            "按 v6 目标默认迁移为空串（上游故障时用户侧静默，内部日志不变）"
        )

    # 未知旧键（不在 82 键表内）只 WARN，不静默吞掉
    for key in old_env:
        if key.startswith("HERMES_"):
            bare = key[len("HERMES_"):].lower()
            if bare not in ct.OLD_KEYS:
                m.warnings.append(f"旧键 {key} 不在 82 键处置表内，跳过（不迁移）")

    # 逐键迁移（merge_history / merge_text 与 deleted / fixed 单独处理）
    for bare, (kind, target, _reason) in ct.OLD_KEYS.items():
        if kind in ("merge_history", "merge_text", "deleted", "fixed"):
            continue
        if target is None:  # 防御：迁移类必然有目标键
            m.errors.append(f"{bare} 处置类型 {kind} 缺少目标键")
            continue
        raw = old_env.get(f"HERMES_{bare.upper()}")
        if raw is None:
            continue
        try:
            if target == "AGENT_API_INTERFACE":
                value = "runs" if _parse_bool(raw) else "completions"
            else:
                value = coerce(raw, ct.TARGET_KEYS[target]["type"])
        except Exception as exc:  # noqa: BLE001 - 迁移需捕获全部解析错误并归集
            m.errors.append(f"{bare} 迁移失败: {exc}")
            continue

        if kind == "env":
            m.env_values[target] = value
        elif kind in ("chat", "misc"):
            m.yaml_values[ct.TARGET_KEYS[target]["file"]][target] = value
        elif kind == "media":
            m.media_doc.setdefault(target, value)
        elif kind == "user_lists":
            m.user_doc.setdefault(target, value)

    # 合并键
    hist_val, hist_warn, hist_err = resolve_history_limit(old_env)
    m.warnings += hist_warn
    m.errors += hist_err
    if not hist_err:
        m.merged["history_message_limit"] = hist_val
        m.yaml_values["chat_config.yaml"]["history_message_limit"] = hist_val

    txt_val, txt_warn, txt_err = resolve_text_max(old_env)
    m.warnings += txt_warn
    m.errors += txt_err
    if not txt_err:
        m.merged["history_text_max_chars"] = txt_val
        m.yaml_values["misc_config.yaml"]["history_text_max_chars"] = txt_val

    # 撤并清单（保持 82 键顺序）
    for bare, (kind, _target, reason) in ct.OLD_KEYS.items():
        if kind == "deleted":
            m.deleted.append((bare, reason))
        elif kind == "fixed":
            m.fixed.append((bare, reason))

    return m


def render_outputs(m: Migration) -> tuple[str, dict[str, str]]:
    """返回 (env_text, {filename: yaml_text})。"""
    env_text = render_env_output(m.env_values)
    yaml_texts: dict[str, str] = {}
    for fname in _YAML_FILES:
        if fname in ("media_config.yaml", "user_lists.yaml"):
            doc = m.media_doc if fname == "media_config.yaml" else m.user_doc
            yaml_texts[fname] = ct.render_migrated_yaml(fname, doc)
        else:
            yaml_texts[fname] = ct.render_migrated_yaml(fname, m.yaml_values[fname])
    return env_text, yaml_texts


def render_env_output(env_values: dict[str, object]) -> str:
    """渲染 AGENT_* .env（逐键注释 + 键间空行）。"""
    blocks: list[list[str]] = []
    for name, default, _type, comment in ct.ENV_SPEC:
        val = env_values.get(name, default)
        blocks.append(ct._comment_lines(comment, 0) + [f"{name}={_env_quote(val)}"])
    return "\n\n".join("\n".join(b) for b in blocks).rstrip() + "\n"


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _load_yaml_optional(path: str | None, label: str, warnings: list[str]) -> dict | None:
    if not path:
        return None
    p = Path(path)
    if not p.is_file():
        return None
    try:
        doc = ct.load_yaml(p.read_text(encoding="utf-8"))
        if doc is None:
            return {}
        if not isinstance(doc, dict):
            raise ValueError("顶层必须是映射(YAML mapping)")
        return doc
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"{label} 读取失败,按空档处理(其自定义值无法保留): {exc}")
        return None


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="S2 v6 配置离线迁移工具")
    ap.add_argument("--env-file", required=True, help="旧 .env（HERMES_* 键）")
    ap.add_argument("--output-dir", required=True, help="四份带注释 YAML 的输出目录")
    ap.add_argument("--env-output", required=True, help="AGENT_* 新 .env 输出路径")
    ap.add_argument("--media-yaml", default=None, help="既有 media_config.yaml（保留自定义值）")
    ap.add_argument("--user-yaml", default=None, help="既有 user_lists.yaml（保留自定义值）")
    ap.add_argument("--dry-run", action="store_true", help="只生成并打印报告，不写盘")
    return ap.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    env_file = Path(args.env_file)
    if not env_file.is_file():
        print(f"错误: 找不到旧 .env: {env_file}", file=sys.stderr)
        return _EXIT_ERR

    old_env = dotenv_values(str(env_file), interpolate=False)
    if not old_env:
        print(f"警告: {env_file} 未解析到任何键", file=sys.stderr)

    warnings: list[str] = []
    if not args.media_yaml:
        warnings.append("未提供 --media-yaml：既有媒体自定义值不会带入输出；如有既有文件请显式指定")
    if not args.user_yaml:
        warnings.append("未提供 --user-yaml：既有名单自定义值不会带入输出；如有既有文件请显式指定")
    media_doc = _load_yaml_optional(args.media_yaml, "media_config.yaml", warnings)
    user_doc = _load_yaml_optional(args.user_yaml, "user_lists.yaml", warnings)

    m = build_migration(old_env, media_doc, user_doc)
    m.warnings = warnings + m.warnings

    env_text, yaml_texts = render_outputs(m)

    # ---- 报告（不回显任何 secret 值） ----
    print("== 迁移报告 ==")
    print(f"旧 .env 键数: {len([k for k in old_env if k.startswith('HERMES_')])}")
    print(f"删除的旧键 ({len(m.deleted)}):")
    for bare, reason in m.deleted:
        print(f"  - {bare}: {reason}")
    print(f"固定行为的旧键 ({len(m.fixed)}):")
    for bare, reason in m.fixed:
        print(f"  - {bare}: {reason}")
    print(f"合并键 ({len(m.merged)}):")
    for k, v in m.merged.items():
        print(f"  - {k} = {v}")
    if m.warnings:
        print(f"警告 ({len(m.warnings)}):")
        for w in m.warnings:
            print(f"  ! {w}")
    if m.errors:
        print(f"错误 ({len(m.errors)}):")
        for e in m.errors:
            print(f"  ✗ {e}")
        print("因存在错误,未写任何文件。")
        return _EXIT_ERR

    if args.dry_run:
        print("DRY RUN: 未写盘。将产出:")
        for fname in _YAML_FILES:
            print(f"  - {Path(args.output_dir) / fname}")
        print(f"  - {args.env_output}")
        return _EXIT_OK

    # ---- 写盘（默认只写不存在的新文件,绝不覆盖） ----
    outputs: dict[Path, str] = {Path(args.env_output): env_text}
    for fname in _YAML_FILES:
        outputs[Path(args.output_dir) / fname] = yaml_texts[fname]

    created: list[str] = []
    skipped: list[str] = []
    for path, text in outputs.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        # O_EXCL 把“检查不存在 + 创建”合成一个原子动作；ENV 含凭据，从创建起即 0600。
        mode = 0o600 if path == Path(args.env_output) else 0o644
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
        except FileExistsError:
            skipped.append(str(path))
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        created.append(str(path))

    print(f"创建 ({len(created)}):")
    for p in created:
        print(f"  + {p}")
    if skipped:
        print(f"跳过(已存在,未覆盖) ({len(skipped)}):")
        for p in skipped:
            print(f"  - {p}")
    return _EXIT_OK


if __name__ == "__main__":
    sys.exit(run())
