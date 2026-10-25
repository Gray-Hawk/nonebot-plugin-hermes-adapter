"""配置缺档自动建档公共引线。

统一 ensure／bootstrap 能力：建目录、缺档写默认模板（原子写入）、失败 WARN。
各配置文件（身份表、群档案等）的缺档自动建立都走这里，不各写各的。

原子写入：先写同目录临时文件再 os.replace，避免半写文件被读进程看到。
失败一律返回 False 并 WARN，调用方按原「空表／缺失」容错语义继续，
绝不把建档失败升级成消息处理失败。
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

from nonebot import logger


def write_text_atomic(path: Path, content: str, encoding: str = "utf-8", *, overwrite: bool = True) -> None:
    """原子写入文本文件：建父目录 → 同目录临时文件 → 原子落盘。失败抛 OSError。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".nph-tmp-")
    try:
        with os.fdopen(fd, "w", encoding=encoding) as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        if overwrite:
            os.replace(tmp, path)
        else:
            # 同目录硬链以原子方式创建目标；目标已存在时抛错，不覆盖先到的写入方。
            os.link(tmp, path)
            os.unlink(tmp)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def ensure_file(path: Path, default_content: str, log_tag: str, label: str = "配置档") -> bool:
    """缺档自动建档：文件不存在时原子写默认内容；已有文件绝不覆盖。

    成功（已存在或本次创建）返回 True；创建失败返回 False（已 WARN），
    调用方按原缺失容错语义继续，不拖垮消息链。
    """
    if path.exists():
        return True
    try:
        write_text_atomic(path, default_content, overwrite=False)
    except FileExistsError:
        return True
    except OSError as exc:
        logger.warning(f"[{log_tag}] 自动创建失败 {path}: {exc}")
        return False
    logger.info(f"[{log_tag}] 自动创建{label}: {path}")
    return True
