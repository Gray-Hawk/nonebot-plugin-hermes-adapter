# 发布工具补丁：release_sync.py 的公共发行面排除

**这份补丁不是仓库的一部分运行逻辑**：`release_sync.py` 住在默认 profile 的
`~/.hermes/skills/nonebot-hermes-plugin-modding/scripts/`，改它属于另一条独立授权面
（t_46fb47e4 明文禁止写 `~/.hermes/skills/**`）。本目录只交付可复现补丁，正式同步由
主人授权的卡执行。

## 为什么需要

`release_sync.py` 按「除 deny 清单外全复制」组发布暂存区，而 `pyproject.toml` 的
`packages.find` 会把包目录下的子包一起收进 wheel。结果 0.2.0 候选 wheel 里带着
`tests/`、`scripts/`、`gateway_plugins/`、`turn_tests/`、`verify.py`（212 个 `.py`），
既超出主人 2026-11-14 定的发行边界，也把 17 行个人主机路径与一处内部取证群号带进发行物
（复核 t_9392b23d：wheel 解包残留扫描 49 命中 / 3 豁免 / 218 文件）。

补丁只做一件事：复制前把开发文件挡在暂存区外（`DEV_ONLY_PATHS`，按相对路径首段匹配）。
工具其余行为（布局、逐文件 sha256 双向核对、`--require-clean`、`--emit-examples`）不变。

## 补丁基线

| 项 | 值 |
|---|---|
| 目标文件 | `~/.hermes/skills/nonebot-hermes-plugin-modding/scripts/release_sync.py` |
| 基线行数 | 368 |
| 基线 sha256 | `5db9f6f06fe999ffb343c47bc655a8762523aab0587e6da4411d212c576d6265` |
| 打补丁后行数 | 375 |
| 打补丁后 sha256 | `276b7e27d880305cfd1b33c9e3a8e7aaecbe9c4580b03f8a01e38c1495f63447` |

## 应用

```bash
cd ~/.hermes/skills/nonebot-hermes-plugin-modding/scripts
patch -p1 --dry-run < <repo>/scripts/release_tooling/release_sync_public_surface.patch
patch -p1            < <repo>/scripts/release_tooling/release_sync_public_surface.patch
sha256sum release_sync.py   # 应为 276b7e27…
```

若基线 sha256 已变（别的卡改过该工具），不要硬打：以本补丁的三处语义（docstring 发行面
说明、`DEV_ONLY_PATHS` 常量、`deny_reason` 首段拒绝）为准重新落一遍。

## 本卡怎么用的

未改 `~/.hermes/skills/**`：把基线脚本复制到本卡 scratch，在副本上应用同一补丁
（sha256 `276b7e27…` 一致），用该副本跑 `--require-clean` 组暂存区与构建 wheel。
仓库侧 `pyproject.toml` 另有一道 `packages.find.exclude` 闸，`scripts/audit_public_wheel.py`
再对成品 wheel 做反向断言。
