# EVIDENCE_T_4e54acd6 — Agent Adapter 更名施工原始输出

候选 `2032bfc`（发布元数据 + 运行代码）＋ `d612dc4`（测试/脚本/网关/verify），基线 `944cc50`。
全部跑法统一：解释器 `/home/neonsky/NeonBot/.venv/bin/python`（网关插件用 Hermes venv），
`-B` ＋ `PYTHONDONTWRITEBYTECODE=1`，`HOME`／`XDG_DATA_HOME`／`XDG_CACHE_HOME`／`TMPDIR`
全部重定向到 `~/.hermes/profiles/coder_deepseek/cache/scratch/aa-rename/base/` 下的沙箱，
cwd 在中立目录（网关插件必须离开仓库根，避免 `utils.py` 遮蔽）。

| 文件 | 内容 |
|---|---|
| `01_focused.log` | `tests.test_module_rename`（新回归 7 例，-v） |
| `02_related.log` | 相关套件：module_rename + antigravity_acp + evidence_probe_readonly + queued_lane_interim_client + user_facing_neutral_text + antigravity_mcp_files + antigravity_stream_steer |
| `03_full.log` | 全量 `discover -s tests -t .`（901 例）；首尾保留，INFO 行略 |
| `04_turn.log` | turn 门 `tests._bootstrap turn_tests.test_envelope_entry`（6 例） |
| `05_gateway_queued.log` / `06_gateway_steer.log` | 网关插件独立测试（48 / 21） |
| `07_verify.log` | `verify.py` 25 项（含新名七处一致、旧名白名单、旧名 import 阻断） |
| `08_old_name_mask_probe.log` | 旧包遮罩探针：未装引导器时旧名被 site-packages 满足；装引导器后被阻断、新名指向本工作树 |
| `09_wheel_audit.log` | wheel 审计：Name/Version/top_level/RECORD 自校验/package-data/旧名白名单 |
| `10_wheel_install_check.log` | 隔离 venv 装 wheel → `load_plugin` 成功、元数据与子模块检查 |
| `11_release_sync.log` | 发布暂存区构建（234 文件、逐文件 sha256、17 项禁入） |
| `12_build.log` | 隔离 venv `python -m build --wheel`（首尾保留） |
| `13_empty_wheel_guard.log` | 扁平开发树直接构建被 `setup.py` 守卫拒绝（无空 wheel） |
| `14_scan_source_digest.log` | 脱敏残留扫描：源树（digest: 总数 + 按文件命中） |
| `15_scan_staging.log` | 同一规则扫暂存区（66 命中，与源树活文件逐文件一致） |
| `16_wheel_sha256.txt` | wheel 校验和与字节数 |
| `17_scan_baseline_digest.log` | 基线 `944cc50` 同一扫描（对照：活文件命中数相同） |

历史快照纪律：本目录只放本次施工的原始输出；`EVIDENCE*/DELIVERY*/review_t_*` 下的旧产物一字未改。
