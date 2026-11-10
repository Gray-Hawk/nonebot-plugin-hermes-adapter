"""逐字节对照两份呈递矩阵 JSON:按用例输出 SAME / DIFF。"""
import json
import sys
from pathlib import Path

base = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
new = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))

missing = sorted(set(base) ^ set(new))
if missing:
    print("CASE SET MISMATCH:", missing)

same, diff = [], []
for key in sorted(base):
    if key not in new:
        continue
    if base[key] == new[key]:
        same.append(key)
    else:
        diff.append(key)

print(f"相同 {len(same)} 例: {', '.join(same)}")
print(f"不同 {len(diff)} 例:")
for key in diff:
    print(f"\n--- {key}")
    print(f"  父(979eb63): {base[key]!r}")
    print(f"  候选       : {new[key]!r}")
