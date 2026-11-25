from __future__ import annotations

import importlib.util
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from tests import _bootstrap  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
GATE_PATH = ROOT / "scripts" / "check_upload_gate.py"

AGENT = ("NormiAgent", "344182818+NormiAgent@users.noreply.github.com")
HUMAN = ("Gray Hawk", "34613788+Gray-Hawk@users.noreply.github.com")
LOCAL_AGENT = ("coder_deepseek", "coder_deepseek@local")


def load_gate():
    spec = importlib.util.spec_from_file_location("check_upload_gate", GATE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


gate = load_gate()


class FixtureRepo:
    """临时仓库：用固定身份提交，供树/历史/暂存区检查使用。"""

    def __init__(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name)

    def close(self) -> None:
        self._tmp.cleanup()

    def run(self, *args: str, identity=AGENT) -> subprocess.CompletedProcess:
        env = {
            **os.environ,
            "GIT_AUTHOR_NAME": identity[0], "GIT_AUTHOR_EMAIL": identity[1],
            "GIT_COMMITTER_NAME": identity[0], "GIT_COMMITTER_EMAIL": identity[1],
            "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null",
        }
        proc = subprocess.run(
            ["git", "-c", f"safe.directory={self.path}", *args],
            cwd=str(self.path), capture_output=True, text=True, env=env,
        )
        if proc.returncode != 0:
            raise AssertionError(f"git {args} 失败: {proc.stderr}")
        return proc

    def init(self) -> None:
        self.run("init", "-q", "-b", "dev")

    def write(self, rel: str, text: str = "x\n") -> None:
        target = self.path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    def commit(self, message: str, identity=AGENT) -> str:
        self.run("add", "-A")
        self.run("commit", "-q", "-m", message, identity=identity)
        return self.run("rev-parse", "HEAD").stdout.strip()


class PathWhitelistTests(unittest.TestCase):
    def test_product_paths_allowed(self) -> None:
        for path in (
            "core/session.py",
            "handlers/message.py",
            "mcp/tools/push_message.py",
            "scripts/check_upload_gate.py",
            "tests/test_upload_gate.py",
            "gateway_plugins/session-compress/plugin.yaml",
            "skill/SKILL.md",
            "SAMPLES/runs_terminal_single_b4.json",
            "README.md",
            "pyproject.toml",
            "chat_config.example.yaml",
        ):
            with self.subTest(path=path):
                self.assertEqual(gate.path_violations(path), [])

    def test_delivery_docs_rejected(self) -> None:
        self.assertTrue(gate.path_violations("DELIVERY_t_f87e7426.md"))

    def test_evidence_dirs_rejected(self) -> None:
        for path in ("EVIDENCE/full_tests.txt", "ACCEPTANCE/x.md", "REVIEW/x.md", "core/EVIDENCE/x.txt"):
            with self.subTest(path=path):
                self.assertTrue(gate.path_violations(path))

    def test_caches_rejected(self) -> None:
        for path in ("core/__pycache__/session.cpython-313.pyc", "tests/x.pyc", ".pytest_cache/v", "core/x.log"):
            with self.subTest(path=path):
                self.assertTrue(gate.path_violations(path))

    def test_unlisted_root_and_dirs_rejected(self) -> None:
        for path in ("notes.md", "docs/design.md", "internal/report.md"):
            with self.subTest(path=path):
                self.assertTrue(gate.path_violations(path))


class MessageTests(unittest.TestCase):
    def test_conventional_chinese_passes(self) -> None:
        for subject in (
            "feat(qqtpc): 会话域归一到 qqtpc，真实 adapter 运行时解析",
            "fix: 残留路由不作存活证据",
            "chore: 引入上游 0.5.4 基线",
        ):
            with self.subTest(subject=subject):
                self.assertEqual(gate.message_violations(subject), [])

    def test_non_conventional_rejected(self) -> None:
        for subject in ("更新一下", "feature: 中文", "feat 中文"):
            with self.subTest(subject=subject):
                self.assertTrue(gate.message_violations(subject))

    def test_english_only_rejected(self) -> None:
        self.assertTrue(gate.message_violations("feat: rebuild Runs delivery and steer presentation"))

    def test_process_noise_rejected(self) -> None:
        cases = {
            "feat: 候选方案落地": "候选",
            "fix: 返修第 2 轮补丁": "返修",
            "fix(qqtpc): 复审 N1-N4 闭合": "复审",
            "test(evidence): RED/GREEN 原始输出": "RED",
            "docs: verify 22/22 输出": "verify 22/22",
            "feat: 全量 982 OK": "982 OK",
            "fix: 关掉 t_979bff13 的问题": "t_979bff13",
            "feat: 支持 1234567890 的指令": "1234567890",
            "fix: 对齐 DeepSeek 行为": "DeepSeek",
            "feat: 补 /home/" + "neonsky 路径": "/home/",
        }
        for subject, token in cases.items():
            with self.subTest(subject=subject):
                problems = gate.message_violations(subject)
                self.assertTrue(problems, f"应拦下：{subject}")
                self.assertTrue(
                    any(token in item for item in problems),
                    f"{subject} 的报错里应含 {token}：{problems}",
                )

    def test_body_noise_rejected(self) -> None:
        problems = gate.message_violations("feat: 新增会话域", "本轮返修后全量 982 OK，verify 22/22。")
        self.assertTrue(problems)


class AuthorTests(unittest.TestCase):
    def test_allowed(self) -> None:
        self.assertEqual(gate.author_violations("NormiAgent <344182818+NormiAgent@users.noreply.github.com>"), [])
        self.assertEqual(gate.author_violations("Gray Hawk <34613788+Gray-Hawk@users.noreply.github.com>"), [])

    def test_local_identities_rejected(self) -> None:
        for identity in (
            "coder_deepseek <coder_deepseek@local>",
            "NormiAgent <normi@local>",
            "Neonsky <neonsky@" + "VM-" + "101.lan>",
            "Hermes Worker <worker@local>",
            "DeepSeek <deepseek@local>",
        ):
            with self.subTest(identity=identity):
                self.assertTrue(gate.author_violations(identity))


class TreeAndHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = FixtureRepo()
        self.repo.init()
        self.addCleanup(self.repo.close)

    def test_clean_repo_passes(self) -> None:
        self.repo.write("core/session.py")
        self.repo.write("README.md")
        head = self.repo.commit("feat: 新增会话域")
        self.assertEqual(gate.check_tree(head, root=self.repo.path), [])
        self.assertEqual(gate.check_commits(head, root=self.repo.path), [])

    def test_delivery_and_evidence_tree_rejected(self) -> None:
        self.repo.write("core/session.py")
        self.repo.write("DELIVERY_t_f87e7426.md")
        self.repo.write("EVIDENCE/full.txt")
        head = self.repo.commit("feat: 新增会话域")
        problems = gate.check_tree(head, root=self.repo.path)
        self.assertTrue(any("DELIVERY_" in item for item in problems), problems)
        self.assertTrue(any("EVIDENCE" in item for item in problems), problems)
        # 历史里带进去同样要被拦下（本轮这类污染的复现）
        self.assertTrue(gate.check_commits(head, root=self.repo.path))

    def test_pyc_tree_rejected(self) -> None:
        self.repo.write("core/session.py")
        self.repo.write("core/__pycache__/session.cpython-313.pyc")
        head = self.repo.commit("feat: 新增会话域")
        self.assertTrue(gate.check_tree(head, root=self.repo.path))

    def test_local_author_rejected(self) -> None:
        self.repo.write("core/session.py")
        head = self.repo.commit("feat: 新增会话域", identity=LOCAL_AGENT)
        self.assertTrue(gate.check_commits(head, root=self.repo.path))

    def test_human_author_kept_but_noisy_message_rejected(self) -> None:
        self.repo.write("README.md")
        head = self.repo.commit("docs: 更新 README", identity=HUMAN)
        self.assertEqual(gate.check_commits(head, root=self.repo.path), [])
        self.repo.write("LICENSE", "license\n")
        head2 = self.repo.commit("更新", identity=HUMAN)
        self.assertTrue(gate.check_commits(head2, root=self.repo.path))

    def test_staged_paths_rejected_before_commit(self) -> None:
        self.repo.write("core/session.py")
        self.repo.commit("feat: 新增会话域")
        self.repo.write("EVIDENCE/pending.txt")
        self.repo.run("add", "-A")
        self.assertTrue(gate.check_index(root=self.repo.path))

    def test_push_ready_requires_fast_forward(self) -> None:
        self.repo.write("core/session.py")
        base = self.repo.commit("feat: 新增会话域")
        self.repo.write("core/session.py", "y\n")
        self.repo.commit("fix: 调整会话域")
        self.assertEqual(gate.check_push_ready(base, root=self.repo.path), [])
        # 被改写的分支不再是该 SHA 的快进后继
        self.repo.run("checkout", "-q", "--orphan", "rewrite")
        self.repo.write("core/session.py", "z\n")
        rewritten = self.repo.commit("feat: 重造后的会话域")
        problems = gate.check_push_ready(base, root=self.repo.path)
        self.assertTrue(any("快进后继" in item for item in problems), problems)
        self.assertEqual(gate.check_commits(rewritten, root=self.repo.path), [])


class DeliveredTreeTests(unittest.TestCase):
    """交付树自己必须过硬门——含本次重造出来的历史。"""

    def test_delivered_history_passes_gate(self) -> None:
        if not (ROOT / ".git").exists():
            self.skipTest("非 git 检出，跳过整树审计")
        proc = subprocess.run(
            ["python3", str(GATE_PATH), "--commits", "HEAD", "--tree", "HEAD"],
            cwd=str(ROOT), capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_delivered_tree_has_no_caches_or_deliveries(self) -> None:
        proc = subprocess.run(
            ["git", "-c", f"safe.directory={ROOT}", "ls-files"],
            cwd=str(ROOT), capture_output=True, text=True,
        )
        if proc.returncode != 0:
            self.skipTest("非 git 检出")
        offenders = [
            line for line in proc.stdout.splitlines()
            if line.startswith(("DELIVERY_", "EVIDENCE/", "ACCEPTANCE/", "REVIEW/"))
            or line.endswith(".pyc") or "__pycache__" in line
        ]
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
