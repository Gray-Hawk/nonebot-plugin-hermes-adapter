import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from tests import _bootstrap  # noqa: F401


SCRIPT = Path(__file__).parents[1] / 'scripts' / 'repair_session_generation_fences.py'
spec = importlib.util.spec_from_file_location('repair_session_generation_fences', SCRIPT)
repair = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = repair
spec.loader.exec_module(repair)


class RepairSessionGenerationFenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.keys_path = root / 'session_keys.db'
        self.runs_path = root / 'active_runs.db'
        keys = sqlite3.connect(self.keys_path)
        keys.execute('''CREATE TABLE session_keys (
            internal_id TEXT PRIMARY KEY, session_key TEXT NOT NULL,
            generation INTEGER NOT NULL, updated_at REAL NOT NULL,
            last_activity REAL NOT NULL DEFAULT 0, idle_armed INTEGER NOT NULL DEFAULT 0,
            keepalive_last REAL NOT NULL DEFAULT 0)''')
        keys.executemany(
            'INSERT INTO session_keys VALUES (?,?,?,?,?,?,?)',
            [
                ('qqtpc+group+1', 'hermes-qqtpc+group+1-g23', 23, 1.0, 10.0, 1, 9.0),
                ('qqtpc+group+2', 'hermes-qqtpc+group+2-g5', 5, 1.0, 10.0, 1, 9.0),
                ('qqtpc+private+3', 'hermes-qqtpc+private+3-g7', 7, 1.0, 10.0, 1, 9.0),
            ],
        )
        keys.commit()
        keys.close()
        runs = sqlite3.connect(self.runs_path)
        runs.execute('''CREATE TABLE delivery_generation_fences (
            scope_key TEXT PRIMARY KEY, fence_seq INTEGER NOT NULL,
            fence_generation INTEGER, updated_at REAL NOT NULL)''')
        runs.executemany(
            'INSERT INTO delivery_generation_fences VALUES (?,?,?,?)',
            [
                ('onebotv11:group:1', 1, 1079, 1.0),
                ('qqtpc:group:2', 1, 5, 1.0),
                ('qqtpc:private:3', 1, 7, 1.0),
            ],
        )
        runs.commit()
        runs.close()

    def row(self, internal_id):
        con = sqlite3.connect(self.keys_path)
        try:
            return con.execute(
                'SELECT session_key,generation,idle_armed,keepalive_last FROM session_keys WHERE internal_id=?',
                (internal_id,),
            ).fetchone()
        finally:
            con.close()

    def test_dry_run_reports_legacy_domain_max_and_writes_nothing(self):
        before = Path(self.keys_path).read_bytes()
        rows = repair.scan(self.keys_path, self.runs_path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].internal_id, 'qqtpc+group+1')
        self.assertEqual(rows[0].new_generation, 1080)
        self.assertEqual(rows[0].new_session_key, 'hermes-qqtpc+group+1-g1080')
        self.assertEqual(Path(self.keys_path).read_bytes(), before)

    def test_apply_raises_only_stale_generation_and_disarms_idle(self):
        rows = repair.apply(self.keys_path, self.runs_path, now=100.0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(self.row('qqtpc+group+1'), ('hermes-qqtpc+group+1-g1080', 1080, 0, 9.0))
        self.assertEqual(self.row('qqtpc+group+2'), ('hermes-qqtpc+group+2-g5', 5, 1, 9.0))
        self.assertEqual(self.row('qqtpc+private+3'), ('hermes-qqtpc+private+3-g7', 7, 1, 9.0))
        self.assertEqual(repair.scan(self.keys_path, self.runs_path), [])


if __name__ == '__main__':
    unittest.main()
