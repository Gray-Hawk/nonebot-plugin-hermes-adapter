"""Unified public configuration contract; runtime replay lives in entry tests."""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import unittest

from tests import _bootstrap  # noqa: F401
from nonebot_plugin_hermes_adapter.core.chat_config import ChatConfig, parse_chat_config
from nonebot_plugin_hermes_adapter.core.config_templates import CHAT_CONFIG_TEMPLATE, load_yaml
from scripts.migrate_config_v6 import build_migration, render_outputs

OLD_DEBOUNCE_KEYS = (
    "reply_debounce_private_sec",
    "reply_debounce_group_sec",
    "reply_debounce_max_wait_sec",
)


class UnifiedDebounceConfigTests(unittest.TestCase):
    def test_migration_discards_old_debounce_values_instead_of_exposing_aliases(self):
        migration = build_migration({
            "HERMES_REPLY_DEBOUNCE_PRIVATE_SEC": "0",
            "HERMES_REPLY_DEBOUNCE_GROUP_SEC": "90",
            "HERMES_REPLY_DEBOUNCE_MAX_WAIT_SEC": "1",
            "HERMES_REPLY_DELAY_SEC": "30",
        })
        self.assertFalse(migration.errors)
        _, outputs = render_outputs(migration)
        doc = load_yaml(outputs["chat_config.yaml"])
        self.assertEqual({k: v for k, v in doc.items() if k.startswith("reply_debounce")},
                         {"reply_debounce_sec": 6.0})
        self.assertTrue(set(OLD_DEBOUNCE_KEYS).issubset(k for k, _ in migration.deleted))
        self.assertEqual(doc["reply_delay_for_bot_sec"], 30.0)

    def test_generated_and_example_yaml_expose_only_unified_debounce(self):
        example = Path(__file__).resolve().parents[1] / "chat_config.example.yaml"
        for name, text in (("generated", CHAT_CONFIG_TEMPLATE), ("example", example.read_text(encoding="utf-8"))):
            with self.subTest(source=name):
                doc = load_yaml(text)
                self.assertEqual({k: v for k, v in doc.items() if k.startswith("reply_debounce")},
                                 {"reply_debounce_sec": 6.0})
                for key in OLD_DEBOUNCE_KEYS:
                    self.assertNotIn(key, text)
                self.assertEqual(doc["reply_delay_for_bot_sec"], 0.0)

    def test_only_unified_debounce_field_with_six_second_default(self):
        for cfg in (ChatConfig(), parse_chat_config({})):
            with self.subTest(cfg=cfg):
                fields = asdict(cfg)
                self.assertEqual({k: v for k, v in fields.items() if k.startswith("reply_debounce")},
                                 {"reply_debounce_sec": 6.0})


if __name__ == "__main__":
    unittest.main()
