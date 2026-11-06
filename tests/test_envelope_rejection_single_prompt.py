import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tests import _bootstrap  # noqa: E402,F401

from nonebot_plugin_hermes_adapter.config import plugin_config  # noqa: E402
from nonebot_plugin_hermes_adapter.handlers import message_turns as mt  # noqa: E402


def _reject(reason: str):
    result = SimpleNamespace(parse_failed_reason=reason)
    with patch.object(mt.task_notes, "push_plugin_note") as note, \
            patch.object(mt, "_fire_followup_turn") as fire, \
            patch.object(mt, "_settle"):
        mt._handle_envelope_rejection(
            result, fb_key=("onebotv11", "private:u"), bot=object(), target=object(),
            adapter_name="onebotv11", user_id="u", group_id=None,
            is_private=True, tag="passive",
        )
    return note, fire


class EnvelopeRejectionPromptTests(unittest.TestCase):
    def test_runs_rejection_uses_only_immediate_retry_prompt(self):
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "runs")
        try:
            note, fire = _reject("runs_missing_body")
        finally:
            object.__setattr__(plugin_config, "hermes_api_interface", previous)
        note.assert_not_called()
        fire.assert_called_once()
        prompt = fire.call_args.kwargs["prompt"]
        self.assertIn("控制 JSON", prompt)
        self.assertIn("正文对象", prompt)

    def test_legacy_rejection_keeps_existing_note_behavior(self):
        previous = plugin_config.hermes_api_interface
        object.__setattr__(plugin_config, "hermes_api_interface", "legacy")
        try:
            note, fire = _reject("envelope_fields_missing")
        finally:
            object.__setattr__(plugin_config, "hermes_api_interface", previous)
        note.assert_called_once()
        fire.assert_called_once()
