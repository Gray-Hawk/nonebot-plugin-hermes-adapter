from __future__ import annotations

import inspect
import unittest

from tests import _bootstrap  # noqa: F401
from nonebot_plugin_hermes_adapter.core.config_templates import CHAT_CONFIG_TEMPLATE, OLD_KEYS, load_yaml
from nonebot_plugin_hermes_adapter.handlers.message import _debounce_arrive, _debounce_states


class GroupDebounceTests(unittest.TestCase):
    def setUp(self) -> None:
        _debounce_states.clear()
        if "fixed_window_sec" not in inspect.signature(_debounce_arrive).parameters:
            self.fail("缺少群聊固定窗口参数")

    def tearDown(self) -> None:
        _debounce_states.clear()

    def arrive(self, key, user_id, now, *, is_bot=False, replaceable=True, window=3.0):
        return _debounce_arrive(
            key,
            {"user_id": user_id, "fragments": [], "is_direct": False, "addressed_to_bot": False, "ephemeral": False, "now_ms": int(now * 1000)},
            debounce_sec=99.0,
            max_wait_sec=99.0,
            now_mono=now,
            replaceable=replaceable,
            fixed_window_sec=window,
            fire_on_cross_human=True,
            trigger_sender_is_bot=is_bot,
        )

    def test_same_sender_does_not_extend_fixed_three_second_window(self) -> None:
        first, is_new = self.arrive(("qq", "group:g1"), "A", 100.0)
        self.assertTrue(is_new)
        self.assertEqual(first.deadline_mono, 103.0)
        second, _ = self.arrive(("qq", "group:g1"), "A", 102.0)
        self.assertEqual(second.deadline_mono, 103.0)
        third, _ = self.arrive(("qq", "group:g1"), "A", 102.9)
        self.assertEqual(third.deadline_mono, 103.0)

    def test_second_human_fires_immediately(self) -> None:
        self.arrive(("qq", "group:g1"), "A", 100.0)
        second, _ = self.arrive(("qq", "group:g1"), "B", 102.0)
        self.assertEqual(second.deadline_mono, 102.0)

    def test_bot_second_does_not_interrupt_fixed_window(self) -> None:
        self.arrive(("qq", "group:g1"), "A", 100.0)
        second, _ = self.arrive(("qq", "group:g1"), "BOT", 102.0, is_bot=True)
        self.assertEqual(second.deadline_mono, 103.0)

    def test_bot_between_same_human_does_not_create_a_second_human(self) -> None:
        key = ("qq", "group:g1")
        self.arrive(key, "A", 100.0)
        self.arrive(key, "BOT", 101.0, is_bot=True)
        resumed, _ = self.arrive(key, "A", 102.0)
        self.assertEqual(resumed.deadline_mono, 103.0)

    def test_media_only_second_human_also_fires_immediately(self) -> None:
        self.arrive(("qq", "group:g1"), "A", 100.0)
        second, _ = self.arrive(("qq", "group:g1"), "B", 102.0, replaceable=False)
        self.assertEqual(second.deadline_mono, 102.0)

    def test_bot_first_delay_is_not_shortened_by_later_bot(self) -> None:
        key = ("qq", "group:g1")
        self.arrive(key, "BOT", 100.0, is_bot=True, window=30.0)
        later, _ = self.arrive(key, "BOT2", 102.0, is_bot=True)
        self.assertEqual(later.deadline_mono, 130.0)

    def test_generated_config_has_no_group_debounce_knob(self) -> None:
        self.assertNotIn("reply_debounce_group_sec", load_yaml(CHAT_CONFIG_TEMPLATE))

    def test_old_group_setting_is_deleted_by_migration(self) -> None:
        kind, target, _ = OLD_KEYS["reply_debounce_group_sec"]
        self.assertEqual((kind, target), ("deleted", None))

    def test_stale_generation_human_does_not_interrupt_current_window(self) -> None:
        key = ("qq", "group:g1")
        first, _ = _debounce_arrive(
            key, {"user_id": "A", "delivery_generation": 2},
            debounce_sec=3, max_wait_sec=0, now_mono=100,
            fixed_window_sec=3, fire_on_cross_human=True,
        )
        _debounce_arrive(
            key, {"user_id": "B", "delivery_generation": 1},
            debounce_sec=3, max_wait_sec=0, now_mono=102,
            fixed_window_sec=3, fire_on_cross_human=True,
        )
        self.assertEqual(first.payload["user_id"], "A")
        self.assertEqual(first.deadline_mono, 103)

    def test_new_generation_does_not_count_old_generation_human(self) -> None:
        key = ("qq", "group:g1")
        _debounce_arrive(
            key, {"user_id": "A", "delivery_generation": 1},
            debounce_sec=3, max_wait_sec=0, now_mono=100,
            fixed_window_sec=3, fire_on_cross_human=True,
        )
        current, _ = _debounce_arrive(
            key, {"user_id": "B", "delivery_generation": 2},
            debounce_sec=3, max_wait_sec=0, now_mono=102,
            fixed_window_sec=3, fire_on_cross_human=True,
        )
        self.assertEqual(current.payload["user_id"], "B")
        self.assertEqual(current.deadline_mono, 103)

    def test_private_helper_preserves_sliding_deadline_cap(self) -> None:
        key = ("qq", "private:u1")
        first, _ = _debounce_arrive(
            key, {"user_id": "A", "fragments": [], "is_direct": False, "addressed_to_bot": False, "ephemeral": False, "now_ms": 100000},
            debounce_sec=5.0, max_wait_sec=8.0, now_mono=100.0,
        )
        self.assertEqual(first.deadline_mono, 105.0)
        second, _ = _debounce_arrive(
            key, {"user_id": "A", "fragments": [], "is_direct": False, "addressed_to_bot": False, "ephemeral": False, "now_ms": 100000},
            debounce_sec=5.0, max_wait_sec=8.0, now_mono=102.0,
        )
        self.assertEqual(second.deadline_mono, 107.0)
        third, _ = _debounce_arrive(
            key, {"user_id": "A", "fragments": [], "is_direct": False, "addressed_to_bot": False, "ephemeral": False, "now_ms": 100000},
            debounce_sec=5.0, max_wait_sec=8.0, now_mono=104.0,
        )
        self.assertEqual(third.deadline_mono, 108.0)


if __name__ == "__main__":
    unittest.main()
