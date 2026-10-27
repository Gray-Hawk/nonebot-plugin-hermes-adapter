from __future__ import annotations

import base64
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

from tests import _bootstrap  # noqa: F401
from nonebot_plugin_hermes_adapter.core import misc_config, prompt_builder, sticker_library
from nonebot_plugin_hermes_adapter.core.config_templates import MISC_CONFIG_TEMPLATE, load_yaml


PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
    "+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


class StickerFeatureSwitchTests(unittest.TestCase):
    def tearDown(self) -> None:
        misc_config._store.commit(misc_config.MiscConfig())
        init = getattr(sticker_library, "init_sticker_feature", None)
        if init is not None:
            init()

    def test_misc_config_defaults_off_and_template_matches(self) -> None:
        self.assertFalse(misc_config.load_misc_config().sticker_enabled)
        enabled = misc_config.parse_misc_config({"sticker_enabled": True})
        self.assertTrue(enabled.sticker_enabled)
        from_template = misc_config.parse_misc_config(load_yaml(MISC_CONFIG_TEMPLATE))
        self.assertFalse(from_template.sticker_enabled)

    def test_init_captures_restart_effective_switch(self) -> None:
        init = getattr(sticker_library, "init_sticker_feature", None)
        if init is None:
            self.fail("缺少表情开关启动初始化")
        misc_config._store.commit(misc_config.MiscConfig(sticker_enabled=True))
        init()
        self.assertTrue(sticker_library.sticker_feature_enabled())
        misc_config._store.commit(misc_config.MiscConfig(sticker_enabled=False))
        self.assertTrue(sticker_library.sticker_feature_enabled())
        init()
        self.assertFalse(sticker_library.sticker_feature_enabled())

    def test_prompt_guidance_follows_switch_and_allows_sticker_only_reply(self) -> None:
        for enabled in (True, False):
            sticker_library._STICKER_FEATURE_ENABLED = enabled
            reactive = prompt_builder.build_reactive_system_prompt(
                adapter="qq", group_id="123456", self_id="999", self_nickname="bot"
            )
            passive = prompt_builder.build_passive_system_prompt(
                adapter="qq", is_private=True, user_id="123456", group_id=None,
                private_decision=True,
            )
            for prompt in (reactive, passive):
                if enabled:
                    self.assertIn(prompt_builder._STICKER_TOOL_GUIDANCE, prompt)
                    self.assertIn("可只回一个表情，不附文字", prompt)
                else:
                    self.assertNotIn(prompt_builder._STICKER_TOOL_GUIDANCE, prompt)
                    self.assertNotIn("search_sticker_library", prompt)
                    self.assertNotIn("表情=ID", prompt)

    def test_startup_creates_template_without_overwriting(self) -> None:
        ensure = getattr(sticker_library, "ensure_sticker_library_template", None)
        if ensure is None:
            self.fail("缺少表情目录与范本自动建档")
        with tempfile.TemporaryDirectory() as tmp:
            index = Path(tmp) / "sticker_library" / "index.csv"
            with patch.object(sticker_library, "default_sticker_index_path", return_value=index):
                self.assertTrue(ensure())
                self.assertTrue(index.is_file())
                content = index.read_text(encoding="utf-8")
                self.assertTrue(content.startswith("id,kind,file,note\n"))
                self.assertIn("speechless", content)
                index.write_text("id,kind,file,note\ncustom,自用,custom.gif,不覆盖\n", encoding="utf-8")
                second = ensure()
                self.assertTrue(second)
                self.assertIn("custom", index.read_text(encoding="utf-8"))
                self.assertNotIn("speechless", index.read_text(encoding="utf-8"))
        startup = (Path(__file__).resolve().parents[1] / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("init_sticker_feature()", startup)
        self.assertIn("ensure_sticker_library_template()", startup)

    def test_template_example_is_comment_only_and_keeps_library_empty(self) -> None:
        ensure = getattr(sticker_library, "ensure_sticker_library_template", None)
        if ensure is None:
            self.fail("缺少表情目录与范本自动建档")
        with tempfile.TemporaryDirectory() as tmp:
            index = Path(tmp) / "sticker_library" / "index.csv"
            with patch.object(sticker_library, "default_sticker_index_path", return_value=index):
                self.assertTrue(ensure())
            content = index.read_text(encoding="utf-8")
            self.assertTrue(content.startswith("id,kind,file,note\n"))
            self.assertIn("speechless", content)  # 范本仍保留可读示例
            library = sticker_library.StickerLibrary(index.parent)
            with patch.object(sticker_library, "logger") as fake_logger:
                rows, total = library.search()
            self.assertEqual((rows, total), ([], 0))
            fake_logger.warning.assert_not_called()

            (index.parent / "ok.png").write_bytes(PNG)
            with index.open("a", encoding="utf-8", newline="") as f:
                f.write("   # 缩进注释行也整行跳过\n")
                f.write("ok,开心,ok.png,真实条目\n")
            with patch.object(sticker_library, "logger") as fake_logger:
                rows, total = library.search()
            self.assertEqual(total, 1)
            self.assertEqual([row.id for row in rows], ["ok"])
            fake_logger.warning.assert_not_called()


if __name__ == "__main__":
    unittest.main()
