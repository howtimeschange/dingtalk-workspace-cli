#!/usr/bin/env python3

import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("ai_autoreply_config_tui.py")
SPEC = importlib.util.spec_from_file_location("ai_autoreply_config_tui", MODULE_PATH)
tui = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules["ai_autoreply_config_tui"] = tui
SPEC.loader.exec_module(tui)


class AutoReplyConfigTuiTest(unittest.TestCase):
    def sample_doc(self):
        return {
            "defaults": {
                "mode": "guarded",
                "send": True,
                "reply_engine": "codex",
                "reply_skill": "laoxing",
                "codex_audit_session_dir": "/tmp/audit",
                "codex_execute": False,
            },
            "groups": [
                {
                    "name": "SHEIN API自动上新测试群",
                    "group": "cid-1",
                    "project": "listingify",
                    "repo": "/ABS/PATH/TO/Listingify",
                    "trigger": "mention",
                    "history_context": {"enabled": True, "limit": 20},
                    "codex_proactive_followup": True,
                    "codex_execute": True,
                    "code_write_policy": "approval",
                    "enabled": True,
                },
                {
                    "name": "赢在AI",
                    "group": "cid-2",
                    "project": "crawshrimp",
                    "repo": "/ABS/PATH/TO/crawshrimp",
                    "enabled": False,
                },
            ],
        }

    def test_config_summary_includes_strategy_permissions_and_disabled_groups(self):
        summary = tui.config_summary(self.sample_doc())

        self.assertEqual(summary["enabled_count"], 1)
        self.assertEqual(summary["disabled_count"], 1)
        shein = summary["groups"][0]
        self.assertEqual(shein["strategy"], "guarded/mention/codex")
        self.assertEqual(shein["listener_count"], 2)
        self.assertIn("send", shein["permissions"])
        self.assertIn("codex-exec", shein["permissions"])
        self.assertIn("write:approval", shein["permissions"])
        self.assertIn("hist:20", shein["permissions"])
        self.assertIn("audit", shein["permissions"])

    def test_duplicate_group_supports_same_project_multi_open_but_disables_clone(self):
        doc = self.sample_doc()

        new_index = tui.duplicate_group(doc, 0)
        rows = tui.config_rows(doc)

        self.assertEqual(new_index, 1)
        self.assertEqual(rows[1].project, "listingify")
        self.assertFalse(rows[1].enabled)
        self.assertEqual(rows[1].group_id, "")
        self.assertIn("copy", rows[1].name)
        validation = tui.validate_config_document(doc)
        self.assertTrue(validation.ok, validation.errors)

    def test_validation_rejects_enabled_duplicate_group_ids(self):
        doc = self.sample_doc()
        doc["groups"][1]["enabled"] = True
        doc["groups"][1]["group"] = "cid-1"

        validation = tui.validate_config_document(doc)

        self.assertFalse(validation.ok)
        self.assertTrue(any("duplicate enabled group id" in item for item in validation.errors))

    def test_toggle_and_cycle_fields_update_model(self):
        doc = self.sample_doc()

        tui.toggle_group_enabled(doc, 1)
        tui.cycle_group_field(doc, 1, "mode", ("draft", "guarded"))
        tui.toggle_bool_field(doc, 1, "send", False)
        tui.toggle_history(doc, 1)
        tui.adjust_history_limit(doc, 1, 5)
        rows = tui.config_rows(doc)

        self.assertTrue(rows[1].enabled)
        self.assertEqual(rows[1].mode, "draft")
        self.assertFalse(rows[1].send)
        self.assertTrue(rows[1].history_context)
        self.assertEqual(rows[1].history_context_limit, 25)

    def test_save_writes_backup_and_summary_json_cli_reads_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "config.json"
            doc = self.sample_doc()
            tui.write_config_document(path, doc, make_backup=False)
            doc["groups"][0]["project"] = "listingify-v2"

            backup = tui.write_config_document(path, doc)
            completed = subprocess.run(
                [sys.executable, str(MODULE_PATH), "--config", str(path), "--summary-json"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIsNotNone(backup)
            assert backup is not None
            self.assertTrue(backup.exists())
            summary = json.loads(completed.stdout)
            self.assertEqual(summary["groups"][0]["project"], "listingify-v2")


if __name__ == "__main__":
    unittest.main()
