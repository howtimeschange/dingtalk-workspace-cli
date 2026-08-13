#!/usr/bin/env python3

import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stdout


MODULE_PATH = pathlib.Path(__file__).with_name("ai_autoreply_worker.py")
SPEC = importlib.util.spec_from_file_location("ai_autoreply_worker", MODULE_PATH)
worker = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules["ai_autoreply_worker"] = worker
SPEC.loader.exec_module(worker)


class AutoReplyWorkerTest(unittest.TestCase):
    def parse(self, argv):
        return worker.normalize_args(worker.build_parser().parse_args(argv))

    def make_reply_skill(self, tmp, name="laoxing-test"):
        skill_dir = pathlib.Path(tmp) / name
        skill_dir.mkdir()
        skill_path = skill_dir / "SKILL.md"
        skill_path.write_text(
            "---\nname: laoxing-test\n---\n# 老邢测试 Skill\n先判断，少废话，追证据，有下一步。",
            encoding="utf-8",
        )
        return skill_path

    def tearDown(self):
        worker.load_reply_skill_context.cache_clear()

    def test_build_consume_command_uses_flatten_ndjson_and_scope(self):
        args = self.parse([
            "--dws-bin", "/usr/local/bin/dws",
            "--profile", "corp:user",
            "--event-key", "user_im_message_receive_group",
            "--group", "cid-1",
            "--query", "urgent,help",
            "--duration", "55m",
            "--max-events", "3",
        ])

        self.assertEqual(worker.build_consume_command(args), [
            "/usr/local/bin/dws",
            "--profile", "corp:user",
            "event", "consume", "user_im_message_receive_group",
            "--flatten", "-f", "ndjson",
            "--group", "cid-1",
            "--query", "urgent,help",
            "--duration", "55m",
            "--max-events", "3",
        ])

    def test_resolve_reply_target_auto_group_and_sender(self):
        group_event = {
            "type": "user_im_message_receive_group",
            "conversation_id": "cid-1",
            "sender_open_dingtalk_id": "open-1",
        }
        self.assertEqual(
            worker.resolve_reply_target(group_event, "user_im_message_receive_group", "auto"),
            worker.ReplyTarget("--group", "cid-1"),
        )

        o2o_event = {
            "type": "user_im_message_receive_o2o",
            "conversation_id": "cid-o2o",
            "sender_open_dingtalk_id": "open-2",
        }
        self.assertEqual(
            worker.resolve_reply_target(o2o_event, "user_im_message_receive_o2o", "auto"),
            worker.ReplyTarget("--open-dingtalk-id", "open-2"),
        )

    def test_send_requires_explicit_scope(self):
        args = self.parse(["--send"])
        self.assertFalse(worker.has_send_scope(args))

        args = self.parse(["--send", "--allow-conversation", "cid-1,cid-2"])
        self.assertTrue(worker.has_send_scope(args))
        self.assertTrue(worker.target_allowed(worker.ReplyTarget("--group", "cid-2"), args))
        self.assertFalse(worker.target_allowed(worker.ReplyTarget("--group", "cid-x"), args))

    def test_extract_reply_accepts_chat_completion_shape(self):
        self.assertEqual(
            worker.extract_reply({
                "choices": [
                    {"message": {"content": "  好的，我稍后看一下。  "}},
                ],
            }),
            "好的，我稍后看一下。",
        )

    def test_generate_reply_injects_reply_skill_into_system_prompt(self):
        with tempfile.TemporaryDirectory() as tmp:
            skill_path = self.make_reply_skill(tmp)
            args = self.parse(["--reply-skill", "laoxing-test", "--reply-skill-path", str(skill_path)])
            event = {
                "sender": "张三",
                "content": "这个怎么处理？",
                "conversation_id": "cid-1",
                "message_id": "msg-1",
            }
            captured = {}
            previous = worker.call_chat_completion

            def fake_call(messages, _args, **_kwargs):
                captured["messages"] = messages
                return {"choices": [{"message": {"content": "可以，先把截图发我。"}}]}

            worker.call_chat_completion = fake_call
            try:
                reply = worker.generate_reply(event, args)
            finally:
                worker.call_chat_completion = previous

            self.assertEqual(reply, "可以，先把截图发我。")
            system = captured["messages"][0]["content"]
            self.assertIn("已加载回复 Skill: laoxing-test", system)
            self.assertIn("老邢测试 Skill", system)

    def test_clip_reply(self):
        self.assertEqual(worker.clip_reply("abcdef", 4), "abcd...")
        self.assertEqual(worker.clip_reply("abcdef", 0), "abcdef")

    def test_build_send_command_uses_uuid_and_profile(self):
        target = worker.ReplyTarget("--open-dingtalk-id", "open-1")
        cmd = worker.build_send_command("dws", "corp:user", target, "收到", "uuid-1")
        self.assertEqual(cmd, [
            "dws", "--profile", "corp:user",
            "chat", "message", "send",
            "--open-dingtalk-id", "open-1",
            "--text", "收到",
            "--uuid", "uuid-1",
            "--format", "json",
        ])

    def test_seen_file_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "seen.jsonl"
            worker.append_seen(path, "event-1", "drafted")
            self.assertEqual(worker.load_seen(path), {"event-1"})

    def test_process_event_draft_writes_seen(self):
        args = self.parse(["--event-key", "user_im_message_receive_o2o"])
        event = {
            "type": "user_im_message_receive_o2o",
            "event_id": "event-1",
            "message_id": "msg-1",
            "sender": "张三",
            "sender_open_dingtalk_id": "open-1",
            "content": "在吗",
        }
        with tempfile.TemporaryDirectory() as tmp:
            args.state_file = pathlib.Path(tmp) / "seen.jsonl"
            previous = worker.generate_reply
            worker.generate_reply = lambda _event, _args: "在的，我稍后回复。"
            try:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    worker.process_event(event, set(), args)
            finally:
                worker.generate_reply = previous

            record = json_loads_line(buf.getvalue())
            self.assertEqual(record["action"], "draft")
            self.assertEqual(record["reply"], "在的，我稍后回复。")
            self.assertEqual(worker.load_seen(args.state_file), {"event-1"})

    def test_process_event_fetches_history_context_before_reply(self):
        args = self.parse([
            "--event-key", "user_im_message_receive_group",
            "--group", "cid-1",
            "--history-context",
            "--history-context-limit", "2",
        ])
        event = {
            "type": "user_im_message_receive_group",
            "event_id": "event-history",
            "message_id": "msg-history",
            "conversation_id": "cid-1",
            "sender": "张三",
            "sender_open_dingtalk_id": "open-1",
            "content": "这个怎么处理？",
        }
        history = [{"sender": "李四", "content": "前面说过要等发布窗口"}]

        with tempfile.TemporaryDirectory() as tmp:
            args.state_file = pathlib.Path(tmp) / "seen.jsonl"
            previous_fetch = worker.fetch_history_context
            previous_reply = worker.generate_reply
            worker.fetch_history_context = lambda _event, _args: history

            def reply_with_history(received_event, _args):
                self.assertEqual(received_event.get("_history_context"), history)
                return "看到了，我结合前面的发布窗口确认一下。"

            worker.generate_reply = reply_with_history
            try:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    worker.process_event(event, set(), args)
            finally:
                worker.fetch_history_context = previous_fetch
                worker.generate_reply = previous_reply

            record = json_loads_line(buf.getvalue())
            self.assertEqual(record["action"], "draft")
            self.assertEqual(record["history_context_count"], 1)
            self.assertEqual(record["reply"], "看到了，我结合前面的发布窗口确认一下。")

    def test_message_items_from_payload_reads_result_messages(self):
        payload = {
            "success": True,
            "result": {
                "hasMore": True,
                "messages": [
                    {
                        "content": "表格链接在这里",
                        "createTime": "2026-08-12 15:36:55",
                        "openMessageId": "msg-doc",
                        "sender": "黄宇森",
                    },
                    {
                        "content": "之前是从批量上传图包进去的",
                        "createTime": "2026-08-10 15:18:38",
                        "openMessageId": "msg-history",
                        "sender": "易成",
                    },
                ],
            },
        }

        items = worker.message_items_from_payload(payload)
        compact = [worker.compact_history_item(item) for item in items]

        self.assertEqual(len(items), 2)
        self.assertEqual(compact[0]["message_id"], "msg-doc")
        self.assertIn("表格链接", compact[0]["content"])

    def test_fetch_history_context_uses_result_messages_and_prompt_includes_history(self):
        args = self.parse([
            "--event-key", "user_im_message_receive_at",
            "--group", "cid-listingify",
            "--history-context",
            "--history-context-limit", "20",
            "--reply-engine", "codex",
        ])
        event = {
            "type": "user_im_message_receive_at",
            "event_id": "event-with-history",
            "message_id": "msg-current",
            "conversation_id": "cid-listingify",
            "event_time": 1786520231842,
            "content": "@我 还有色块图问题",
        }
        completed = worker.subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps({
                "success": True,
                "result": {
                    "messages": [
                        {
                            "content": "listingfy测试问题汇总 https://alidocs.dingtalk.com/i/nodes/doc",
                            "createTime": "2026-08-12 15:36:55",
                            "openMessageId": "msg-doc",
                            "sender": "黄宇森",
                        },
                        {
                            "content": "@我 还有色块图问题",
                            "createTime": "2026-08-12 15:37:11",
                            "openMessageId": "msg-current",
                            "sender": "黄宇森",
                        },
                    ],
                },
            }),
            stderr="",
        )
        previous_run = worker.subprocess.run
        worker.subprocess.run = lambda *_args, **_kwargs: completed
        try:
            history = worker.fetch_history_context(event, args)
        finally:
            worker.subprocess.run = previous_run

        self.assertEqual(len(history), 1)
        self.assertIn("alidocs.dingtalk.com", history[0]["content"])
        classification = worker.classify_feedback(dict(event, _history_context=history), args)
        prompt = worker.build_codex_session_prompt(dict(event, _history_context=history), classification, args)
        self.assertIn("最近聊天背景", prompt)
        self.assertIn("钉钉文档、表格", prompt)
        self.assertIn("alidocs.dingtalk.com", prompt)

    def test_process_event_send_skips_disallowed_target(self):
        args = self.parse(["--event-key", "user_im_message_receive_group", "--send"])
        event = {
            "type": "user_im_message_receive_group",
            "event_id": "event-2",
            "message_id": "msg-2",
            "conversation_id": "cid-not-allowed",
            "sender_open_dingtalk_id": "open-1",
            "content": "帮我看一下",
        }
        with tempfile.TemporaryDirectory() as tmp:
            args.state_file = pathlib.Path(tmp) / "seen.jsonl"
            buf = io.StringIO()
            with redirect_stdout(buf):
                worker.process_event(event, set(), args)

            record = json_loads_line(buf.getvalue())
            self.assertEqual(record["action"], "skipped")
            self.assertEqual(record["reason"], "target_not_allowed")
            self.assertEqual(worker.load_seen(args.state_file), {"event-2"})

    def test_load_agent_config_parses_enabled_group_allowlist(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "config.json"
            path.write_text(json.dumps({
                "defaults": {"mode": "draft", "profile": "corp:user"},
                "groups": [
                    {
                        "name": "抓虾反馈群",
                        "group": "cid-crawshrimp",
                        "project": "crawshrimp",
                        "repo": "/ABS/PATH/TO/crawshrimp",
                        "query": "问题,反馈,bug",
                        "ready_message": "我在了",
                    },
                    {
                        "name": "disabled",
                        "group": "cid-disabled",
                        "enabled": False,
                    },
                ],
            }), encoding="utf-8")

            defaults, groups = worker.load_agent_config(path)
            self.assertEqual(defaults["profile"], "corp:user")
            self.assertEqual(len(groups), 1)
            self.assertEqual(groups[0].name, "抓虾反馈群")
            self.assertEqual(groups[0].group_id, "cid-crawshrimp")
            self.assertEqual(groups[0].project, "crawshrimp")
            self.assertEqual(groups[0].ready_message, "我在了")

    def test_load_agent_config_parses_reply_skill_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            skill_path = self.make_reply_skill(tmp)
            audit_dir = pathlib.Path(tmp) / "audit"
            path = pathlib.Path(tmp) / "config.json"
            path.write_text(json.dumps({
                "defaults": {
                    "reply_skill": "laoxing-test",
                    "reply_skill_path": str(skill_path),
                    "codex_audit_session_dir": str(audit_dir),
                },
                "groups": [
                    {
                        "name": "赢在AI",
                        "group": "cid-winyai",
                    },
                ],
            }), encoding="utf-8")

            _defaults, groups = worker.load_agent_config(path)

            self.assertEqual(groups[0].reply_skill, "laoxing-test")
            self.assertEqual(groups[0].reply_skill_path, str(skill_path))
            self.assertEqual(groups[0].codex_audit_session_dir, str(audit_dir))

    def test_args_for_group_sets_admission_and_send_allowlist(self):
        base = self.parse(["--config", "config.json"])
        group = worker.GroupConfig(
            name="Listingify 反馈群",
            group_id="cid-listingify",
            project="listingify",
            repo="/ABS/PATH/TO/Listingify",
            send=True,
            ready_message="我在了",
        )

        args = worker.args_for_group(base, {}, group)

        self.assertEqual(args.event_key, "user_im_message_receive_group")
        self.assertEqual(args.group, "cid-listingify")
        self.assertTrue(args.send)
        self.assertEqual(args.allow_conversation, {"cid-listingify"})
        self.assertEqual(args.admit_conversation, {"cid-listingify"})
        self.assertEqual(args.project, "listingify")
        self.assertEqual(args.ready_message, "我在了")

    def test_args_for_group_applies_reply_skill_to_prompt_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            skill_path = self.make_reply_skill(tmp)
            audit_dir = pathlib.Path(tmp) / "visible-sessions"
            base = self.parse(["--config", "config.json"])
            group = worker.GroupConfig(
                name="赢在AI",
                group_id="cid-winyai",
                reply_skill="laoxing-test",
                reply_skill_path=str(skill_path),
                codex_audit_session_dir=str(audit_dir),
            )

            args = worker.args_for_group(base, {}, group)

            self.assertEqual(args.reply_skill, "laoxing-test")
            self.assertEqual(args.codex_audit_session_dir, str(audit_dir))
            self.assertIn("老邢测试 Skill", worker.build_effective_system_prompt(args))
            self.assertIn("追证据", worker.build_effective_system_prompt(args))

    def test_args_for_group_supports_mention_trigger_and_history_context(self):
        base = self.parse(["--config", "config.json"])
        group = worker.GroupConfig(
            name="赢在AI",
            group_id="cid-winyai",
            trigger="mention",
            history_context=True,
            history_context_limit=5,
            history_context_minutes=30,
        )

        args = worker.args_for_group(base, {}, group)

        self.assertEqual(args.trigger, "mention")
        self.assertEqual(args.event_key, "user_im_message_receive_at")
        self.assertTrue(args.history_context)
        self.assertEqual(args.history_context_limit, 5)
        self.assertEqual(args.history_context_minutes, 30)
        self.assertEqual(worker.build_consume_command(args), [
            "dws",
            "event", "consume", "user_im_message_receive_at",
            "--flatten", "-f", "ndjson",
        ])

    def test_consumer_args_for_mention_approval_adds_group_confirmation_listener(self):
        base = self.parse(["--config", "config.json"])
        group = worker.GroupConfig(
            name="SHEIN API自动上新测试群",
            group_id="cid-listingify",
            trigger="mention",
            query="listingify",
            filter_json='{"at":true}',
            ready_message="我在了",
            code_write_policy="approval",
        )

        consumers = worker.consumer_args_for_group(base, {}, group)

        self.assertEqual([role for role, _args in consumers], ["main", "approval"])
        main_args = consumers[0][1]
        approval_args = consumers[1][1]
        self.assertEqual(main_args.event_key, "user_im_message_receive_at")
        self.assertFalse(main_args.approval_only)
        self.assertEqual(approval_args.event_key, "user_im_message_receive_group")
        self.assertEqual(approval_args.group, "cid-listingify")
        self.assertTrue(approval_args.approval_only)
        self.assertEqual(approval_args.query, "")
        self.assertEqual(approval_args.filter_json, "")
        self.assertEqual(approval_args.ready_message, "")
        self.assertEqual(approval_args.reply_target, "conversation")
        self.assertEqual(worker.build_consume_command(approval_args), [
            "dws",
            "event", "consume", "user_im_message_receive_group",
            "--flatten", "-f", "ndjson",
            "--group", "cid-listingify",
        ])

    def test_load_agent_config_parses_mention_trigger_and_history_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "config.json"
            path.write_text(json.dumps({
                "defaults": {"mode": "draft"},
                "groups": [
                    {
                        "name": "赢在AI",
                        "group": "cid-winyai",
                        "trigger": "mention_only",
                        "history_context": {
                            "enabled": True,
                            "limit": 20,
                        },
                    },
                ],
            }), encoding="utf-8")

            _defaults, groups = worker.load_agent_config(path)

            self.assertEqual(groups[0].trigger, "mention")
            self.assertTrue(groups[0].history_context)
            self.assertEqual(groups[0].history_context_limit, 20)

    def test_load_agent_config_parses_approval_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "config.json"
            path.write_text(json.dumps({
                "groups": [
                    {
                        "name": "SHEIN API自动上新测试群",
                        "group": "cid-listingify",
                        "project": "listingify",
                        "repo": "/ABS/PATH/TO/Listingify",
                        "code_write_policy": "approval_required",
                        "approval_phrases": ["OK", "好的", "继续", "确认", "可以"],
                        "approval_ttl_minutes": 60,
                    },
                ],
            }), encoding="utf-8")

            _defaults, groups = worker.load_agent_config(path)

            self.assertEqual(groups[0].code_write_policy, "approval")
            self.assertEqual(groups[0].approval_phrases, ("OK", "好的", "继续", "确认", "可以"))
            self.assertEqual(groups[0].approval_ttl_minutes, 60)

    def test_approval_confirmation_matches_short_safe_phrases(self):
        args = self.parse(["--code-write-policy", "approval"])

        self.assertTrue(worker.is_approval_confirmation("@Codex OK", args))
        self.assertTrue(worker.is_approval_confirmation("好的，继续", args))
        self.assertFalse(worker.is_approval_confirmation("不可以，先别改", args))
        self.assertFalse(worker.is_approval_confirmation("可以先看看，不要改代码", args))

    def test_build_history_context_command_lists_older_group_messages(self):
        args = self.parse([
            "--dws-bin", "/usr/local/bin/dws",
            "--profile", "corp:user",
            "--history-context",
            "--history-context-limit", "5",
        ])
        event = {
            "conversation_id": "cid-1",
            "event_time": 1767196800000,
        }

        self.assertEqual(worker.build_history_context_command(event, args), [
            "/usr/local/bin/dws",
            "--profile", "corp:user",
            "chat", "message", "list",
            "--group", "cid-1",
            "--time", "2026-01-01 00:00:00",
            "--direction", "older",
            "--limit", "5",
            "--format", "json",
        ])

    def test_build_ready_message_command_targets_configured_group(self):
        args = self.parse([
            "--group", "cid-1",
            "--profile", "corp:user",
            "--ready-message", "我在了",
        ])

        cmd = worker.build_ready_message_command(args)

        self.assertEqual(cmd[:7], [
            "dws", "--profile", "corp:user",
            "chat", "message", "send",
            "--group",
        ])
        self.assertEqual(cmd[7], "cid-1")
        self.assertIn("--text", cmd)
        self.assertEqual(cmd[cmd.index("--text") + 1], "我在了")
        self.assertIn("--uuid", cmd)

    def test_duplicate_uuid_payload_treats_idempotent_send_as_success(self):
        payload = worker.duplicate_uuid_payload(
            "Request is repeated with uuid 'ai-autoreply-ready-abc123'."
        )

        self.assertEqual(payload, {
            "duplicate_uuid": True,
            "uuid": "ai-autoreply-ready-abc123",
        })

    def test_process_event_skips_not_admitted_conversation_before_ai(self):
        args = self.parse([
            "--event-key", "user_im_message_receive_group",
            "--admit-conversation", "cid-allowed",
        ])
        event = {
            "type": "user_im_message_receive_group",
            "event_id": "event-3",
            "message_id": "msg-3",
            "conversation_id": "cid-other",
            "content": "页面报错",
        }
        with tempfile.TemporaryDirectory() as tmp:
            args.state_file = pathlib.Path(tmp) / "seen.jsonl"
            previous = worker.generate_reply
            worker.generate_reply = lambda _event, _args: self.fail("AI should not be called")
            try:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    worker.process_event(event, set(), args)
            finally:
                worker.generate_reply = previous

            record = json_loads_line(buf.getvalue())
            self.assertEqual(record["action"], "skipped")
            self.assertEqual(record["reason"], "conversation_not_admitted")
            self.assertEqual(worker.load_seen(args.state_file), set())

    def test_non_admitted_mention_does_not_block_matching_group_consumer(self):
        wrong_args = self.parse([
            "--event-key", "user_im_message_receive_at",
            "--group", "cid-a",
            "--admit-conversation", "cid-a",
        ])
        right_args = self.parse([
            "--event-key", "user_im_message_receive_at",
            "--group", "cid-b",
            "--admit-conversation", "cid-b",
        ])
        event = {
            "type": "user_im_message_receive_at",
            "event_id": "event-mention",
            "message_id": "msg-mention",
            "conversation_id": "cid-b",
            "sender": "张三",
            "sender_open_dingtalk_id": "open-1",
            "content": "@我 看下这个",
        }
        seen = set()

        with tempfile.TemporaryDirectory() as tmp:
            state_file = pathlib.Path(tmp) / "seen.jsonl"
            wrong_args.state_file = state_file
            right_args.state_file = state_file
            previous = worker.generate_reply
            worker.generate_reply = lambda _event, _args: "我看一下。"
            try:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    worker.process_event(event, seen, wrong_args)
                    worker.process_event(event, seen, right_args)
            finally:
                worker.generate_reply = previous

            records = [json.loads(line) for line in buf.getvalue().strip().splitlines()]
            self.assertEqual(records[0]["reason"], "conversation_not_admitted")
            self.assertEqual(records[1]["action"], "draft")
            self.assertEqual(worker.load_seen(state_file), {"event-mention"})

    def test_heuristic_classifier_maps_question_without_code(self):
        args = self.parse(["--project", "crawshrimp", "--repo", "/tmp/repo"])
        event = {"content": "这个鞋包图片怎么重新生成？", "message_id": "msg-q"}

        result = worker.classify_feedback(event, args)

        self.assertEqual(result.type, "support")
        self.assertFalse(result.needs_code)
        self.assertEqual(result.risk, "low")

    def test_heuristic_classifier_maps_feature_to_codex_task(self):
        args = self.parse([
            "--project", "listingify",
            "--repo", "/ABS/PATH/TO/Listingify",
            "--test-command", "npm test",
        ])
        event = {
            "content": "希望 Listingify 上新草稿支持批量选择尺码表",
            "event_id": "evt-feature",
            "message_id": "msg-feature",
            "conversation_id": "cid-listingify",
        }

        result = worker.classify_feedback(event, args)

        self.assertEqual(result.type, "feature")
        self.assertTrue(result.needs_code)
        self.assertEqual(result.risk, "medium")
        self.assertEqual(result.codex_task["repo"], "/ABS/PATH/TO/Listingify")
        self.assertEqual(result.codex_task["test_commands"], ["npm test"])

    def test_code_keyword_gate_blocks_unrelated_group_bug_from_repo(self):
        args = self.parse([
            "--project", "crawshrimp",
            "--repo", "/ABS/PATH/TO/crawshrimp",
            "--code-keyword", "抓虾,crawshrimp",
        ])

        unrelated = worker.classify_feedback({"content": "这个页面报错了"}, args)
        related = worker.classify_feedback({"content": "抓虾生成视频报错了"}, args)

        self.assertEqual(unrelated.type, "bug")
        self.assertFalse(unrelated.needs_code)
        self.assertEqual(related.type, "bug")
        self.assertTrue(related.needs_code)
        self.assertEqual(related.codex_task["repo"], "/ABS/PATH/TO/crawshrimp")

    def test_process_event_feature_in_draft_creates_codex_draft_without_ai_reply(self):
        args = self.parse([
            "--event-key", "user_im_message_receive_group",
            "--group", "cid-1",
            "--project", "listingify",
            "--repo", "/ABS/PATH/TO/Listingify",
        ])
        event = {
            "type": "user_im_message_receive_group",
            "event_id": "event-feature",
            "message_id": "msg-feature",
            "conversation_id": "cid-1",
            "sender_open_dingtalk_id": "open-1",
            "content": "希望上新草稿支持批量选择尺码表",
        }
        with tempfile.TemporaryDirectory() as tmp:
            args.state_file = pathlib.Path(tmp) / "seen.jsonl"
            previous = worker.generate_reply
            worker.generate_reply = lambda _event, _args: self.fail("dialogue AI should not be called for code tasks")
            try:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    worker.process_event(event, set(), args)
            finally:
                worker.generate_reply = previous

            record = json_loads_line(buf.getvalue())
            self.assertEqual(record["action"], "codex_draft")
            self.assertEqual(record["classification"]["type"], "feature")
            self.assertIn("Codex", record["codex_prompt"])
            self.assertEqual(worker.load_seen(args.state_file), {"event-feature"})

    def test_guarded_code_task_writes_task_bundle_but_does_not_execute_without_flag(self):
        args = self.parse([
            "--event-key", "user_im_message_receive_group",
            "--group", "cid-1",
            "--mode", "guarded",
            "--project", "crawshrimp",
        ])
        event = {
            "type": "user_im_message_receive_group",
            "event_id": "event-bug",
            "message_id": "msg-bug",
            "conversation_id": "cid-1",
            "sender_open_dingtalk_id": "open-1",
            "content": "抓虾生成视频报错，提示 exception",
        }
        with tempfile.TemporaryDirectory() as tmp:
            args.repo = tmp
            args.codex_log_dir = str(pathlib.Path(tmp) / "codex")
            args.state_file = pathlib.Path(tmp) / "seen.jsonl"
            buf = io.StringIO()
            with redirect_stdout(buf):
                worker.process_event(event, set(), args)

            record = json_loads_line(buf.getvalue())
            self.assertEqual(record["action"], "codex_blocked")
            self.assertEqual(record["codex_result"]["reason"], "codex_execute_disabled")
            self.assertTrue(pathlib.Path(record["codex_result"]["task_dir"], "prompt.md").exists())

    def test_autofix_requires_low_risk_bug_repo_tests_and_execute(self):
        args = self.parse([
            "--mode", "autofix",
            "--codex-execute",
            "--test-command", "python3 -m pytest",
        ])
        with tempfile.TemporaryDirectory() as tmp:
            args.repo = tmp
            event = {"content": "明确 BUG：保存失败报错", "message_id": "msg-bug"}
            classification = worker.classify_feedback(event, args)

            ok, reason = worker.is_autofix_eligible(classification, args)

            self.assertTrue(ok, reason)

    def test_autofix_blocks_high_risk_or_missing_tests(self):
        args = self.parse(["--mode", "autofix", "--codex-execute"])
        with tempfile.TemporaryDirectory() as tmp:
            args.repo = tmp
            event = {"content": "明确 BUG：生产数据库删除报错", "message_id": "msg-risk"}
            classification = worker.classify_feedback(event, args)

            ok, reason = worker.is_autofix_eligible(classification, args)

            self.assertFalse(ok)
            self.assertEqual(reason, "risk_not_low")

    def test_build_codex_command_uses_readonly_for_guarded_and_workspace_for_autofix(self):
        with tempfile.TemporaryDirectory() as tmp:
            guarded = self.parse(["--mode", "guarded", "--repo", tmp])
            classification = worker.classify_feedback({"content": "页面报错"}, guarded)
            guarded_cmd = worker.build_codex_command(guarded, classification, pathlib.Path(tmp))
            self.assertIn("read-only", guarded_cmd)

            autofix = self.parse(["--mode", "autofix", "--repo", tmp])
            classification = worker.classify_feedback({"content": "页面报错"}, autofix)
            autofix_cmd = worker.build_codex_command(autofix, classification, pathlib.Path(tmp))
            self.assertIn("workspace-write", autofix_cmd)

    def test_extract_thread_id_from_codex_jsonl(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "out.jsonl"
            path.write_text(
                '{"type":"thread.started","thread_id":"019fb69d-ce05-7881-8f96-1569517cdc87"}\n',
                encoding="utf-8",
            )

            self.assertEqual(
                worker.extract_thread_id_from_jsonl(path),
                "019fb69d-ce05-7881-8f96-1569517cdc87",
            )

    def test_build_codex_session_prompt_includes_reply_skill(self):
        with tempfile.TemporaryDirectory() as tmp:
            skill_path = self.make_reply_skill(tmp)
            args = self.parse([
                "--reply-engine", "codex",
                "--reply-skill", "laoxing-test",
                "--reply-skill-path", str(skill_path),
                "--group-name", "赢在AI",
            ])
            classification = worker.classify_feedback({"content": "有结果了吗"}, args)
            prompt = worker.build_codex_session_prompt(
                {
                    "sender": "张三",
                    "conversation_id": "cid-1",
                    "message_id": "msg-1",
                    "content": "有结果了吗",
                },
                classification,
                args,
            )

            self.assertIn("已加载回复 Skill: laoxing-test", prompt)
            self.assertIn("老邢测试 Skill", prompt)
            self.assertIn("最终输出只能是要发送到钉钉群里的中文正文", prompt)

    def test_codex_audit_session_dir_uses_readable_sanitized_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = self.parse([
                "--codex-audit-session-dir", str(pathlib.Path(tmp) / "audit"),
                "--project", "listingify/api",
                "--group-name", "SHEIN API自动上新测试群",
            ])
            event = {"conversation_id": "cid/example/with/slash=="}

            audit_dir = worker.codex_audit_session_dir_for_event(event, args)

            self.assertIsNotNone(audit_dir)
            assert audit_dir is not None
            self.assertEqual(audit_dir.parent, pathlib.Path(tmp) / "audit")
            self.assertIn("listingify_api", audit_dir.name)
            self.assertIn("SHEIN_API自动上新测试群", audit_dir.name)
            self.assertIn("cid_example_with_slash==", audit_dir.name)

    def test_generate_codex_session_reply_resumes_when_approval_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = self.parse([
                "--reply-engine", "codex",
                "--code-write-policy", "approval",
                "--codex-session-dir", str(pathlib.Path(tmp) / "sessions"),
            ])
            event = {
                "event_id": "event-resume",
                "message_id": "msg-resume",
                "conversation_id": "cid-resume",
                "content": "有结果了吗",
            }
            classification = worker.classify_feedback(event, args)
            session_dir = worker.codex_session_dir_for_event(event, args)
            session_dir.mkdir(parents=True)
            worker.save_codex_session_id(event, args, "thread-old")
            captured = {}
            previous_run = worker.subprocess.run

            def fake_run(cmd, input, stdout, stderr, text, timeout, check):
                captured["cmd"] = cmd
                (session_dir / "last_message.md").write_text("可以，我接着上次的上下文看。", encoding="utf-8")
                return worker.subprocess.CompletedProcess(args=cmd, returncode=0)

            worker.subprocess.run = fake_run
            try:
                _reply, result = worker.generate_codex_session_reply(event, classification, args)
            finally:
                worker.subprocess.run = previous_run

            self.assertIn("resume", captured["cmd"])
            self.assertIn("thread-old", captured["cmd"])
            self.assertTrue(result["resumed"])
            self.assertEqual(worker.load_codex_session_id(event, args), "thread-old")

    def test_generate_codex_session_reply_writes_audit_mirror(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = self.parse([
                "--reply-engine", "codex",
                "--codex-session-dir", str(pathlib.Path(tmp) / "sessions"),
                "--codex-audit-session-dir", str(pathlib.Path(tmp) / "audit"),
                "--project", "listingify",
                "--group-name", "SHEIN API自动上新测试群",
            ])
            event = {
                "event_id": "event-audit",
                "message_id": "msg-audit",
                "conversation_id": "cid-audit",
                "content": "这个接口是不是挂了",
            }
            classification = worker.classify_feedback(event, args)
            session_dir = worker.codex_session_dir_for_event(event, args)
            previous_run = worker.subprocess.run

            def fake_run(cmd, input, stdout, stderr, text, timeout, check):
                stdout.write('{"type":"thread.started","thread_id":"thread-audit"}\n')
                stdout.flush()
                stderr.write("")
                (session_dir / "last_message.md").write_text("我先看下证据，别急着拍脑袋。", encoding="utf-8")
                return worker.subprocess.CompletedProcess(args=cmd, returncode=0)

            worker.subprocess.run = fake_run
            try:
                reply, result = worker.generate_codex_session_reply(event, classification, args)
            finally:
                worker.subprocess.run = previous_run

            audit_dir = pathlib.Path(result["audit_session_dir"])
            self.assertEqual(reply, "我先看下证据，别急着拍脑袋。")
            self.assertTrue((audit_dir / "README.md").exists())
            self.assertTrue((audit_dir / "session.json").exists())
            self.assertTrue((audit_dir / "events.jsonl").exists())
            self.assertTrue((audit_dir / "event-audit.prompt.md").exists())
            self.assertTrue((audit_dir / "event-audit.last_message.md").exists())
            self.assertTrue((audit_dir / "event-audit.codex.jsonl").exists())
            session = json.loads((audit_dir / "session.json").read_text(encoding="utf-8"))
            self.assertEqual(session["thread_id"], "thread-audit")
            self.assertEqual(session["project"], "listingify")
            self.assertEqual(session["internal_session_dir"], str(session_dir))
            index = json.loads((audit_dir / "events.jsonl").read_text(encoding="utf-8").strip())
            self.assertEqual(index["event_id"], "event-audit")
            self.assertTrue(index["copied"]["prompt"])
            self.assertTrue(index["copied"]["last_message"])

    def test_process_event_codex_reply_engine_uses_session_not_direct_ai(self):
        args = self.parse([
            "--event-key", "user_im_message_receive_group",
            "--group", "cid-1",
            "--send",
            "--reply-engine", "codex",
            "--allow-conversation", "cid-1",
        ])
        event = {
            "type": "user_im_message_receive_group",
            "event_id": "event-codex",
            "message_id": "msg-codex",
            "conversation_id": "cid-1",
            "sender_open_dingtalk_id": "open-1",
            "content": "有结果了吗",
        }
        with tempfile.TemporaryDirectory() as tmp:
            args.state_file = pathlib.Path(tmp) / "seen.jsonl"
            previous_reply = worker.generate_reply
            previous_codex = worker.generate_codex_session_reply
            previous_send = worker.send_reply
            worker.generate_reply = lambda _event, _args: self.fail("direct AI reply should not be called")
            worker.generate_codex_session_reply = (
                lambda _event, _classification, _args: ("Codex session 回复", {"thread_id": "thread-1"})
            )
            worker.send_reply = lambda _event, _target, _reply, _args, purpose="reply": {"success": True}
            try:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    worker.process_event(event, set(), args)
            finally:
                worker.generate_reply = previous_reply
                worker.generate_codex_session_reply = previous_codex
                worker.send_reply = previous_send

            record = json_loads_line(buf.getvalue())
            self.assertEqual(record["action"], "codex_session_sent")
            self.assertEqual(record["reply"], "Codex session 回复")
            self.assertEqual(record["codex_session"]["thread_id"], "thread-1")
            self.assertEqual(worker.load_seen(args.state_file), {"event-codex"})

    def test_process_event_code_task_can_ack_then_send_proactive_followup(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = self.parse([
                "--event-key", "user_im_message_receive_at",
                "--group", "cid-listingify",
                "--send",
                "--reply-engine", "codex",
                "--project", "listingify",
                "--repo", tmp,
                "--codex-execute",
                "--code-write-policy", "approval",
                "--codex-proactive-followup",
                "--approval-dir", str(pathlib.Path(tmp) / "approvals"),
            ])
            args.state_file = pathlib.Path(tmp) / "seen.jsonl"
            event = {
                "type": "user_im_message_receive_at",
                "event_id": "event-proactive",
                "message_id": "msg-proactive",
                "conversation_id": "cid-listingify",
                "sender_open_dingtalk_id": "open-1",
                "content": "@我 Listingify 色块图缺失报错，帮忙排查",
            }
            sent = []
            previous_codex = worker.generate_codex_session_reply
            previous_send = worker.send_reply
            previous_bridge = worker.run_codex_bridge
            worker.generate_codex_session_reply = (
                lambda _event, _classification, _args: ("收到，我先只读排查。", {"thread_id": "thread-1"})
            )
            worker.send_reply = lambda _event, _target, _reply, _args, purpose="reply": sent.append({
                "purpose": purpose,
                "reply": _reply,
            }) or {"success": True}
            worker.run_codex_bridge = lambda _event, _classification, _args: worker.CodexBridgeResult(
                task_id="task-proactive",
                status="completed",
                task_dir=tmp,
                delivery_message="排查完了。更像色块图转存读回为空，先确认样本后再改代码。",
                executed=True,
                returncode=0,
                last_message="FIXED? no, read-only diagnosis.",
            )
            try:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    worker.process_event(event, set(), args)
            finally:
                worker.generate_codex_session_reply = previous_codex
                worker.send_reply = previous_send
                worker.run_codex_bridge = previous_bridge

            record = json_loads_line(buf.getvalue())
            self.assertEqual(record["action"], "code_proactive_followup_sent")
            self.assertEqual([item["purpose"] for item in sent], ["proactive_ack", "proactive_followup"])
            self.assertIn("只读排查", sent[0]["reply"])
            self.assertIn("如果同意我实际修改代码", sent[1]["reply"])
            self.assertTrue(worker.pending_approval_path(args).exists())
            self.assertEqual(worker.load_seen(args.state_file), {"event-proactive"})

    def test_code_task_requests_approval_before_writing(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = self.parse([
                "--event-key", "user_im_message_receive_at",
                "--group", "cid-listingify",
                "--send",
                "--reply-engine", "codex",
                "--project", "listingify",
                "--repo", tmp,
                "--code-write-policy", "approval",
                "--approval-dir", str(pathlib.Path(tmp) / "approvals"),
            ])
            args.state_file = pathlib.Path(tmp) / "seen.jsonl"
            event = {
                "type": "user_im_message_receive_at",
                "event_id": "event-approval-request",
                "message_id": "msg-approval-request",
                "conversation_id": "cid-listingify",
                "sender_open_dingtalk_id": "open-1",
                "content": "@我 Listingify 保存失败报错，帮忙修复",
            }
            previous_codex = worker.generate_codex_session_reply
            previous_send = worker.send_reply
            previous_run = worker.run_approved_code_change
            worker.generate_codex_session_reply = (
                lambda _event, _classification, _args: ("方案：先定位保存失败，再做最小修复。", {"thread_id": "thread-1"})
            )
            worker.send_reply = lambda _event, _target, _reply, _args, purpose="reply": {
                "success": True,
                "purpose": purpose,
                "reply": _reply,
            }
            worker.run_approved_code_change = lambda _pending, _event, _args: self.fail("write path must wait for approval")
            try:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    worker.process_event(event, set(), args)
            finally:
                worker.generate_codex_session_reply = previous_codex
                worker.send_reply = previous_send
                worker.run_approved_code_change = previous_run

            record = json_loads_line(buf.getvalue())
            self.assertEqual(record["action"], "code_approval_requested")
            self.assertIn("实际修改代码", record["reply"])
            self.assertIn("请在群里回复", record["reply"])
            self.assertTrue(pathlib.Path(record["pending_approval"]["path"]).exists())
            self.assertEqual(worker.load_seen(args.state_file), {"event-approval-request"})

    def test_approval_confirmation_runs_approved_code_change_and_clears_pending(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = self.parse([
                "--event-key", "user_im_message_receive_group",
                "--group", "cid-listingify",
                "--send",
                "--reply-engine", "codex",
                "--project", "listingify",
                "--repo", tmp,
                "--codex-execute",
                "--code-write-policy", "approval",
                "--approval-dir", str(pathlib.Path(tmp) / "approvals"),
            ])
            args.state_file = pathlib.Path(tmp) / "seen.jsonl"
            pending = {
                "approval_id": "task-1",
                "created_at": int(worker.time.time()),
                "conversation_id": "cid-listingify",
                "group_name": "SHEIN API自动上新测试群",
                "project": "listingify",
                "repo": tmp,
                "request_message": "方案：做最小修复。",
                "event": {
                    "event_id": "event-original",
                    "message_id": "msg-original",
                    "conversation_id": "cid-listingify",
                    "content": "保存失败，帮忙修复",
                },
                "classification": {
                    "project": "listingify",
                    "type": "bug",
                    "confidence": 0.88,
                    "needs_code": True,
                    "risk": "low",
                    "reply_draft": "收到，按 BUG 反馈进入排查。",
                    "codex_task": {"repo": tmp},
                    "source": "heuristic",
                },
            }
            worker.write_json_file(worker.pending_approval_path(args), pending)
            event = {
                "type": "user_im_message_receive_group",
                "event_id": "event-confirm",
                "message_id": "msg-confirm",
                "conversation_id": "cid-listingify",
                "sender": "老板",
                "sender_open_dingtalk_id": "open-1",
                "content": "OK",
            }
            sent: list[dict[str, str]] = []
            previous_send = worker.send_reply
            previous_run = worker.run_approved_code_change
            worker.send_reply = lambda _event, _target, _reply, _args, purpose="reply": sent.append({
                "purpose": purpose,
                "reply": _reply,
            }) or {"success": True}
            worker.run_approved_code_change = lambda _pending, _approval_event, _args: worker.CodexBridgeResult(
                task_id="approved-task-1",
                status="completed",
                task_dir=tmp,
                delivery_message="已按群内确认完成本地代码处理。当前状态：本地已改，未提交、未推送、未发布。",
                executed=True,
                returncode=0,
                last_message="FIXED: 已修复并通过测试。",
            )
            try:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    worker.process_event(event, set(), args)
            finally:
                worker.send_reply = previous_send
                worker.run_approved_code_change = previous_run

            record = json_loads_line(buf.getvalue())
            self.assertEqual(record["action"], "code_approval_executed")
            self.assertEqual(record["codex_result"]["status"], "completed")
            self.assertEqual([item["purpose"] for item in sent], ["approval_ack", "approval_delivery"])
            self.assertFalse(worker.pending_approval_path(args).exists())
            self.assertEqual(worker.load_seen(args.state_file), {"event-confirm"})

    def test_approval_only_listener_ignores_non_confirmation_group_chat(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = self.parse([
                "--event-key", "user_im_message_receive_group",
                "--group", "cid-listingify",
                "--send",
                "--reply-engine", "codex",
                "--code-write-policy", "approval",
                "--approval-dir", str(pathlib.Path(tmp) / "approvals"),
            ])
            args.approval_only = True
            args.state_file = pathlib.Path(tmp) / "seen.jsonl"
            event = {
                "type": "user_im_message_receive_group",
                "event_id": "event-chatter",
                "message_id": "msg-chatter",
                "conversation_id": "cid-listingify",
                "sender": "同事",
                "sender_open_dingtalk_id": "open-2",
                "content": "大家先看下这个页面",
            }
            previous_classify = worker.classify_feedback
            previous_send = worker.send_reply
            previous_run = worker.run_approved_code_change
            worker.classify_feedback = lambda _event, _args: self.fail("approval-only listener must not classify chat")
            worker.send_reply = lambda *_args, **_kwargs: self.fail("approval-only listener must not send chat replies")
            worker.run_approved_code_change = lambda _pending, _event, _args: self.fail("approval-only listener must not run code")
            try:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    worker.process_event(event, set(), args)
            finally:
                worker.classify_feedback = previous_classify
                worker.send_reply = previous_send
                worker.run_approved_code_change = previous_run

            record = json_loads_line(buf.getvalue())
            self.assertEqual(record["action"], "skipped")
            self.assertEqual(record["reason"], "approval_only_no_confirmation")
            self.assertEqual(worker.load_seen(args.state_file), set())

    def test_build_approved_codex_command_uses_workspace_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = self.parse(["--repo", tmp, "--code-write-policy", "approval"])

            cmd = worker.build_approved_codex_command(args, pathlib.Path(tmp))

            self.assertIn("workspace-write", cmd)
            self.assertIn("--cd", cmd)
            self.assertEqual(cmd[cmd.index("--cd") + 1], tmp)


def json_loads_line(value):
    import json

    return json.loads(value.strip().splitlines()[0])


if __name__ == "__main__":
    unittest.main()
