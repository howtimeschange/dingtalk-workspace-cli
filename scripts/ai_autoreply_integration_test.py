#!/usr/bin/env python3
"""Offline worker/runtime integration using temporary state and fake transports."""
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch


def load_file(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


worker = load_file("ai_autoreply_integration_worker", "ai_autoreply_worker.py")
runtime = load_file("ai_autoreply_integration_runtime", "ai_autoreply_runtime.py")


class WorkerRuntimeIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.database = self.root / "runtime.sqlite3"
        self.store = runtime.Store(self.database)
        self.schedulers = []
        self.args = worker.normalize_args(worker.build_parser().parse_args([
            "--profile", "fixture-profile", "--group", "fixture-group",
            "--project", "fixture-project", "--repo", str(self.root),
            "--mode", "guarded", "--code-write-policy", "approval",
            "--approval-sender", "owner-id", "--codex-execute",
            "--state-file", str(self.root / "seen.jsonl"),
            "--approval-dir", str(self.root / "approvals"),
            "--codex-log-dir", str(self.root / "codex-tasks"),
            "--codex-session-dir", str(self.root / "sessions"),
            "--codex-worktree-dir", str(self.root / "worktrees"),
        ]))
        self.args.state_file = Path(self.args.state_file)
        self.args._runtime_store = self.store
        self.logs = []
        self.log_patch = patch.object(worker, "log_json", side_effect=self.logs.append)
        self.log_patch.start()
        # Any transport/model not explicitly faked by a test is an immediate failure.
        self.network_patch = patch.object(worker.urllib.request, "urlopen", side_effect=AssertionError("network forbidden in offline integration"))
        self.network_patch.start()

    def tearDown(self):
        for scheduler in self.schedulers:
            scheduler.stop()
        self.network_patch.stop()
        self.log_patch.stop()
        self.store.close()
        self.temp.cleanup()

    def reopen(self):
        self.store.close()
        self.store = runtime.Store(self.database)
        self.args._runtime_store = self.store

    def scheduler(self, seen):
        scheduler = runtime.Scheduler(self.store, max_workers=2)
        self.schedulers.append(scheduler)
        self.args._runtime_scheduler = scheduler
        worker.register_runtime_handler(self.args, seen)
        return scheduler

    def event(self, identity, content="OK", owner="owner-id"):
        return {"event_id": "event-" + identity, "message_id": "message-" + identity,
                "conversation_id": self.args.group, "sender_open_dingtalk_id": owner,
                "content": content, "type": "user_im_message_receive_group"}

    def pending(self, identity):
        event = self.event(identity, "能不能加个批量检查功能")
        classification = worker.ClassificationResult(
            project=self.args.project, type="feature", confidence=0.99,
            needs_code=True, risk="low", reply_draft="请确认方案",
            codex_task={"repo": self.args.repo},
        )
        return worker.save_pending_approval(event, classification, self.args, "请确认方案")

    def code_result(self, pending, *_args):
        return worker.CodexBridgeResult(
            task_id=pending["approval_id"], status="completed", task_dir=str(self.root / "artifact"),
            delivery_message="本地修复完成，测试通过，待发布。", executed=True, returncode=0,
        )

    def test_completed_approved_code_delivery_timeout_reopen_new_ok_never_reexecutes_or_resends(self):
        self.args.send = True
        pending = self.pending("request")
        sends = []

        def transport(command, **_kwargs):
            sends.append(command)
            if len(sends) == 1:
                return subprocess.CompletedProcess(command, 0, '{"message_id":"ack-confirmed"}', "")
            raise subprocess.TimeoutExpired(command, 0.01)

        with patch.object(worker, "run_approved_code_change", side_effect=self.code_result) as code, \
                patch.object(worker.subprocess, "run", side_effect=transport):
            with self.assertRaises(worker.RecoveryRequired):
                worker.process_event(self.event("owner-first"), set(), self.args)
            self.assertEqual(code.call_count, 1)
            self.assertEqual(len(sends), 2)
            self.assertIn(pending["approval_id"], worker.load_pending_approvals(self.args))
            checkpoint = self.store.get_step(worker.runtime_scope(self.args) + ":approved:" + pending["approval_id"])
            self.assertEqual(checkpoint.status, "completed")
            self.assertEqual([item.status for item in self.store.list_outbox()], ["sent", "unknown"])
            old_uuids = [item.uuid for item in self.store.list_outbox()]
            self.reopen()
            # Expired approval must not hide the already completed code result or
            # erase its unresolved delivery. It grants no new code authority.
            pending["created_at"] = int(time.time()) - 86401
            worker.write_json_file(worker.pending_approval_path(self.args), {
                "version": 2, "pending": {pending["approval_id"]: pending},
            })
            with self.assertRaises(worker.RecoveryRequired):
                worker.process_event(self.event("owner-second"), set(), self.args)
            self.assertEqual(code.call_count, 1, "new OK must reuse completed code checkpoint")
            self.assertEqual(len(sends), 2, "unknown delivery must not make another transport call")
            self.assertEqual([item.uuid for item in self.store.list_outbox()], old_uuids)
            self.assertIn(pending["approval_id"], worker.load_pending_approvals(self.args))

    def test_job_replay_reuses_completed_classification_reply_and_sent_outbox(self):
        self.args.send = True
        event = self.event("dialogue", "怎么操作？")
        key = worker.runtime_event_key(event, self.args)
        policy = worker.authorization_snapshot(self.args)
        self.store.enqueue(key, worker.runtime_scope(self.args, event), worker.runtime_route(self.args), policy, event)
        self.store.claim_job(key)
        classification = worker.ClassificationResult(self.args.project, "question", 1.0, False, "low", "草稿", {})
        with patch.object(worker, "classify_feedback", return_value=classification) as classify, \
                patch.object(worker, "generate_dialogue_reply", return_value="请打开设置。") as reply, \
                patch.object(worker.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, '{"message_id":"confirmed"}', "")) as send, \
                patch.object(worker, "append_seen", side_effect=SystemExit("fixture crash after durable delivery")):
            with self.assertRaises(SystemExit):
                worker.process_event(event, set(), self.args)
            self.assertEqual((classify.call_count, reply.call_count, send.call_count), (1, 1, 1))
        self.reopen()
        self.assertEqual(self.store.get_job(key).status, "queued")
        seen = set()
        scheduler = self.scheduler(seen)
        with patch.object(worker, "classify_feedback", side_effect=AssertionError("classification replayed")), \
                patch.object(worker, "generate_dialogue_reply", side_effect=AssertionError("reply replayed")), \
                patch.object(worker.subprocess, "run", side_effect=AssertionError("sent outbox replayed")):
            scheduler.start()
            self.assertTrue(scheduler.wait_idle(3))
            scheduler.stop()
        self.assertEqual(self.store.get_job(key).status, "completed")
        self.assertIn(event["event_id"], seen)
        self.assertEqual(len(self.store.list_outbox()), 1)
        self.assertEqual(self.store.list_outbox()[0].status, "sent")

    def test_multiple_approvals_plain_ok_selects_none_explicit_id_selects_one(self):
        first, second = self.pending("first"), self.pending("second")
        with patch.object(worker, "run_approved_code_change", side_effect=self.code_result) as code:
            worker.process_event(self.event("ambiguous"), set(), self.args)
            self.assertEqual(code.call_count, 0)
            self.assertEqual(len(worker.load_pending_approvals(self.args)), 2)
            self.assertEqual(self.logs[-1]["action"], "code_approval_selection_required")
            worker.process_event(self.event("explicit", "确认 " + second["approval_id"]), set(), self.args)
            self.assertEqual(code.call_count, 1)
            self.assertEqual(code.call_args.args[0]["approval_id"], second["approval_id"])
            self.assertEqual(set(worker.load_pending_approvals(self.args)), {first["approval_id"]})

    def test_owner_ttl_and_policy_drift_reject_code(self):
        with patch.object(worker, "run_approved_code_change", side_effect=AssertionError("unapproved code executed")) as code:
            pending = self.pending("owner-check")
            worker.process_event(self.event("intruder", owner="intruder-id"), set(), self.args)
            self.assertEqual(self.logs[-1]["action"], "code_approval_ignored_unauthorized_sender")
            self.assertIn(pending["approval_id"], worker.load_pending_approvals(self.args))
            pending["created_at"] = int(time.time()) - 86401
            worker.write_json_file(worker.pending_approval_path(self.args), {"version": 2, "pending": {pending["approval_id"]: pending}})
            worker.process_event(self.event("expired"), set(), self.args)
            self.assertEqual(self.logs[-1]["action"], "code_approval_expired")
            self.assertEqual(worker.load_pending_approvals(self.args), {})
            changed = self.pending("policy-check")
            self.args.codex_execute = False
            worker.process_event(self.event("changed"), set(), self.args)
            self.assertEqual(self.logs[-1]["action"], "code_approval_policy_changed")
            self.assertIn(changed["approval_id"], worker.load_pending_approvals(self.args))
            self.assertEqual(code.call_count, 0)

    def test_main_and_approval_different_event_ids_same_message_dispatch_once(self):
        self.pending("request")
        seen, lock = set(), threading.Lock()
        scheduler = self.scheduler(seen)
        approval_args = worker.clone_namespace(self.args)
        approval_args.approval_only = True
        worker.register_runtime_handler(approval_args, seen)
        first = self.event("confirmation")
        second = dict(first, event_id="different-listener-event-id", type="user_im_message_receive_at")
        with patch.object(worker, "run_approved_code_change", side_effect=self.code_result) as code:
            # Intake both before running; dedupe must work without a seen completion.
            worker.dispatch_event(first, seen, approval_args, lock)
            worker.dispatch_event(second, seen, self.args, lock)
            self.assertEqual(len(self.store.list_jobs()), 1)
            scheduler.start()
            self.assertTrue(scheduler.wait_idle(3))
            scheduler.stop()
            self.assertEqual(code.call_count, 1)
        self.assertEqual(self.store.list_jobs()[0].status, "completed")

    def test_malformed_event_does_not_stop_pump_or_following_valid_job(self):
        self.args.mode = "observe"
        seen, lock = set(), threading.Lock()
        scheduler = self.scheduler(seen)
        valid = self.event("valid", "怎么操作？")
        stream = io.StringIO("bad json\n[]\nnull\n" + json.dumps(valid) + "\n")
        consumer = worker.ManagedConsumer("offline-fixture", self.args, SimpleNamespace(stdout=stream), threading.Event())
        with patch.object(worker.sys, "stderr", new=io.StringIO()) as errors:
            pump = worker.start_stdout_pump(consumer, seen, lock)
            pump.join(timeout=2)
            self.assertFalse(pump.is_alive())
            self.assertIn("invalid event JSON", errors.getvalue())
            self.assertIn("event must be a JSON object", errors.getvalue())
        scheduler.start()
        self.assertTrue(scheduler.wait_idle(3))
        scheduler.stop()
        self.assertEqual(len(self.store.list_jobs()), 1)
        self.assertEqual(self.store.list_jobs()[0].status, "completed")
        self.assertIn(valid["event_id"], seen)

    def test_policy_snapshot_durable_payload_excludes_api_credentials(self):
        self.args.api_key = "fixture-api-secret-do-not-persist"
        self.args.base_url = "https://fixture.invalid/?credential=secret"
        self.args.some_provider_token = "fixture-provider-secret"
        with patch.dict(worker.os.environ, {"DWS_AI_API_KEY": "fixture-env-secret"}):
            snapshot = worker.authorization_snapshot(self.args)
            text = json.dumps(snapshot)
            for forbidden in (self.args.api_key, self.args.base_url, self.args.some_provider_token, "fixture-env-secret"):
                self.assertNotIn(forbidden, text)
            event = self.event("snapshot")
            self.store.enqueue("snapshot", "lane", "route", snapshot, event)
            worker.checkpoint(self.args, "snapshot-step", "fixture", lambda: {"ok": True})
        self.reopen()
        durable = json.dumps({"job": self.store.get_job("snapshot").to_dict(), "step": self.store.get_step("snapshot-step").to_dict()})
        self.assertNotIn("fixture-api-secret", durable)
        self.assertNotIn("fixture-provider-secret", durable)
        self.assertNotIn("fixture-env-secret", durable)

    def test_shutdown_without_drain_waits_active_but_preserves_unbegun_actions(self):
        entered, release = threading.Event(), threading.Event()
        external_actions = []
        scheduler = runtime.Scheduler(self.store, max_workers=1)
        self.schedulers.append(scheduler)

        def handler(event):
            external_actions.append(event["id"])
            if event["id"] == "active":
                entered.set()
                if not release.wait(3):
                    raise RuntimeError("fixture barrier timed out")

        scheduler.register("fixture", "policy", handler)
        scheduler.submit("active", "same-lane", "fixture", "policy", {"id": "active"})
        scheduler.submit("unbegun", "same-lane", "fixture", "policy", {"id": "unbegun"})
        scheduler.start()
        self.assertTrue(entered.wait(2))
        try:
            scheduler.stop(wait=False, drain=False)
        finally:
            release.set()
        scheduler.stop(wait=True, drain=False)
        self.assertEqual(external_actions, ["active"])
        self.assertEqual(self.store.get_job("active").status, "completed")
        self.assertEqual(self.store.get_job("unbegun").status, "queued")
        scheduler.start()
        self.assertTrue(scheduler.wait_idle(3))
        scheduler.stop()
        self.assertEqual(external_actions, ["active", "unbegun"])

    def test_success_exit_without_valid_receipt_stays_unknown_and_never_resends(self):
        self.args.send = True
        target = worker.ReplyTarget("--group", self.args.group)
        for index, receipt in enumerate(("", "invalid json", "{}", "[]", "null", '{"success":false}', '{"error":"denied"}')):
            with self.subTest(receipt=receipt):
                event = self.event("receipt-" + str(index))
                before = len(self.store.list_outbox())
                with patch.object(worker.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, receipt, "")) as transport:
                    with self.assertRaises(worker.RecoveryRequired):
                        worker.send_reply(event, target, "fixture reply", self.args)
                    with self.assertRaises(worker.RecoveryRequired):
                        worker.send_reply(event, target, "fixture reply", self.args)
                    self.assertEqual(transport.call_count, 1)
                self.assertEqual(len(self.store.list_outbox()), before + 1)
                self.assertEqual(self.store.list_outbox()[-1].status, "unknown")

    def test_profiles_use_distinct_outbox_identity_for_same_event_and_target(self):
        self.args.send = True
        target = worker.ReplyTarget("--group", self.args.group)
        event = self.event("same-event")
        other = worker.clone_namespace(self.args)
        other.profile = "other-fixture-profile"
        with patch.object(worker.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, '{"message_id":"fixture-confirmed"}', "")) as transport:
            worker.send_reply(event, target, "same text", self.args)
            worker.send_reply(event, target, "same text", other)
        items = self.store.list_outbox()
        self.assertEqual(transport.call_count, 2)
        self.assertEqual(len({item.uuid for item in items}), 2)
        self.assertEqual({item.profile for item in items}, {self.args.profile, other.profile})
        self.assertEqual({item.status for item in items}, {"sent"})

    def test_resume_process_reasserts_readonly_sandbox_and_repository(self):
        event = self.event("resume", "继续")
        session = self.root / "sessions" / "fixture"
        session.mkdir(parents=True)
        command = worker.build_codex_session_command(event, self.args, "fixture-thread-id", session)
        self.assertIn("resume", command)
        self.assertIn("fixture-thread-id", command)
        self.assertEqual(command[command.index("--sandbox") + 1], "read-only")
        self.assertEqual(command[command.index("--cd") + 1], str(self.root))

    def test_autofix_uses_real_isolated_worktree_and_worker_verification_failure(self):
        real_run = subprocess.run
        for command in (["git", "init", "-q"], ["git", "config", "user.email", "fixture@example.invalid"],
                        ["git", "config", "user.name", "Offline Fixture"]):
            completed = real_run(command, cwd=self.root, capture_output=True, text=True)
            self.assertEqual(completed.returncode, 0, completed.stderr)
        subject = self.root / "subject.txt"
        subject.write_text("baseline", encoding="utf-8")
        for command in (["git", "add", "subject.txt"], ["git", "commit", "-qm", "fixture baseline"]):
            completed = real_run(command, cwd=self.root, capture_output=True, text=True)
            self.assertEqual(completed.returncode, 0, completed.stderr)
        subject.write_text("user dirty work", encoding="utf-8")
        self.args.mode = "autofix"
        self.args.code_write_policy = "auto"
        self.args.test_command = ('python3 -c "raise SystemExit(2)"',)
        classification = worker.ClassificationResult(self.args.project, "bug", 0.99, True, "low", "", {"repo": str(self.root)})
        codex_directories = []

        def transport(command, **kwargs):
            if isinstance(command, list) and command[0] == self.args.codex_bin:
                cwd = Path(command[command.index("--cd") + 1])
                codex_directories.append(cwd)
                self.assertNotEqual(cwd, self.root)
                self.assertEqual((cwd / "subject.txt").read_text(), "baseline")
                (cwd / "subject.txt").write_text("fixed", encoding="utf-8")
                Path(command[command.index("--output-last-message") + 1]).write_text("FIXED: fixture", encoding="utf-8")
                return subprocess.CompletedProcess(command, 0)
            return real_run(command, **kwargs)

        with patch.object(worker.subprocess, "run", side_effect=transport):
            result = worker.run_codex_bridge(self.event("bug", "明确 bug"), classification, self.args)
        self.assertEqual(len(codex_directories), 1)
        self.assertEqual(result.worktree_path, str(codex_directories[0]))
        self.assertEqual(subject.read_text(), "user dirty work")
        self.assertEqual((codex_directories[0] / "subject.txt").read_text(), "fixed")
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.reason, "verification_failed")
        self.assertFalse(result.verification["passed"])
        self.assertEqual(result.verification["results"][0]["returncode"], 2)
        self.assertTrue((Path(result.task_dir) / "verification.json").is_file())

    def test_ai_autofix_approval_waits_for_owner_before_any_code_bridge(self):
        self.args.mode = "autofix"
        self.args.reply_engine = "ai"
        self.args.code_write_policy = "approval"
        self.args.test_command = ("python3 -c 'pass'",)
        classification = worker.ClassificationResult(self.args.project, "bug", 0.99, True, "low", "", {"repo": self.args.repo})
        with patch.object(worker, "classify_feedback", return_value=classification), \
                patch.object(worker, "run_codex_bridge", side_effect=AssertionError("approval bypassed")) as bridge, \
                patch.object(worker, "run_approved_code_change", side_effect=self.code_result) as approved:
            worker.process_event(self.event("bug-request", "明确 bug 请修复"), set(), self.args)
            self.assertEqual(bridge.call_count, 0)
            self.assertEqual(approved.call_count, 0)
            self.assertEqual(len(worker.load_pending_approvals(self.args)), 1)
            worker.process_event(self.event("owner-confirmation"), set(), self.args)
            self.assertEqual(bridge.call_count, 0)
            self.assertEqual(approved.call_count, 1)
            self.assertEqual(worker.load_pending_approvals(self.args), {})

    def test_none_policy_blocks_autofix_before_worktree_or_codex_process(self):
        self.args.mode = "autofix"
        self.args.reply_engine = "ai"
        self.args.code_write_policy = "none"
        self.args.test_command = ("python3 -c 'pass'",)
        classification = worker.ClassificationResult(self.args.project, "bug", 0.99, True, "low", "", {"repo": self.args.repo})
        with patch.object(worker, "classify_feedback", return_value=classification), \
                patch.object(worker, "create_codex_task_worktree", side_effect=AssertionError("unauthorized worktree")) as worktree, \
                patch.object(worker.subprocess, "run", side_effect=AssertionError("unauthorized subprocess")) as process:
            worker.process_event(self.event("no-policy", "明确 bug 请修复"), set(), self.args)
            self.assertEqual((worktree.call_count, process.call_count), (0, 0))
        self.assertEqual(self.logs[-1]["codex_result"]["status"], "blocked")
        self.assertEqual(self.logs[-1]["codex_result"]["reason"], "auto_write_not_authorized")

    def test_negation_and_request_to_explain_are_not_owner_approval(self):
        pending = self.pending("request")
        self.args.approval_only = True
        with patch.object(worker, "run_approved_code_change", side_effect=AssertionError("non-consent executed code")) as code:
            for index, content in enumerate(("不确认", "确认一下原因", "不确认 " + pending["approval_id"], "确认一下原因 " + pending["approval_id"])):
                with self.subTest(content=content):
                    self.assertFalse(worker.is_approval_confirmation(content, self.args))
                    worker.process_event(self.event("not-consent-" + str(index), content), set(), self.args)
                    self.assertEqual(code.call_count, 0)
                    self.assertIn(pending["approval_id"], worker.load_pending_approvals(self.args))

    def test_signal_during_ready_notification_never_starts_queued_work(self):
        self.args.mode = "observe"
        seen = set()
        scheduler = self.scheduler(seen)
        event = self.event("waiting", "怎么操作？")
        key = worker.runtime_event_key(event, self.args)
        self.store.enqueue(key, worker.runtime_scope(self.args, event), worker.runtime_route(self.args),
                           worker.authorization_snapshot(self.args), event)
        callbacks = {}

        class FakeListener:
            def __init__(self):
                self.stdin = io.StringIO()
                self.stdout = io.StringIO()
                self.returncode = None

            def poll(self):
                return self.returncode

            def terminate(self):
                self.returncode = 0

            def wait(self, **_kwargs):
                return self.returncode or 0

            def kill(self):
                self.returncode = -9

        listener = FakeListener()

        def register_signal(number, callback):
            previous = callbacks.get(number)
            callbacks[number] = callback
            return previous

        def notify(_args):
            callbacks[worker.signal.SIGTERM](worker.signal.SIGTERM, None)

        with patch.object(worker.subprocess, "Popen", return_value=listener), \
                patch.object(worker, "start_stderr_pump"), \
                patch.object(worker, "wait_until_ready"), \
                patch.object(worker.signal, "signal", side_effect=register_signal), \
                patch.object(worker, "notify_ready", side_effect=notify), \
                patch.object(scheduler, "start", wraps=scheduler.start) as start:
            self.assertEqual(worker.run_worker(self.args), 0)
            start.assert_not_called()
        self.assertEqual(self.store.get_job(key).status, "queued")
        self.assertEqual(self.store.list_steps(), [])
        self.assertEqual(self.store.list_outbox(), [])


if __name__ == "__main__":
    unittest.main()
