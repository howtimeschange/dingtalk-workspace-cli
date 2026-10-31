"""Offline regression tests: no real messages, credentials, or services."""
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from ai_autoreply_runtime import ProcessLock, Scheduler, Store


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "runtime.sqlite3"
        self.store = Store(self.path)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_sqlite_durability_permissions_and_unique_immutable_event(self):
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
        self.assertEqual(self.store._db.execute("PRAGMA synchronous").fetchone()[0], 2)
        self.assertTrue(self.store.enqueue("e", "lane", "route", {"send": True}, {"text": "hello"}))
        self.assertFalse(self.store.enqueue("e", "lane", "route", {"send": True}, {"text": "hello"}))
        with self.assertRaises(ValueError):
            self.store.enqueue("e", "lane", "route", {"send": True}, {"text": "changed"})
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(self.store.get_job("e").event, {"text": "hello"})

    def test_reopen_requeues_job_but_unknown_step_is_not_reexecuted(self):
        self.store.enqueue("e", "lane", "route", "v1", {"id": "e"})
        job = self.store.claim_job("e")
        self.assertEqual(self.store.begin_step("code-e", "code", {"repo": "fake"}, job.id).status, "started")
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(self.store.get_job("e").status, "queued")
        self.assertEqual(self.store.begin_step("code-e", "code", {"repo": "fake"}, job.id).status, "unknown")
        with self.assertRaises(ValueError):
            self.store.finish_step("code-e", {"ok": True})
        executions = []

        def handler(event):
            step = self.store.begin_step("code-e", "code", {"repo": "fake"}, job.id)
            if step.status == "started":
                executions.append(event)

        scheduler = Scheduler(self.store)
        scheduler.register("route", "v1", handler)
        scheduler.start()
        self.assertTrue(scheduler.wait_idle(3))
        scheduler.stop()
        self.assertEqual(executions, [])

    def test_completed_checkpoint_returns_result_and_refuses_payload_collision(self):
        self.assertEqual(self.store.begin_step("s", "inspect", {"a": 1}).status, "started")
        self.store.finish_step("s", {"answer": 42})
        step = self.store.begin_step("s", "inspect", {"a": 1})
        self.assertEqual(step.status, "completed")
        self.assertEqual(step["result"], {"answer": 42})
        with self.assertRaises(ValueError):
            self.store.begin_step("s", "inspect", {"a": 2})
        self.store.begin_step("duplicate", "code", {})
        self.assertEqual(self.store.begin_step("duplicate", "code", {}).status, "unknown")

    def test_abrupt_process_exit_preserves_claims_and_recovers_conservatively(self):
        crash_path = Path(self.temp.name) / "crash.sqlite3"
        program = """
import os, sys
from ai_autoreply_runtime import Store
store = Store(sys.argv[1])
store.enqueue('e', 'lane', 'r', 'v1', {})
job = store.claim_job('e')
store.begin_step('code', 'code', {}, job.id)
store.enqueue_outbox('u', 'p', {'group': 'fixture'}, 'hello', 'send')
store.claim_outbox('u')
os._exit(23)
"""
        completed = subprocess.run([sys.executable, "-B", "-c", program, str(crash_path)],
                                   cwd=str(Path(__file__).parent), capture_output=True, text=True)
        self.assertEqual(completed.returncode, 23, completed.stderr)
        with Store(crash_path) as recovered:
            self.assertEqual(recovered.get_job("e").status, "queued")
            self.assertEqual(recovered.get_step("code").status, "unknown")
            self.assertEqual(recovered.get_outbox("u").status, "unknown")
            self.assertIsNone(recovered.claim_outbox("u"))

    def test_outbox_sending_crash_unknown_and_sent_never_reclaim(self):
        args = ("u", "profile-a", {"group": "fake"}, "hello", "send")
        self.assertTrue(self.store.enqueue_outbox(*args))
        self.assertFalse(self.store.enqueue_outbox(*args))
        with self.assertRaises(ValueError):
            self.store.enqueue_outbox("u", "profile-b", {}, "hello", "send")
        self.assertEqual(self.store.claim_outbox("u").status, "sending")
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(self.store.get_outbox("u").status, "unknown")
        self.assertIsNone(self.store.claim_outbox("u"))
        self.store.enqueue_outbox("ok", "p", {"group": "g"}, "sent", "send")
        self.store.claim_outbox("ok")
        self.store.finish_outbox("ok", {"message_id": "confirmed"})
        self.assertIsNone(self.store.claim_outbox("ok"))
        self.assertEqual(self.store.get_outbox("ok").result, {"message_id": "confirmed"})
        self.store.enqueue_outbox("bad", "p", {}, "failed", "send")
        self.store.claim_outbox("bad")
        self.store.mark_outbox_unknown("bad", "timeout")
        self.assertIsNone(self.store.claim_outbox("bad"))
        self.assertEqual(len(self.store.list_outbox("unknown")), 2)

    def test_lanes_are_serial_cross_lane_parallel_and_bounded(self):
        release = threading.Event()
        two_active = threading.Event()
        guard = threading.Lock()
        active = set()
        maximum = 0
        order = []
        conflicts = []

        def handler(event):
            nonlocal maximum
            lane = event["lane"]
            with guard:
                if lane in active:
                    conflicts.append(lane)
                active.add(lane)
                maximum = max(maximum, len(active))
                order.append(event["id"])
                if len(active) == 2:
                    two_active.set()
            if not release.wait(3):
                raise RuntimeError("test barrier timed out")
            with guard:
                active.remove(lane)

        scheduler = Scheduler(self.store, max_workers=2)
        scheduler.register("r", {"v": 1}, handler)
        for key, lane in (("a1", "a"), ("a2", "a"), ("b1", "b"), ("c1", "c")):
            scheduler.submit(key, lane, "r", {"v": 1}, {"id": key, "lane": lane})
        scheduler.start()
        try:
            self.assertTrue(two_active.wait(2), "different lanes must run concurrently")
            self.assertFalse(scheduler.wait_idle(0.01))
            self.assertNotIn("a2", order, "same lane must stay queued")
        finally:
            release.set()
            self.assertTrue(scheduler.wait_idle(3))
            scheduler.stop()
        self.assertEqual(conflicts, [])
        self.assertEqual(maximum, 2)
        self.assertLess(order.index("a1"), order.index("a2"))
        self.assertEqual({job.status for job in self.store.list_jobs()}, {"completed"})

    def test_queued_recovery_policy_drift_and_unknown_route_fail_closed(self):
        self.store.enqueue("old", "a", "r", {"send": True}, {})
        self.store.enqueue("missing", "b", "removed", {}, {})
        self.store.close()
        self.store = Store(self.path)
        calls = []
        scheduler = Scheduler(self.store)
        scheduler.register("r", {"send": False}, lambda event: calls.append(event))
        scheduler.start()
        self.assertTrue(scheduler.wait_idle(3))
        scheduler.stop()
        self.assertEqual(calls, [])
        self.assertEqual(self.store.get_job("old").error, "policy_changed")
        self.assertEqual(self.store.get_job("missing").error, "route_not_registered")

    def test_handler_failure_is_recorded_and_same_lane_continues(self):
        errors = []
        calls = []

        def handler(event):
            calls.append(event["id"])
            if event["id"] == "first":
                raise ValueError("fixture failure")

        scheduler = Scheduler(self.store, on_error=lambda job, exc: errors.append((job.event_key, str(exc))))
        scheduler.register("r", "v1", handler)
        scheduler.submit("e1", "a", "r", "v1", {"id": "first"})
        scheduler.submit("e2", "a", "r", "v1", {"id": "second"})
        scheduler.start()
        self.assertTrue(scheduler.wait_idle(3))
        scheduler.stop()
        self.assertEqual(calls, ["first", "second"])
        self.assertEqual(errors, [("e1", "fixture failure")])
        self.assertEqual(self.store.get_job("e1").status, "failed")
        self.assertEqual(self.store.get_job("e2").status, "completed")

    def test_stop_without_wait_then_restart_keeps_queued_work(self):
        entered = threading.Event()
        release = threading.Event()
        calls = []

        def handler(event):
            calls.append(event["id"])
            if event["id"] == "first":
                entered.set()
                if not release.wait(3):
                    raise RuntimeError("test barrier timed out")

        scheduler = Scheduler(self.store, max_workers=1)
        scheduler.register("r", "v1", handler)
        scheduler.submit("e1", "a", "r", "v1", {"id": "first"})
        scheduler.submit("e2", "a", "r", "v1", {"id": "second"})
        scheduler.start()
        self.assertTrue(entered.wait(2))
        scheduler.stop(wait=False)
        try:
            with self.assertRaises(RuntimeError):
                scheduler.start()
        finally:
            release.set()
            for thread in scheduler._threads:
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())
        self.assertEqual(self.store.get_job("e2").status, "queued")
        scheduler.start()
        self.assertTrue(scheduler.wait_idle(3))
        scheduler.stop()
        self.assertEqual(calls, ["first", "second"])
        self.assertEqual(self.store.get_job("e2").status, "completed")

    def test_process_lock_refuses_second_instance_and_releases(self):
        path = Path(self.temp.name) / "runtime.lock"
        with ProcessLock(path):
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
            with self.assertRaises(RuntimeError):
                ProcessLock(path).acquire()
        with ProcessLock(path):
            pass


if __name__ == "__main__":
    unittest.main()
