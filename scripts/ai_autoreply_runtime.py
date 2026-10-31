"""Durable, single-process dispatch primitives for the DingTalk worker.

No network or business actions live here. A recovered checkpoint/outbox with an
uncertain outcome must be reconciled by its caller, never replayed automatically.
"""
from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Optional


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class _Record:
    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Job(_Record):
    id: int
    event_key: str
    lane: str
    route: str
    policy: Any
    event: dict[str, Any]
    status: str
    error: str = ""


@dataclass(frozen=True)
class Step(_Record):
    id: str
    kind: str
    payload: Any
    status: str
    result: Any = None
    job_id: Optional[int] = None


@dataclass(frozen=True)
class Outbox(_Record):
    uuid: str
    profile: str
    target: Any
    text: str
    mode: str
    payload: Any
    status: str
    result: Any = None
    error: str = ""


class ProcessLock:
    """Hold this lock for the entire Store/Scheduler lifetime."""

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser()
        self._fd: Optional[int] = None

    def acquire(self) -> "ProcessLock":
        if self._fd is not None:
            return self
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            os.fchmod(fd, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.ftruncate(fd, 0)
            os.write(fd, str(os.getpid()).encode("ascii"))
            self._fd = fd
        except BaseException:
            os.close(fd)
            raise RuntimeError(f"another worker holds runtime lock: {self.path}") from None
        return self

    def release(self) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> "ProcessLock":
        return self.acquire()

    def __exit__(self, *_args: Any) -> None:
        self.release()


class Store:
    """Short SQLite transactions; callers must own a ProcessLock before opening."""

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.path.is_symlink():
            raise ValueError("runtime database must not be a symlink")
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        os.fchmod(fd, 0o600)
        os.close(fd)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(self.path), timeout=15, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY, event_key TEXT NOT NULL UNIQUE,
                lane TEXT NOT NULL, route TEXT NOT NULL, policy TEXT NOT NULL,
                event TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
                error TEXT NOT NULL DEFAULT '', created REAL NOT NULL, updated REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS jobs_status ON jobs(status, id);
            CREATE TABLE IF NOT EXISTS steps (
                id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL,
                status TEXT NOT NULL, result TEXT, job_id INTEGER, updated REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outbox (
                uuid TEXT PRIMARY KEY, profile TEXT NOT NULL, target TEXT NOT NULL,
                text TEXT NOT NULL, mode TEXT NOT NULL, payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued', result TEXT,
                error TEXT NOT NULL DEFAULT '', created REAL NOT NULL, updated REAL NOT NULL
            );
        """)
        self.recover_running()

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    @staticmethod
    def _job(row: Optional[sqlite3.Row]) -> Optional[Job]:
        if row is None:
            return None
        return Job(row["id"], row["event_key"], row["lane"], row["route"],
                   json.loads(row["policy"]), json.loads(row["event"]), row["status"], row["error"])

    @staticmethod
    def _step(row: Optional[sqlite3.Row]) -> Optional[Step]:
        if row is None:
            return None
        return Step(row["id"], row["kind"], json.loads(row["payload"]), row["status"],
                    json.loads(row["result"]) if row["result"] is not None else None, row["job_id"])

    @staticmethod
    def _outbox(row: Optional[sqlite3.Row]) -> Optional[Outbox]:
        if row is None:
            return None
        return Outbox(row["uuid"], row["profile"], json.loads(row["target"]), row["text"],
                      row["mode"], json.loads(row["payload"]), row["status"],
                      json.loads(row["result"]) if row["result"] is not None else None, row["error"])

    def recover_running(self) -> None:
        with self._lock, self._db:
            now = time.time()
            self._db.execute("UPDATE jobs SET status='queued', updated=? WHERE status='running'", (now,))
            self._db.execute("UPDATE steps SET status='unknown', updated=? WHERE status='started'", (now,))
            self._db.execute("UPDATE outbox SET status='unknown', error='interrupted_send', updated=? WHERE status='sending'", (now,))

    def enqueue(self, event_key: str, lane: str, route: str, policy: Any, event: dict[str, Any]) -> bool:
        if not event_key or not lane or not route:
            raise ValueError("event_key, lane and route must be nonempty")
        values = (lane, route, _json(policy), _json(event))
        with self._lock, self._db:
            existing = self._db.execute("SELECT lane,route,policy,event FROM jobs WHERE event_key=?", (event_key,)).fetchone()
            if existing:
                if tuple(existing) != values:
                    raise ValueError("event key reused with a different immutable payload")
                return False
            now = time.time()
            self._db.execute("INSERT INTO jobs(event_key,lane,route,policy,event,created,updated) VALUES(?,?,?,?,?,?,?)", (event_key, *values, now, now))
            return True

    def get_job(self, identifier: int | str) -> Optional[Job]:
        column = "id" if isinstance(identifier, int) else "event_key"
        with self._lock:
            return self._job(self._db.execute(f"SELECT * FROM jobs WHERE {column}=?", (identifier,)).fetchone())

    def list_jobs(self, status: Optional[str] = None) -> list[Job]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM jobs" + (" WHERE status=?" if status else "") + " ORDER BY id", (status,) if status else ()).fetchall()
            return [self._job(row) for row in rows]  # type: ignore[misc]

    def claim_job(self, identifier: int | str) -> Optional[Job]:
        with self._lock, self._db:
            job = self.get_job(identifier)
            if job is None or job.status != "queued":
                return None
            self._db.execute("UPDATE jobs SET status='running',updated=? WHERE id=? AND status='queued'", (time.time(), job.id))
            return self.get_job(job.id)

    def _set_job(self, identifier: int | str, status: str, error: str = "") -> None:
        with self._lock, self._db:
            job = self.get_job(identifier)
            if job is None:
                raise KeyError(identifier)
            self._db.execute("UPDATE jobs SET status=?,error=?,updated=? WHERE id=?", (status, error, time.time(), job.id))

    def complete_job(self, identifier: int | str) -> None:
        self._set_job(identifier, "completed")

    def fail_job(self, identifier: int | str, error: str) -> None:
        self._set_job(identifier, "failed", error)

    def block_job(self, identifier: int | str, reason: str) -> None:
        self._set_job(identifier, "blocked", reason)

    def begin_step(self, id: str, kind: str, payload: Any, job_id: Optional[int] = None) -> Step:
        encoded = _json(payload)
        with self._lock, self._db:
            row = self._db.execute("SELECT * FROM steps WHERE id=?", (id,)).fetchone()
            if row:
                if row["kind"] != kind or row["payload"] != encoded or row["job_id"] != job_id:
                    raise ValueError("step id reused with a different immutable payload")
                # Only the first claimant may execute; duplicate begin never grants permission.
                if row["status"] == "started":
                    self._db.execute("UPDATE steps SET status='unknown',updated=? WHERE id=?", (time.time(), id))
                return self.get_step(id)  # type: ignore[return-value]
            self._db.execute("INSERT INTO steps(id,kind,payload,status,job_id,updated) VALUES(?,?,?,'started',?,?)", (id, kind, encoded, job_id, time.time()))
            return self.get_step(id)  # type: ignore[return-value]

    def finish_step(self, id: str, result: Any) -> None:
        with self._lock, self._db:
            cursor = self._db.execute("UPDATE steps SET status='completed',result=?,updated=? WHERE id=? AND status='started'", (_json(result), time.time(), id))
            if cursor.rowcount != 1:
                raise ValueError("only a started step can be completed; unknown requires reconciliation")

    def get_step(self, id: str) -> Optional[Step]:
        with self._lock:
            return self._step(self._db.execute("SELECT * FROM steps WHERE id=?", (id,)).fetchone())

    def list_steps(self, job_id: Optional[int] = None) -> list[Step]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM steps" + (" WHERE job_id=?" if job_id is not None else "") + " ORDER BY updated,id", (job_id,) if job_id is not None else ()).fetchall()
            return [self._step(row) for row in rows]  # type: ignore[misc]

    def enqueue_outbox(self, uuid: str, profile: str, target: Any, text: str, mode: str, payload: Any = None) -> bool:
        if not uuid:
            raise ValueError("outbox uuid must be nonempty")
        values = (profile, _json(target), text, mode, _json(payload))
        with self._lock, self._db:
            existing = self._db.execute("SELECT profile,target,text,mode,payload FROM outbox WHERE uuid=?", (uuid,)).fetchone()
            if existing:
                if tuple(existing) != values:
                    raise ValueError("outbox uuid reused with a different immutable payload")
                return False
            now = time.time()
            self._db.execute("INSERT INTO outbox(uuid,profile,target,text,mode,payload,created,updated) VALUES(?,?,?,?,?,?,?,?)", (uuid, *values, now, now))
            return True

    def claim_outbox(self, uuid: Optional[str] = None) -> Optional[Outbox]:
        with self._lock, self._db:
            row = self._db.execute("SELECT * FROM outbox WHERE status='queued'" + (" AND uuid=?" if uuid is not None else "") + " ORDER BY created,uuid LIMIT 1", (uuid,) if uuid is not None else ()).fetchone()
            if row is None:
                return None
            self._db.execute("UPDATE outbox SET status='sending',updated=? WHERE uuid=?", (time.time(), row["uuid"]))
            return self.get_outbox(row["uuid"])

    def finish_outbox(self, uuid: str, result: Any) -> None:
        with self._lock, self._db:
            cursor = self._db.execute("UPDATE outbox SET status='sent',result=?,updated=? WHERE uuid=? AND status='sending'", (_json(result), time.time(), uuid))
            if cursor.rowcount != 1:
                raise ValueError("only a sending outbox item can be confirmed sent")

    def mark_outbox_unknown(self, uuid: str, error: str = "send_outcome_unknown") -> None:
        with self._lock, self._db:
            cursor = self._db.execute("UPDATE outbox SET status='unknown',error=?,updated=? WHERE uuid=? AND status='sending'", (error, time.time(), uuid))
            if cursor.rowcount != 1:
                raise ValueError("only a sending outbox item can become unknown")

    def get_outbox(self, uuid: str) -> Optional[Outbox]:
        with self._lock:
            return self._outbox(self._db.execute("SELECT * FROM outbox WHERE uuid=?", (uuid,)).fetchone())

    def list_outbox(self, status: Optional[str] = None) -> list[Outbox]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM outbox" + (" WHERE status=?" if status else "") + " ORDER BY created,uuid", (status,) if status else ()).fetchall()
            return [self._outbox(row) for row in rows]  # type: ignore[misc]


class Scheduler:
    def __init__(self, store: Store, max_workers: int = 3, on_error: Optional[Callable[[Job, Exception], None]] = None):
        if max_workers < 1:
            raise ValueError("max_workers must be positive")
        self.store = store
        self.max_workers = max_workers
        self.on_error = on_error
        self._condition = threading.Condition()
        self._handlers: dict[str, tuple[str, Callable[[dict[str, Any]], Any]]] = {}
        self._lanes: set[str] = set()
        self._threads: list[threading.Thread] = []
        self._stopping = False
        self._drain = True

    def register(self, route: str, policy: Any, handler: Callable[[dict[str, Any]], Any]) -> None:
        with self._condition:
            self._handlers[route] = (_json(policy), handler)
            self._condition.notify_all()

    register_handler = register

    def submit(self, event_key: str, lane: str, route: str, policy: Any, event: dict[str, Any]) -> bool:
        with self._condition:
            if self._stopping:
                raise RuntimeError("scheduler is stopping")
            added = self.store.enqueue(event_key, lane, route, policy, event)
            self._condition.notify_all()
            return added

    def start(self) -> "Scheduler":
        with self._condition:
            if self._threads:
                if not self._stopping:
                    return self
                if any(thread.is_alive() for thread in self._threads):
                    raise RuntimeError("scheduler is still stopping")
                self._threads = []
            self._stopping = False
            self._drain = True
            self._threads = [threading.Thread(target=self._worker, name=f"autoreply-lane-{i}", daemon=True) for i in range(self.max_workers)]
            for thread in self._threads:
                thread.start()
        return self

    def _next(self) -> Optional[tuple[Job, Callable[[dict[str, Any]], Any]]]:
        for job in self.store.list_jobs("queued"):
            if job.lane in self._lanes:
                continue
            registered = self._handlers.get(job.route)
            if registered is None:
                self.store.block_job(job.id, "route_not_registered")
                self._condition.notify_all()
                continue
            policy, handler = registered
            if policy != _json(job.policy):
                self.store.block_job(job.id, "policy_changed")
                self._condition.notify_all()
                continue
            claimed = self.store.claim_job(job.id)
            if claimed:
                self._lanes.add(job.lane)
                return claimed, handler
        return None

    def _worker(self) -> None:
        while True:
            with self._condition:
                if self._stopping and not self._drain:
                    return
                selected = self._next()
                if selected is None:
                    if self._stopping:
                        return
                    self._condition.wait(timeout=0.2)
                    continue
            job, handler = selected
            try:
                handler(job.event)
                self.store.complete_job(job.id)
            except Exception as exc:
                self.store.fail_job(job.id, f"{type(exc).__name__}: {exc}")
                if self.on_error:
                    try:
                        self.on_error(job, exc)
                    except Exception:
                        pass
            finally:
                with self._condition:
                    self._lanes.discard(job.lane)
                    self._condition.notify_all()

    def wait_idle(self, timeout: Optional[float] = None) -> bool:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while self._lanes or self.store.list_jobs("queued"):
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._condition.wait(timeout=remaining)
            return True

    def stop(self, wait: bool = True, drain: bool = True) -> None:
        with self._condition:
            self._stopping = True
            self._drain = wait and drain
            self._condition.notify_all()
        if wait:
            for thread in self._threads:
                thread.join()
            self._threads = []
