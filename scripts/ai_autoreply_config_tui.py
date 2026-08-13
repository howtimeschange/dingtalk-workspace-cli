#!/usr/bin/env python3
"""
Terminal configuration UI for scripts/ai_autoreply_worker.py.

The model functions in this file are intentionally separate from curses so they
can be tested without a terminal. The interactive UI only edits the JSON config;
it does not start listeners or send DingTalk messages.
"""

from __future__ import annotations

import argparse
import copy
import curses
import json
import os
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import ai_autoreply_worker as worker  # noqa: E402


MISSING = object()

GROUP_ID_KEYS = ("group", "group_id", "id", "openConversationId", "open_conversation_id")
REPO_KEYS = ("repo", "cwd")
CODE_KEYWORD_KEYS = ("code_keywords", "codeKeywords", "repo_keywords", "repoKeywords")
TEST_COMMAND_KEYS = ("test_commands", "testCommands", "test_command", "testCommand")
REPLY_SKILL_KEYS = ("reply_skill", "replySkill")
REPLY_SKILL_PATH_KEYS = ("reply_skill_path", "replySkillPath")
AUDIT_DIR_KEYS = ("codex_audit_session_dir", "codexAuditSessionDir", "audit_session_dir", "auditSessionDir")
PROACTIVE_KEYS = ("codex_proactive_followup", "codexProactiveFollowup", "proactive_followup", "proactiveFollowup")
WRITE_POLICY_KEYS = ("code_write_policy", "codeWritePolicy", "write_policy", "writePolicy")


@dataclass(frozen=True)
class GroupRow:
    index: int
    enabled: bool
    name: str
    group_id: str
    project: str
    repo: str
    mode: str
    trigger: str
    reply_engine: str
    reply_skill: str
    send: bool
    codex_execute: bool
    code_write_policy: str
    codex_proactive_followup: bool
    history_context: bool
    history_context_limit: int
    codex_audit_session_dir: str
    query: str
    code_keywords: tuple[str, ...]
    test_commands: tuple[str, ...]
    listener_count: int
    strategy: str
    permissions: str


@dataclass(frozen=True)
class ValidationResult:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors


def default_config_path() -> Path:
    preferred = Path.home() / ".dws" / "ai-autoreply" / "winyai.config.json"
    if preferred.exists():
        return preferred
    return Path.home() / ".dws" / "ai-autoreply" / "config.json"


def load_config_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "defaults": {
                "mode": "draft",
                "send": False,
                "reply_target": "auto",
                "reply_engine": "codex",
                "classifier": "heuristic",
                "codex_execute": False,
            },
            "groups": [],
        }
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("config root must be an object")
    normalize_config_document(data)
    return data


def normalize_config_document(doc: dict[str, Any]) -> dict[str, Any]:
    defaults = doc.get("defaults")
    if not isinstance(defaults, dict):
        defaults = {}
        doc["defaults"] = defaults
    groups = doc.get("groups")
    if not isinstance(groups, list):
        groups = []
        doc["groups"] = groups
    return doc


def write_config_document(path: Path, doc: dict[str, Any], *, make_backup: bool = True) -> Optional[Path]:
    validation = validate_config_document(doc)
    if validation.errors:
        raise ValueError("; ".join(validation.errors))
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    backup: Optional[Path] = None
    if make_backup and path.exists():
        stamp = time.strftime("%Y%m%d-%H%M%S")
        backup = path.with_name(f"{path.name}.bak-{stamp}")
        backup.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return backup


def raw_value(item: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in item:
            return item[key]
    return MISSING


def config_value(group: dict[str, Any], defaults: dict[str, Any], keys: tuple[str, ...], fallback: Any = "") -> Any:
    value = raw_value(group, keys)
    if value is MISSING:
        value = raw_value(defaults, keys)
    if value is MISSING or value is None:
        return fallback
    return value


def as_string(value: Any, fallback: str = "") -> str:
    if value is MISSING or value is None:
        return fallback
    return str(value).strip()


def string_value(group: dict[str, Any], defaults: dict[str, Any], keys: tuple[str, ...], fallback: str = "") -> str:
    return as_string(config_value(group, defaults, keys, fallback), fallback)


def bool_from(value: Any, fallback: bool = False) -> bool:
    if value is MISSING or value is None:
        return fallback
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def bool_value(group: dict[str, Any], defaults: dict[str, Any], keys: tuple[str, ...], fallback: bool = False) -> bool:
    return bool_from(config_value(group, defaults, keys, fallback), fallback)


def int_from(value: Any, fallback: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def list_value(group: dict[str, Any], defaults: dict[str, Any], keys: tuple[str, ...]) -> tuple[str, ...]:
    value = config_value(group, defaults, keys, ())
    if isinstance(value, list):
        return tuple(str(item).strip() for item in value if str(item).strip())
    if isinstance(value, tuple):
        return tuple(str(item).strip() for item in value if str(item).strip())
    if isinstance(value, str):
        parts = [part.strip() for part in value.split(",")]
        return tuple(part for part in parts if part)
    return ()


def normalized_or_raw(value: str, normalizer, fallback: str) -> str:
    try:
        return normalizer(value)
    except ValueError:
        return value or fallback


def history_settings(group: dict[str, Any], defaults: dict[str, Any]) -> tuple[bool, int, int]:
    raw = config_value(group, defaults, ("history_context", "historyContext"), {})
    if isinstance(raw, dict):
        enabled = bool_from(raw.get("enabled"), False)
        limit = max(0, min(50, int_from(raw.get("limit"), 0)))
        minutes = max(0, int_from(raw.get("minutes"), 0))
        return enabled, limit, minutes
    return bool_from(raw, False), 0, 0


def group_row(index: int, group: dict[str, Any], defaults: dict[str, Any]) -> GroupRow:
    enabled = bool_from(group.get("enabled", True), True)
    group_id = string_value(group, {}, GROUP_ID_KEYS)
    mode = normalized_or_raw(string_value(group, defaults, ("mode",), "draft"), worker.normalize_mode, "draft")
    trigger = normalized_or_raw(
        string_value(group, defaults, ("trigger", "reply_trigger", "listen"), "group"),
        worker.normalize_trigger,
        "group",
    )
    reply_engine = string_value(group, defaults, ("reply_engine", "replyEngine"), "ai") or "ai"
    if reply_engine not in {"ai", "codex"}:
        reply_engine = reply_engine or "ai"
    code_write_policy = normalized_or_raw(
        string_value(group, defaults, WRITE_POLICY_KEYS, "none"),
        worker.normalize_code_write_policy,
        "none",
    )
    send = bool_value(group, defaults, ("send",), False) or mode == "send"
    codex_execute = bool_value(group, defaults, ("codex_execute", "codexExecute"), False)
    proactive = bool_value(group, defaults, PROACTIVE_KEYS, False)
    history_enabled, history_limit, _history_minutes = history_settings(group, defaults)
    audit_dir = string_value(group, defaults, AUDIT_DIR_KEYS)
    listener_count = 0
    if enabled:
        listener_count = 1
        if trigger == "mention" and code_write_policy == "approval":
            listener_count += 1
    permissions = permission_summary(
        send=send,
        codex_execute=codex_execute,
        code_write_policy=code_write_policy,
        history_enabled=history_enabled,
        history_limit=history_limit,
        proactive=proactive,
        audit_enabled=bool(audit_dir),
    )
    return GroupRow(
        index=index,
        enabled=enabled,
        name=string_value(group, {}, ("name",), group_id or f"group-{index + 1}"),
        group_id=group_id,
        project=string_value(group, defaults, ("project",)),
        repo=string_value(group, defaults, REPO_KEYS),
        mode=mode,
        trigger=trigger,
        reply_engine=reply_engine,
        reply_skill=string_value(group, defaults, REPLY_SKILL_KEYS),
        send=send,
        codex_execute=codex_execute,
        code_write_policy=code_write_policy,
        codex_proactive_followup=proactive,
        history_context=history_enabled,
        history_context_limit=history_limit,
        codex_audit_session_dir=audit_dir,
        query=string_value(group, defaults, ("query",)),
        code_keywords=list_value(group, defaults, CODE_KEYWORD_KEYS),
        test_commands=list_value(group, defaults, TEST_COMMAND_KEYS),
        listener_count=listener_count,
        strategy=f"{mode}/{trigger}/{reply_engine}",
        permissions=permissions,
    )


def permission_summary(
    *,
    send: bool,
    codex_execute: bool,
    code_write_policy: str,
    history_enabled: bool,
    history_limit: int,
    proactive: bool,
    audit_enabled: bool,
) -> str:
    parts = [
        "send" if send else "draft",
        "codex-exec" if codex_execute else "codex-off",
        f"write:{code_write_policy}",
        f"hist:{history_limit}" if history_enabled else "hist:off",
    ]
    if proactive:
        parts.append("proactive")
    if audit_enabled:
        parts.append("audit")
    return " ".join(parts)


def config_rows(doc: dict[str, Any]) -> list[GroupRow]:
    normalize_config_document(doc)
    defaults = doc.get("defaults", {})
    return [group_row(index, group, defaults) for index, group in enumerate(doc.get("groups", [])) if isinstance(group, dict)]


def config_summary(doc: dict[str, Any]) -> dict[str, Any]:
    normalize_config_document(doc)
    rows = config_rows(doc)
    validation = validate_config_document(doc)
    return {
        "enabled_count": sum(1 for row in rows if row.enabled),
        "disabled_count": sum(1 for row in rows if not row.enabled),
        "total_groups": len(rows),
        "errors": list(validation.errors),
        "warnings": list(validation.warnings),
        "groups": [asdict(row) for row in rows],
    }


def validate_config_document(doc: dict[str, Any]) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []
    if not isinstance(doc, dict):
        return ValidationResult(errors=("config root must be an object",), warnings=())
    normalize_config_document(doc)
    defaults = doc.get("defaults", {})
    groups = doc.get("groups", [])
    if not groups:
        errors.append("config.groups must contain at least one group")
    enabled_ids: dict[str, int] = {}
    any_enabled = False
    for index, group in enumerate(groups):
        if not isinstance(group, dict):
            errors.append(f"groups[{index}] must be an object")
            continue
        row = group_row(index, group, defaults)
        prefix = f"groups[{index}] {row.name}: "
        enabled = bool_from(group.get("enabled", True), True)
        if enabled:
            any_enabled = True
            if not row.group_id:
                errors.append(prefix + "enabled group is missing group/openConversationId")
            if row.group_id:
                previous = enabled_ids.get(row.group_id)
                if previous is not None:
                    errors.append(prefix + f"duplicate enabled group id also used by groups[{previous}]")
                enabled_ids[row.group_id] = index
        try:
            worker.normalize_mode(string_value(group, defaults, ("mode",), "draft"))
        except ValueError as exc:
            (errors if enabled else warnings).append(prefix + str(exc))
        try:
            worker.normalize_trigger(string_value(group, defaults, ("trigger", "reply_trigger", "listen"), "group"))
        except ValueError as exc:
            (errors if enabled else warnings).append(prefix + str(exc))
        try:
            worker.normalize_code_write_policy(string_value(group, defaults, WRITE_POLICY_KEYS, "none"))
        except ValueError as exc:
            (errors if enabled else warnings).append(prefix + str(exc))
        if row.reply_engine not in {"ai", "codex"}:
            (errors if enabled else warnings).append(prefix + "reply_engine must be one of: ai, codex")
    if groups and not any_enabled:
        errors.append("at least one group must be enabled for the worker to start")
    if not errors and any_enabled:
        temp_path = Path()
        try:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as handle:
                json.dump(doc, handle, ensure_ascii=False)
                temp_path = Path(handle.name)
            worker.load_agent_config(temp_path)
        except Exception as exc:  # Keep the exact worker contract visible.
            errors.append(str(exc))
        finally:
            if temp_path:
                try:
                    temp_path.unlink()
                except OSError:
                    pass
    return ValidationResult(errors=tuple(errors), warnings=tuple(warnings))


def toggle_group_enabled(doc: dict[str, Any], index: int) -> None:
    group = doc["groups"][index]
    group["enabled"] = not bool_from(group.get("enabled", True), True)


def cycle_group_field(doc: dict[str, Any], index: int, field: str, values: tuple[str, ...]) -> None:
    rows = config_rows(doc)
    if index >= len(rows):
        return
    current = getattr(rows[index], field)
    try:
        next_index = (values.index(current) + 1) % len(values)
    except ValueError:
        next_index = 0
    doc["groups"][index][field] = values[next_index]


def toggle_bool_field(doc: dict[str, Any], index: int, field: str, default: bool = False) -> None:
    group = doc["groups"][index]
    defaults = doc.get("defaults", {})
    inherited_default = bool_from(defaults.get(field, default), default)
    group[field] = not bool_from(group.get(field, inherited_default), inherited_default)


def toggle_history(doc: dict[str, Any], index: int) -> None:
    row = config_rows(doc)[index]
    doc["groups"][index]["history_context"] = {
        "enabled": not row.history_context,
        "limit": row.history_context_limit or 20,
    }


def adjust_history_limit(doc: dict[str, Any], index: int, delta: int) -> None:
    row = config_rows(doc)[index]
    new_limit = max(0, min(50, row.history_context_limit + delta))
    doc["groups"][index]["history_context"] = {
        "enabled": True,
        "limit": new_limit,
    }


def add_group(doc: dict[str, Any]) -> int:
    normalize_config_document(doc)
    number = len(doc["groups"]) + 1
    doc["groups"].append({
        "name": f"新群配置 {number}",
        "group": "",
        "project": "",
        "repo": "",
        "mode": string_value({}, doc.get("defaults", {}), ("mode",), "draft"),
        "reply_engine": string_value({}, doc.get("defaults", {}), ("reply_engine", "replyEngine"), "codex"),
        "trigger": "mention",
        "send": False,
        "codex_execute": False,
        "code_write_policy": "none",
        "enabled": False,
    })
    return len(doc["groups"]) - 1


def duplicate_group(doc: dict[str, Any], index: int) -> int:
    normalize_config_document(doc)
    source = doc["groups"][index]
    clone = copy.deepcopy(source)
    clone["name"] = f"{as_string(source.get('name'), '群配置')} copy"
    clone["group"] = ""
    clone.pop("group_id", None)
    clone.pop("id", None)
    clone.pop("openConversationId", None)
    clone.pop("open_conversation_id", None)
    clone["enabled"] = False
    doc["groups"].insert(index + 1, clone)
    return index + 1


def set_group_field(doc: dict[str, Any], index: int, field: str, value: str) -> None:
    group = doc["groups"][index]
    if field in {"code_keywords", "test_commands", "approval_phrases"}:
        group[field] = [part.strip() for part in value.split(",") if part.strip()]
    elif field == "history_limit":
        group["history_context"] = {
            "enabled": True,
            "limit": max(0, min(50, int_from(value, 20))),
        }
    else:
        group[field] = value.strip()


def consume_preview(row: GroupRow) -> tuple[str, ...]:
    if not row.enabled:
        return ("disabled",)
    event_key = "user_im_message_receive_at" if row.trigger == "mention" else "user_im_message_receive_group"
    lines = [f"main: {event_key} -> {row.name}"]
    if row.listener_count > 1:
        lines.append("approval: user_im_message_receive_group -> approval confirmations")
    return tuple(lines)


def truncate(text: str, width: int) -> str:
    if width <= 0:
        return ""
    text = str(text)
    if len(text) <= width:
        return text
    if width <= 3:
        return text[:width]
    return text[: width - 3] + "..."


class ConfigTui:
    def __init__(self, stdscr, path: Path, doc: dict[str, Any]):
        self.stdscr = stdscr
        self.path = path
        self.doc = doc
        self.selected = 0
        self.scroll = 0
        self.dirty = False
        self.message = "q quit | S save | a add | d duplicate | enter edit | ? help"

    def rows(self) -> list[GroupRow]:
        return config_rows(self.doc)

    def run(self) -> None:
        curses.curs_set(0)
        self.stdscr.keypad(True)
        while True:
            self.draw()
            key = self.stdscr.getch()
            if self.handle_key(key):
                return

    def handle_key(self, key: int) -> bool:
        rows = self.rows()
        if key in (curses.KEY_UP, ord("k")):
            self.selected = max(0, self.selected - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            self.selected = min(max(0, len(rows) - 1), self.selected + 1)
        elif key == ord("a"):
            self.selected = add_group(self.doc)
            self.mark_dirty("added disabled group; edit group id before enabling")
        elif key == ord("d") and rows:
            self.selected = duplicate_group(self.doc, self.selected)
            self.mark_dirty("duplicated as disabled group; same project can now be opened again")
        elif key == ord("e") and rows:
            toggle_group_enabled(self.doc, self.selected)
            self.mark_dirty("toggled enabled")
        elif key == ord("m") and rows:
            cycle_group_field(self.doc, self.selected, "mode", tuple(sorted(worker.AUTOMATION_MODES)))
            self.mark_dirty("cycled strategy mode")
        elif key == ord("t") and rows:
            cycle_group_field(self.doc, self.selected, "trigger", ("mention", "group"))
            self.mark_dirty("cycled trigger")
        elif key == ord("r") and rows:
            cycle_group_field(self.doc, self.selected, "reply_engine", ("codex", "ai"))
            self.mark_dirty("cycled reply engine")
        elif key == ord("s") and rows:
            toggle_bool_field(self.doc, self.selected, "send", False)
            self.mark_dirty("toggled send permission")
        elif key == ord("x") and rows:
            toggle_bool_field(self.doc, self.selected, "codex_execute", False)
            self.mark_dirty("toggled Codex execute permission")
        elif key == ord("w") and rows:
            cycle_group_field(self.doc, self.selected, "code_write_policy", ("none", "approval", "auto"))
            self.mark_dirty("cycled write policy")
        elif key == ord("p") and rows:
            toggle_bool_field(self.doc, self.selected, "codex_proactive_followup", False)
            self.mark_dirty("toggled proactive followup")
        elif key == ord("h") and rows:
            toggle_history(self.doc, self.selected)
            self.mark_dirty("toggled history context")
        elif key in (ord("+"), ord("=")) and rows:
            adjust_history_limit(self.doc, self.selected, 5)
            self.mark_dirty("increased history limit")
        elif key == ord("-") and rows:
            adjust_history_limit(self.doc, self.selected, -5)
            self.mark_dirty("decreased history limit")
        elif key in (10, 13, curses.KEY_ENTER) and rows:
            self.edit_selected()
        elif key == ord("v"):
            self.validate_message()
        elif key == ord("S"):
            self.save()
        elif key == ord("?"):
            self.show_help()
        elif key == ord("q"):
            if self.dirty:
                answer = self.prompt("unsaved changes; type q again to quit", "")
                return answer.strip().lower() == "q"
            return True
        return False

    def mark_dirty(self, message: str) -> None:
        self.dirty = True
        self.message = message

    def validate_message(self) -> None:
        validation = validate_config_document(self.doc)
        if validation.errors:
            self.message = "ERROR: " + validation.errors[0]
        elif validation.warnings:
            self.message = "WARN: " + validation.warnings[0]
        else:
            self.message = "config validation OK"

    def save(self) -> None:
        validation = validate_config_document(self.doc)
        if validation.errors:
            self.message = "cannot save: " + validation.errors[0]
            return
        try:
            backup = write_config_document(self.path, self.doc)
        except Exception as exc:
            self.message = f"save failed: {exc}"
            return
        self.dirty = False
        self.message = f"saved; backup={backup}" if backup else "saved"

    def edit_selected(self) -> None:
        fields = (
            "name", "group", "project", "repo", "query", "code_keywords",
            "test_commands", "reply_skill", "reply_skill_path",
            "codex_audit_session_dir", "ready_message", "history_limit",
        )
        field = self.prompt("field " + "/".join(fields), "")
        if field not in fields:
            self.message = "unknown field"
            return
        row = self.rows()[self.selected]
        initial = {
            "name": row.name,
            "group": row.group_id,
            "project": row.project,
            "repo": row.repo,
            "query": row.query,
            "code_keywords": ",".join(row.code_keywords),
            "test_commands": ",".join(row.test_commands),
            "reply_skill": row.reply_skill,
            "reply_skill_path": string_value(self.doc["groups"][self.selected], self.doc.get("defaults", {}), REPLY_SKILL_PATH_KEYS),
            "codex_audit_session_dir": row.codex_audit_session_dir,
            "ready_message": as_string(self.doc["groups"][self.selected].get("ready_message"), ""),
            "history_limit": str(row.history_context_limit or 20),
        }[field]
        value = self.prompt(f"{field}", initial)
        set_group_field(self.doc, self.selected, field, value)
        self.mark_dirty(f"edited {field}")

    def prompt(self, label: str, initial: str = "") -> str:
        max_y, max_x = self.stdscr.getmaxyx()
        prompt_text = truncate(f"{label}: {initial}", max_x - 2)
        self.safe_addstr(max_y - 1, 0, " " * (max_x - 1), curses.A_REVERSE)
        self.safe_addstr(max_y - 1, 0, prompt_text, curses.A_REVERSE)
        curses.echo()
        curses.curs_set(1)
        try:
            self.stdscr.move(max_y - 1, min(len(f"{label}: "), max_x - 2))
            raw = self.stdscr.getstr(max_y - 1, min(len(f"{label}: "), max_x - 2), max(1, max_x - len(label) - 3))
            text = raw.decode("utf-8", errors="replace")
        finally:
            curses.noecho()
            curses.curs_set(0)
        return text if text else initial

    def show_help(self) -> None:
        self.message = (
            "keys: e enable, m mode, t trigger, r engine, s send, x codex, "
            "w write, p proactive, h history, +/- history limit, enter edit"
        )

    def draw(self) -> None:
        self.stdscr.erase()
        rows = self.rows()
        max_y, max_x = self.stdscr.getmaxyx()
        selected = rows[self.selected] if rows else None
        title = f"DingTalk Worker Config TUI  path={self.path}"
        if self.dirty:
            title += "  [dirty]"
        summary = config_summary(self.doc)
        self.safe_addstr(0, 0, truncate(title, max_x - 1), curses.A_BOLD)
        self.safe_addstr(
            1,
            0,
            truncate(
                f"groups enabled={summary['enabled_count']} disabled={summary['disabled_count']} total={summary['total_groups']}",
                max_x - 1,
            ),
        )
        header = "No On  Name                 Project        Strategy             Permissions"
        self.safe_addstr(3, 0, truncate(header, max_x - 1), curses.A_UNDERLINE)
        list_height = max(3, max_y - 12)
        if self.selected < self.scroll:
            self.scroll = self.selected
        if self.selected >= self.scroll + list_height:
            self.scroll = self.selected - list_height + 1
        for visible, row in enumerate(rows[self.scroll:self.scroll + list_height], start=0):
            y = 4 + visible
            marker = "on " if row.enabled else "off"
            line = (
                f"{row.index + 1:02d} {marker:<3} "
                f"{truncate(row.name, 20):<20} "
                f"{truncate(row.project, 14):<14} "
                f"{truncate(row.strategy, 20):<20} "
                f"{truncate(row.permissions, max(8, max_x - 65))}"
            )
            attr = curses.A_REVERSE if row.index == self.selected else 0
            self.safe_addstr(y, 0, truncate(line, max_x - 1), attr)
        detail_y = max_y - 7
        self.safe_addstr(detail_y, 0, "-" * max(0, max_x - 1))
        if selected:
            details = [
                f"group={selected.group_id or '<missing>'}",
                f"repo={selected.repo or '<none>'}",
                f"skill={selected.reply_skill or '<none>'} audit={'on' if selected.codex_audit_session_dir else 'off'}",
                f"query={selected.query or '<none>'}",
                "listeners=" + " | ".join(consume_preview(selected)),
            ]
            for offset, line in enumerate(details[:5], start=1):
                self.safe_addstr(detail_y + offset, 0, truncate(line, max_x - 1))
        else:
            self.safe_addstr(detail_y + 1, 0, "No groups yet. Press a to add a disabled group config.")
        self.safe_addstr(max_y - 1, 0, truncate(self.message, max_x - 1), curses.A_REVERSE)
        self.stdscr.refresh()

    def safe_addstr(self, y: int, x: int, text: str, attr: int = 0) -> None:
        max_y, max_x = self.stdscr.getmaxyx()
        if y < 0 or y >= max_y or x >= max_x:
            return
        try:
            self.stdscr.addnstr(y, x, text, max(0, max_x - x - 1), attr)
        except curses.error:
            pass


def run_tui(path: Path) -> None:
    doc = load_config_document(path)

    def wrapped(stdscr) -> None:
        ConfigTui(stdscr, path, doc).run()

    curses.wrapper(wrapped)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TUI editor for DingTalk AI auto-reply worker config")
    parser.add_argument("--config", default=str(default_config_path()), help="worker JSON config path")
    parser.add_argument("--summary-json", action="store_true", help="print config summary JSON and exit")
    parser.add_argument("--validate", action="store_true", help="validate config and exit")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    path = Path(args.config).expanduser()
    doc = load_config_document(path)
    if args.summary_json:
        print(json.dumps(config_summary(doc), ensure_ascii=False, indent=2))
        return 0
    if args.validate:
        validation = validate_config_document(doc)
        for warning in validation.warnings:
            print(f"WARN: {warning}", file=sys.stderr)
        for error in validation.errors:
            print(f"ERROR: {error}", file=sys.stderr)
        if validation.ok:
            print("OK")
            return 0
        return 1
    run_tui(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
