# DingTalk AI Auto Reply Worker

`scripts/ai_autoreply_worker.py` listens to DingTalk personal IM events with
`dws event consume ... --flatten -f ndjson`, classifies project-group feedback,
drafts or sends replies, and can route replies through a persistent per-group
Codex session for memory and read-only local inspection.

This is a guarded worker, not a history polling script. It is designed for
launchd/systemd/cron supervision.

## Safety Model

- Default mode only prints reply drafts and Codex task prompts. It does not send
  messages and does not start Codex.
- Real sending requires `--send` plus a target scope such as `--group`,
  `--open-dingtalk-id`, `--allow-conversation`, `--allow-sender`, or
  `--allow-any-target`.
- Real Codex execution requires `mode=guarded` or `mode=autofix` plus
  `codex_execute=true` / `--codex-execute`.
- `guarded` starts Codex in `read-only` sandbox for assessment only.
- `autofix` starts Codex in `workspace-write` sandbox only for low-risk,
  high-confidence, confirmed BUGs with a configured repo and test command.
- Each event is de-duplicated in `~/.dws/ai-autoreply/seen.jsonl`.
- The worker keeps `dws event consume` stdin open and stops it with SIGTERM, so
  self-created personal subscriptions can be cleaned up by the existing event
  command.

See [`dws-codex-personal-agent-spec.md`](dws-codex-personal-agent-spec.md) for
the full state machine and product-agent workflow.

## AI Provider

Set an OpenAI-compatible endpoint with environment variables:

```bash
export DWS_AI_API_KEY="..."
export DWS_AI_MODEL="..."
# Optional. Defaults to https://api.openai.com/v1
export DWS_AI_BASE_URL="https://api.openai.com/v1"
```

Secrets should stay in the process environment or an OS service environment
file. Do not put API keys into the repository or launchd plist committed to git.

Classification defaults to local deterministic rules (`--classifier heuristic`)
so the worker can route feedback even without an AI provider. Use
`--classifier hybrid` to ask the model for structured classification when
credentials exist and fall back to rules if the model fails. Dialogue reply text
uses `reply_engine`:

- `reply_engine: "ai"` calls the configured OpenAI-compatible chat endpoint for
  a one-shot reply.
- `reply_engine: "codex"` resumes or creates one Codex session per group and
  asks Codex to produce the DingTalk reply. This preserves Codex conversation
  history and can use read-only local repository inspection when that repo is
  configured and the message matches the code keywords.

When Codex session reply is enabled, DWS still only listens and sends; Codex
does the thinking and any allowed read-only probing.

Set `reply_skill` to inject a local Codex skill into every reply prompt. For
example, `reply_skill: "laoxing"` loads the local 老邢 skill and its style
references so both one-shot AI replies and persistent Codex session replies use
the same voice.

Set `codex_audit_session_dir` to mirror each persistent Codex conversation into
a human-visible local project. The worker still keeps its machine resume state
under `codex_session_dir`; the audit directory receives readable copies of
`session.json`, prompts, last replies, Codex JSONL logs, and an `events.jsonl`
index for review. These files can contain DingTalk message context, so keep the
audit project local/private.

For code-like feedback, set `codex_proactive_followup: true` to avoid a pure
one-question-one-answer flow: the worker sends a short acknowledgement, runs a
read-only Codex review, then sends the diagnosis or confirmation request back
to the group.

## Dry Run

### Config allowlist

For project-group automation, prefer a JSON allowlist config. The worker only
starts listeners for enabled groups in this file, and every event is checked
again against the configured `conversation_id` before AI generation or sending.

Start from [`ai-autoreply-config.example.json`](ai-autoreply-config.example.json):

```json
{
  "defaults": {
    "mode": "draft",
    "query": "问题,反馈,bug,报错,不能用,需求",
    "ready_message": "",
    "send": false,
    "reply_engine": "codex",
    "reply_skill": "laoxing",
    "reply_skill_path": "~/.codex/skills/laoxing/SKILL.md",
    "codex_audit_session_dir": "~/Documents/ChatGPT/钉钉分身会话",
    "codex_proactive_followup": false,
    "codex_execute": false
  },
  "groups": [
    {
      "name": "赢在AI",
      "group": "cid_REPLACE_WINYAI",
      "project": "winyai",
      "repo": "",
      "query": "问题,反馈,bug,报错,不能用,需求",
      "ready_message": "我在了",
      "enabled": true
    },
    {
      "name": "抓虾反馈群",
      "group": "cid_REPLACE_CRAWSHRIMP",
      "project": "crawshrimp",
      "repo": "/ABS/PATH/TO/crawshrimp",
      "mode": "guarded",
      "reply_engine": "codex",
      "code_keywords": ["抓虾", "crawshrimp"],
      "query": "抓虾,AI视频,生图,鞋包,报错,不能用,需求",
      "test_commands": ["python3 -m pytest"],
      "enabled": true
    }
  ]
}
```

Validate the config and print the exact `dws event consume` commands without
starting any listener:

```bash
python3 scripts/ai_autoreply_worker.py \
  --config ~/.dws/ai-autoreply/config.json \
  --config-check
```

Run all enabled group listeners in draft mode:

```bash
python3 scripts/ai_autoreply_worker.py \
  --config ~/.dws/ai-autoreply/config.json \
  --print-command
```

Config mode uses `user_im_message_receive_group` and one consumer per group.
This matches the DWS event contract: one `event consume` process owns one event
subscription scope.

### Config TUI

Use the local terminal UI to inspect and edit configured group avatars:

```bash
python3 scripts/ai_autoreply_config_tui.py \
  --config ~/.dws/ai-autoreply/config.json
```

The TUI shows every configured group, including disabled groups, with project,
strategy (`mode/trigger/reply_engine`), active listener count, and permissions
such as send, Codex execution, write policy, history context, proactive
follow-up, and audit mirroring. It also supports duplicating a group as a
disabled copy, so the same project can be opened by multiple group avatars with
different group IDs or policies.

Useful keys:

| Key | Action |
|---|---|
| `e` | enable or disable the selected group |
| `m` | cycle automation mode |
| `t` | switch trigger between mention-only and all group messages |
| `r` | switch reply engine between Codex session and one-shot AI |
| `s` | toggle real send permission |
| `x` | toggle Codex execution |
| `w` | cycle code write policy (`none` / `approval` / `auto`) |
| `p` | toggle proactive follow-up |
| `h`, `+`, `-` | toggle history context and adjust history limit |
| `a` | add a disabled group config |
| `d` | duplicate the selected group as a disabled copy |
| `Enter` | edit text fields like group id, project, repo, skill, audit dir |
| `v` | validate the config |
| `S` | save atomically with a timestamped backup |

For non-interactive review:

```bash
python3 scripts/ai_autoreply_config_tui.py \
  --config ~/.dws/ai-autoreply/config.json \
  --summary-json

python3 scripts/ai_autoreply_config_tui.py \
  --config ~/.dws/ai-autoreply/config.json \
  --validate
```

To send a connection marker after the listener is ready, set
`ready_message` on the group:

```json
{
  "name": "赢在AI",
  "group": "cidxxxxxxxx",
  "project": "winyai",
  "mode": "draft",
  "ready_message": "我在了",
  "enabled": true
}
```

Draft replies for messages that @ you:

```bash
python3 scripts/ai_autoreply_worker.py \
  --event-key user_im_message_receive_at \
  --print-command
```

Draft replies for one group:

```bash
python3 scripts/ai_autoreply_worker.py \
  --event-key user_im_message_receive_group \
  --group cidxxxxxxxx \
  --query "找我,帮忙,确认" \
  --print-command
```

The worker prints one JSON object per handled event:

```json
{"action":"draft","event_id":"...","reply":"我先确认一下，稍后回复。"}
```

Code-affecting messages print a Codex state instead:

```json
{"action":"codex_draft","classification":{"type":"feature","needs_code":true},"task_id":"..."}
```

Task bundles are stored under:

```text
~/.dws/ai-autoreply/codex-tasks/<task_id>/
```

Persistent group Codex session metadata and per-message logs are stored under:

```text
~/.dws/ai-autoreply/codex-sessions/<conversation_id>/
```

## Automation Modes

| Mode | Sends replies | Starts Codex | Can modify code |
|---|---:|---:|---:|
| `observe` | no | no | no |
| `draft` | no | no | no |
| `guarded` | yes, with `send=true` | yes, with `codex_execute=true` | no |
| `autofix` | yes, with `send=true` | yes, with `codex_execute=true` | gated low-risk BUG only |

For `autofix`, configure at least one test command:

```json
{
  "name": "抓虾反馈群",
  "group": "cid_REPLACE_CRAWSHRIMP",
  "project": "crawshrimp",
  "repo": "/ABS/PATH/TO/crawshrimp",
  "mode": "autofix",
  "send": true,
  "codex_execute": true,
  "test_commands": ["python3 -m pytest"],
  "enabled": true
}
```

Autofix is blocked when the feedback is a feature, suspected BUG, high-risk
operation, missing repo, missing test command, or below the confidence
threshold.

## Real Sending

Reply in a single allowlisted group:

```bash
python3 scripts/ai_autoreply_worker.py \
  --event-key user_im_message_receive_group \
  --group cidxxxxxxxx \
  --allow-conversation cidxxxxxxxx \
  --send
```

Or enable sending for exactly one configured group:

```json
{
  "name": "抓虾反馈群",
  "group": "cid_REPLACE_CRAWSHRIMP",
  "project": "crawshrimp",
  "repo": "/ABS/PATH/TO/crawshrimp",
  "mode": "send",
  "send": true,
  "enabled": true
}
```

In config mode, a group with `send: true` automatically allowlists only that
same group as a delivery target. Other conversations are skipped before the AI
call.

Reply to a single allowlisted openDingtalkId:

```bash
python3 scripts/ai_autoreply_worker.py \
  --event-key user_im_message_receive_o2o \
  --open-dingtalk-id openxxxxxxxx \
  --allow-sender openxxxxxxxx \
  --send
```

`--reply-target auto` sends group and @ replies back to the conversation, and
single-chat replies back to the sender. Use `--reply-target sender` if you want
private replies for a sender-scoped subscription.

## macOS launchd

For 7x24 running, prefer launchd `KeepAlive` over polling message history.
Save as `~/Library/LaunchAgents/com.dws.ai-autoreply.plist` and replace paths,
IDs, model, and base URL.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.dws.ai-autoreply</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/python3</string>
    <string>/ABS/PATH/TO/dingtalk-workspace-cli/scripts/ai_autoreply_worker.py</string>
    <string>--config</string>
    <string>~/.dws/ai-autoreply/config.json</string>
    <string>--print-command</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>StandardOutPath</key>
  <string>/tmp/dws-ai-autoreply.out.log</string>
  <key>StandardErrorPath</key>
  <string>/tmp/dws-ai-autoreply.err.log</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin</string>
    <key>DWS_AI_API_KEY</key>
    <string>REPLACE_WITH_RUNTIME_SECRET</string>
    <key>DWS_AI_MODEL</key>
    <string>REPLACE_WITH_MODEL</string>
    <key>DWS_AI_BASE_URL</key>
    <string>https://api.openai.com/v1</string>
  </dict>
</dict>
</plist>
```

Load and inspect:

```bash
launchctl load -w ~/Library/LaunchAgents/com.dws.ai-autoreply.plist
tail -f /tmp/dws-ai-autoreply.out.log /tmp/dws-ai-autoreply.err.log
```

Unload:

```bash
launchctl unload -w ~/Library/LaunchAgents/com.dws.ai-autoreply.plist
```

## Periodic Window

If you explicitly want cron-style periodic windows, keep event consumption
bounded:

```bash
*/15 * * * * cd /ABS/PATH/TO/dingtalk-workspace-cli && \
  DWS_AI_API_KEY=... DWS_AI_MODEL=... \
  python3 scripts/ai_autoreply_worker.py \
    --event-key user_im_message_receive_at \
    --duration 14m \
    >> /tmp/dws-ai-autoreply.out.log 2>> /tmp/dws-ai-autoreply.err.log
```

This only handles messages received while the worker is running. For production
auto-reply, launchd/systemd supervision is more reliable.
