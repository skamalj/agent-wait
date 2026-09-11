# Integrating a consumer

You have been handed an SNS topic (or a queue, a webhook, or a DynamoDB table) that
carries agent-wait envelopes, and you need to build the thing a human clicks. This is
everything you need. You do not need to install agent-wait or know what LangGraph is.

There are exactly two moves: **listen**, and **reply**.

## 1. Listen

Every envelope is a `wait.created` — a question the agent is waiting on. Full schema in
[Message formats](message-formats.md).

```python
def on_envelope(envelope: dict) -> None:
    key = f"{envelope['type']}:{envelope['question_id']}"
    if already_seen(key):  # at-least-once delivery, and deliberate republishing
        return
    mark_seen(key)
    show_approval(envelope)  # render `question`, offer `allowed_actions`
```

**Deduplicate on `type` + `question_id`, not on `event_id`.** The same question is
republished whenever the agent's message is redelivered; `event_id` changes every time,
`question_id` does not.

`source` tells you which tool or node asked. `expires_at` and `default` tell you what
the asker wants if nobody answers — nothing enforces them; if you want a deadline
honoured, you honour it (see §3).

## 2. Reply

Copy `reply_with`, set `answer`, send it to the agent's entry point — `reply_to` if the
envelope has one, otherwise wherever you were told the agent listens.

```python
def on_click(envelope: dict, decision: dict) -> None:
    send_to(agent_entry_point, {**envelope["reply_with"], "answer": decision})
```

`answer` is returned to the graph **verbatim**. If the question came from a `@hitl`
tool, `{"action": "approve"}` runs it and anything else is shown to the model as the
reason it did not run. If the question is a middleware batch (`question.actions`), the
answer is `{"decisions": [...]}` with one entry per action, in order.

Sending the same answer twice is harmless — the agent's framework ignores a resume for a
question it has already moved past. So is a second, different answer: the first stands.

## 3. Deadlines are yours

If nobody answers, nothing happens; the agent waits. If the asker set `expires_at`, the
intent is that *you* notice it passing and send `default` as the answer — it is an
ordinary answer:

```python
for envelope in open_questions_past(now):
    send_to(agent_entry_point, {**envelope["reply_with"], "answer": envelope["default"]})
```

If the questions land in the DynamoDB table, `open_questions_past()` is a query on the
`by_status` GSI with a range condition on `expires_at`. On a topic or queue, it is
whatever you keep. Send `default` unchanged — the asker wrote it.

## 4. If the questions land in a table

`DynamoDbAnnounce` writes one row per question — `pk = THREAD#…`, `sk = WAIT#…`,
`status = open`, every envelope field — and never touches it again. Marking rows
answered, sweeping them, or ignoring them is yours. It exists so an approvals UI can
`Query` for open questions without building a projection off a topic.

## What the agent guarantees you

- Every question is published at least once, with a stable `question_id`.
- `answer` reaches the graph unmodified.
- A duplicate or late answer changes nothing.

## What it does not

- Check your `answer` against `allowed_actions`.
- Enforce `expires_at`, `valid_until` or `answer_ttl`.
- Authenticate you. Whoever can reach the agent's entry point can answer.
