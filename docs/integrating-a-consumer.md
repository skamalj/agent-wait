# Integrating a consumer

You have been handed an SNS topic (or a bus, a queue, or a DynamoDB table) that carries
wait envelopes, and you need to build the thing a human actually clicks. This is everything
you need. You do not need to install agent-wait, read its source, or know what LangGraph is.

There are exactly two moves: **listen**, and **reply**.

---

## 1. Listen

Subscribe, and you will receive
[wait envelopes](message-formats.md#1-the-wait-envelope-outbound).

```python
def on_envelope(envelope: dict) -> None:
    key = f"{envelope['type']}:{envelope['interrupt_id']}"
    if already_seen(key):        # at-least-once delivery, and deliberate republishing
        return
    mark_seen(key)

    if envelope["type"] == "wait.created":
        show_approval(envelope)
    elif envelope["type"] == "wait.resumed":
        retract_approval(envelope["interrupt_id"])
```

**Deduplicate on `type` + `interrupt_id`, not on `event_id`.** `event_id` is fresh on every
publish, and the agent deliberately republishes the same question when a message is
redelivered or an operator repairs a lost announce. Deduplicating on `event_id` would show
the same approval twice; `interrupt_id` is stable for as long as the question stands.

Render `question` however suits you — it is opaque to everything in between — and offer
exactly the buttons in `allowed_actions`. Nothing will refuse a third one, which is
precisely why you should not draw it.

`wait.resumed` means the graph has moved past the question. It may arrive because somebody
else answered, or because a timeout sweep sent the default. Either way: close the ticket.

## 2. Reply

The envelope contains a filled-in reply. Copy it, set `answer`, and post it to `reply_to`.

```python
def on_click(envelope: dict, action: str) -> None:
    body = {**envelope["reply_with"], "answer": {"action": action, "by": current_user()}}
    send_to(envelope["reply_to"], body)
```

That is the whole protocol. Do not construct the message yourself and do not hardcode the
destination — `reply_to` is how the agent tells you where it listens, and it differs
between environments.

### `answer` is returned verbatim

Whatever you put in `answer` is exactly what the graph's `ask()` call returns. Nothing is
merged into it and no fields are added. If the graph reads `decision["action"]`, send
`{"action": "approve"}`; if it expects a string, send a string.

This is worth stating plainly because it is a trust boundary. The graph will act on what
you send, and nothing between you and it will second-guess an `action` that is not in
`allowed_actions`. Validate before you send.

### Answering twice is safe, and answering late is safe

The agent checks whether the thread is still parked on that interrupt before it does
anything. A double click, a retried HTTP request or a second approver an hour later are all
dropped. You do not need an idempotency key.

What is **not** guaranteed is two genuinely different answers arriving in the same instant
— that depends on the agent's transport. Ask whoever runs it; if the entry point is an SQS
FIFO queue keyed by thread, it is serialised and you are fine.

---

## 3. If the questions land in a table instead

`DynamoDbAnnounce` writes each question as a row rather than publishing an event, which is
often a better fit for an approvals UI: you query for open questions instead of maintaining
your own projection of a topic.

```
pk = "THREAD#order-4471"
sk = "WAIT#a1b2c3d4e5f60718"
status = "open" | "closed"
```

Everything the envelope carries is on the row, including `reply_with`. A GSI on `status`
with `expires_at` as the range key gives you both queries you want:

```python
# everything a human should see
table.query(IndexName="by_status", KeyConditionExpression=Key("status").eq("open"))

# everything overdue -- see the next section
table.query(
    IndexName="by_status",
    KeyConditionExpression=Key("status").eq("open") & Key("expires_at").lt(now_iso()),
)
```

Rows are overwritten in place when a question is republished, so a repair does not create a
duplicate. `wait.resumed` sets `status = "closed"` rather than deleting the row, so there
is still something to show when someone asks why the button disappeared.

---

## 4. Somebody has to enforce the timeout

**The agent will not.** `expires_at` and `default` are published as information; nothing
acts on them. If nobody runs a sweep, an unanswered question waits forever.

It may or may not be your job — agree with whoever runs the agent — but it is somebody's,
and it is about ten lines:

```python
def sweep(now_iso: str) -> None:
    for row in open_questions_past(now_iso):
        send_to(row["reply_to"], {**row["reply_with"], "answer": row["default"]})
```

Two details that matter:

* **Send `default` unchanged.** The graph's author wrote it next to the question; it is
  published verbatim so that nothing in the middle rewrites it.
* **Re-check that the question is still open** immediately before sending, or you race a
  human answering right at the deadline. Both answers being dropped is fine; the graph
  resuming twice is not.

A working sweep is `scenario_b` in `examples/refund_agent/demo_scenarios.py`.

---

## 5. What the agent guarantees you

* The question will be published **at least once**. Deduplicate on `type` + `interrupt_id`.
* `interrupt_id` is stable for as long as the question stands.
* Your `answer` reaches the graph unmodified.
* An answer to a question that has already been resolved is dropped, not applied.
* A `wait.resumed` envelope follows every question the graph moves past.

## What it does not

* It does not check your `action` against `allowed_actions`.
* It does not enforce `expires_at`.
* It does not authenticate you. Whoever can write to `reply_to` can answer any open
  question on it.
