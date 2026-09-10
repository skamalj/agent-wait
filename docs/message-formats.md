# Message formats

This is the contract. Three JSON documents: one agent-wait sends, two it can be given
back. A team that has read only this file should be able to write a working consumer *and*
a working host.

Everything else in this repository is an implementation detail. These are not.

---

## 1. The wait envelope (outbound)

Emitted by every announce adapter, on every transition. This is what arrives on your SNS
topic, your queue, your bus, or as a row in your table.

```json
{
  "type": "wait.created",
  "event_id": "01JAV3K2R8Q9WZ7M4X1YB6N0PD",
  "thread_id": "order-4471",
  "interrupt_id": "a1b2c3d4e5f60718",
  "question": {
    "kind": "refund_approval",
    "order_id": "order-4471",
    "amount": 41000,
    "customer": "customer-of-order-4471"
  },
  "allowed_actions": ["approve", "reject"],
  "expires_at": "2026-09-12T09:00:00Z",
  "default": { "action": "reject", "reason": "no response within P3D" },
  "reply_to": { "kind": "sqs", "url": "https://sqs.ap-south-1.amazonaws.com/…/agent-runs.fifo" },
  "reply_with": {
    "thread_id": "order-4471",
    "interrupt_id": "a1b2c3d4e5f60718",
    "answer": null
  },
  "correlation": null,
  "tags": { "approver_group": "finance" }
}
```

| Field | Meaning |
|---|---|
| `type` | `wait.created` or `wait.resumed`. Nothing else. |
| `event_id` | ULID, fresh on every publish. Useful in logs. **Not** the deduplication key. |
| `thread_id` | The agent's conversation identity. |
| `interrupt_id` | The question's identity, stable for as long as the question stands. |
| `question` | Whatever the agent asked. Opaque to every adapter; only your UI interprets it. |
| `allowed_actions` | Which answers are meaningful. **Advisory** — see below. |
| `expires_at` | RFC 3339 UTC, or `null` when the wait has no timeout. **Advisory.** |
| `default` | What the graph's author said to assume if nobody answers. **Advisory.** |
| `reply_to` | Where to send the answer. The agent's own entry point. |
| `reply_with` | A filled-in reply. Copy it, set `answer`, post it to `reply_to`. |
| `correlation` | `{"provider": …, "id": …}` when the wait is tied to an external job; else `null`. |
| `tags` | Routing hints from the policy. Become SNS message attributes, so filter policies can use them. |

### Deduplicate on `type` + `interrupt_id`

Not on `event_id`. The same question is republished whenever a start message is
redelivered or an operator calls `republish()`, and each publish mints a fresh
`event_id` — so deduplicating on it would show the same approval twice.

`interrupt_id` is stable across re-entry and resume-from-checkpoint (verified against
langgraph 1.2.11 in `test_spike_langgraph.py`), and `type` keeps a close from cancelling
out its own open. The `WaitEnvelope.dedupe_key` property is exactly `"{type}:{interrupt_id}"`.

### "Advisory" means the library does not enforce it

`allowed_actions`, `expires_at` and `default` are published so that *you* can act on
them. agent-wait does not check an answer against `allowed_actions`, does not notice when
`expires_at` passes, and never sends `default` itself. It cannot: since v0.2 no answer
passes through this library. See `migrating-from-0.1.md`.

### What a consumer should do with it

1. **Dedupe on `type` + `interrupt_id`.**
2. **Render `question` and offer exactly `allowed_actions`.** Nothing will refuse a
   third button, which is precisely why you should not draw one.
3. **Answer by copying `reply_with`** — set `answer`, post it to `reply_to`. Do not
   hardcode an endpoint; `reply_to` is how the agent tells you where it listens, and it
   differs per environment.
4. **Treat `wait.resumed` as "close the ticket".** It may arrive because somebody else
   answered, or because a timeout sweep sent the default.

---

## 2. The answer message (inbound)

Send this to `reply_to`. On an SQS FIFO entry point, use `MessageGroupId = thread_id`.

```json
{
  "thread_id": "order-4471",
  "interrupt_id": "a1b2c3d4e5f60718",
  "answer": { "action": "approve", "note": "within budget" }
}
```

| Field | Required | Meaning |
|---|---|---|
| `thread_id` | yes | Which conversation. |
| `interrupt_id` | yes | Which question. Its presence is what makes this a resume. |
| `answer` | yes | **Returned verbatim** by the `ask()` call that raised the question. |

`answer` is whatever you want it to be. The library does not merge anything into it, does
not add an `action` key, and does not validate it — a string, a number and a nested object
are all fine, and the node receives exactly what you sent. If your graph reads
`decision["action"]`, then send `{"action": "approve"}`.

There is no token and no `answer_id`. Authentication is your entry point's business — the
queue policy, the API Gateway authoriser, the IAM role allowed to `SendMessage`. agent-wait
deliberately does not pretend otherwise.

---

## 3. The start message (inbound)

The ordinary "please run" message, unchanged from whatever you had before.

```json
{
  "thread_id": "order-4471",
  "input": { "order_id": "order-4471", "amount": 41000 }
}
```

**Starts and answers arrive at the same place.** There is no second endpoint to build or
secure.

---

## 4. Telling them apart

```python
if message.get("interrupt_id"):
    agent.invoke(resume_command(message), message["thread_id"])   # a resume
else:
    agent.invoke(message["input"], message["thread_id"])          # a start
```

`interrupt_id` present → resume. Absent → start. That is the whole rule, and it holds by
construction rather than by convention: the envelope ships a filled-in `reply_with` that
already contains the key, and the consumer echoes it back.

`langgraph_wait.is_answer(message)` is that check as a named function, and
`resume_command(message)` builds the `Command`. Both are pure functions.

### One more line you need

A start message for a thread that is **already parked** must not be re-invoked with its
original input — LangGraph would treat it as a fresh turn and ask the question again under
a new `interrupt_id`, which no consumer could deduplicate away.

```python
if agent.pending(thread_id):
    agent.republish(thread_id)      # a redelivery; repair the announce, do not re-ask
else:
    agent.invoke(message["input"], thread_id)
```

`examples/refund_agent/handler.py` is the whole router, with both guards, in a dozen lines.

---

## 5. What you own

The list is short, and it is the price of the library being this small.

| Concern | Who does it now | How |
|---|---|---|
| Deciding start vs resume | you | `interrupt_id` present or not |
| Not re-asking a parked thread | you | `pending()` then `republish()` |
| Enforcing `expires_at` | you | a scheduled sweep over your open questions, sending `default` |
| Authenticating an answer | you | the queue policy / authoriser in front of `reply_to` |
| Rejecting a stale answer | you | `pending()` before invoking; see `is_still_open()` |
| Serialising two answers to one question | your transport | SQS FIFO with `MessageGroupId = thread_id` |

The last one is the one to think hardest about. `pending()` narrows the window but does
not close it: two answers a millisecond apart can both see the question open. On a FIFO
queue keyed by thread they are delivered in order and the second finds it closed. Without
that ordering guarantee, you need a conditional write of your own.
