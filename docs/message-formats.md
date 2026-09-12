# Message formats

This is the contract. One document agent-wait **sends**, and a recommended shape for what
a consumer **sends back**. The first is precise and tested field by field; the second is
guidance — agent-wait never receives it, so nothing enforces it, but a consumer and a
host that both follow it need no other agreement.

---

## 1. The envelope (outbound)

One per question, emitted through every announcer. This is what arrives on your topic,
your queue, your webhook, or as a row in your table.

```json
{
  "type": "wait.created",
  "event_id": "01JAV3K2R8Q9WZ7M4X1YB6N0PD",
  "thread_id": "order-4471",
  "question_id": "a1b2c3d4e5f60718",
  "question": {
    "function": "issue_refund",
    "args": { "order_id": "order-4471", "amount": 41000 }
  },
  "allowed_actions": ["approve", "reject"],
  "expires_at": "2026-09-14T09:00:00Z",
  "default": { "action": "reject", "reason": "no response within P3D" },
  "answer_ttl": "PT15M",
  "source": { "function": "issue_refund" },
  "reply_to": null,
  "reply_with": {
    "thread_id": "order-4471",
    "question_id": "a1b2c3d4e5f60718",
    "answer": null
  },
  "correlation": null,
  "tags": { "approver_group": "finance" }
}
```

| Field | Type | Meaning |
|---|---|---|
| `type` | string | Always `wait.created`. |
| `event_id` | ULID | Fresh on every publish. For logs and tracing. **Not** the deduplication key. |
| `thread_id` | string | The agent's conversation identity. |
| `question_id` | string | The question's identity. In interrupt mode it is the framework's own id — LangGraph's `Interrupt.id`, Pydantic AI's `tool_call_id`; in async mode it is `sha256(thread_id | function | args)`. Stable across republishes. |
| `question` | any | From `@wait`: `{"function": name, "args": {...}}`, the call that is waiting. From a bare LangGraph `interrupt(value)`: `value` as is; from a Pydantic AI `requires_approval` or `CallDeferred` tool: `{"function", "args"}` plus any `metadata`. Opaque to every adapter. |
| `allowed_actions` | string[] | Which answers are meaningful. Advisory. |
| `expires_at` | RFC 3339 UTC or null | When the asker considers the question stale. Advisory. Measured from publish time unless the caller supplied `asked_at`. |
| `default` | any or null | What the asker said to assume if nobody answers. Advisory. |
| `answer_ttl` | ISO 8601 duration or null | How long an answer stays usable after it is given. Advisory. |
| `source` | object or null | `{"function": name}` for a `@wait` question; null for a bare interrupt. |
| `reply_to` | object or null | A hint for where to send the answer, if the host chose to publish one. Null otherwise. |
| `reply_with` | object | A filled-in reply. Copy it, set `answer`, send it to the agent's entry point. |
| `correlation` | object or null | `{"provider": ..., "id": ...}` when the question is tied to an external job. |
| `tags` | object | Routing hints from the policy. Become SNS message attributes, so filter policies can use them. |

### Deduplicate on `type` + `question_id`

The same question **will** be published more than once — a redelivered start message
re-runs the thread, and LangGraph hands back the same `Interrupt.id`; an async tool
called twice with the same arguments produces the same derived id. Each publish mints a
fresh `event_id`, so deduplicating on that would show the same approval twice.

`WaitEnvelope.dedupe_key` is `"wait.created:<question_id>"`. `SqsAnnounce` sends it as
the FIFO `MessageDeduplicationId`; `DynamoDbAnnounce` uses it as the item key;
`WebhookAnnounce` sends it as `X-Agent-Wait-Dedupe-Key`.

### Everything marked *advisory* is exactly that

agent-wait publishes `allowed_actions`, `expires_at`, `default` and `answer_ttl` so that
whoever consumes the envelope has the asker's intent in machine-readable form. Nothing
in agent-wait acts on any of them — it never sees the answer.

---

## 2. The answer (inbound) — recommended

agent-wait does not receive this. It is the shape a consumer should send and a host
should expect, so that "is this message a new request or an answer?" is one line.

```json
{
  "thread_id": "order-4471",
  "question_id": "a1b2c3d4e5f60718",
  "answer": { "action": "approve", "note": "within budget" },
  "valid_until": "2026-09-12T09:00:00Z"
}
```

| Field | | Meaning |
|---|---|---|
| `thread_id` | required | Which conversation. Copied from `reply_with`. |
| `question_id` | required | Which question. Copied from `reply_with`. **Its presence is what makes this an answer.** |
| `answer` | required | Received **verbatim** by the `@wait` function. Any JSON. |
| `valid_until` | optional | The answer's own expiry, if the approver wants one. |

`answer` is whatever the `@wait` function expects. Nothing is merged into it or added to
it. For a function without a `decision` parameter, `{"action": "approve"}` runs it,
`{"action": "approve", "args": {...}}` runs it with those arguments, and anything else is
returned in its place. For a function with a `decision` parameter (or whatever name
`@wait(decision=...)` was given), the whole `answer` arrives there, whatever it is.

---

## 3. The start message (inbound) — recommended

Whatever your agent already accepted. Only its *absence* of `question_id` matters.

```json
{ "thread_id": "order-4471", "input": { "order_id": "order-4471", "amount": 41000 } }
```

---

## 4. What a host does with an answer

Interrupt mode, LangGraph:

```python
if "question_id" in message:
    value = Command(resume={message["question_id"]: message["answer"]})
else:
    value = message["input"]

result = graph.invoke(value, {"configurable": {"thread_id": message["thread_id"]}})
publish_interrupts(result, message["thread_id"], announce)
```

That is the whole host. LangGraph loads the thread's latest checkpoint by `thread_id`,
re-runs the interrupted node from the top, and the `@wait` call receives `answer`.

Facts worth knowing, none of which the host has to code for:

- **A duplicate answer runs nothing.** A second `Command(resume=...)` for a question the
  thread has already moved past is a no-op in LangGraph. Verified on 1.2.11.
- **A different answer after the first also runs nothing.** The first decision stands.
- **`expires_at` is the consumer's to honour.** If nobody answers, the consumer decides
  the deadline has passed and sends `default` as the answer — it is an ordinary answer.
  If the consumer sends nothing, the thread waits indefinitely; that is LangGraph.
- **`valid_until` / `answer_ttl` are the consumer's and host's to honour**, if they care.

Interrupt mode, Pydantic AI — the framework has no checkpointer, so the host stores
`message_history` under `thread_id` and the answer rides in `metadata`:

```python
from pydantic_ai import DeferredToolResults, ToolApproved, ToolDenied

if "question_id" in message:
    answer, qid = message["answer"], message["question_id"]
    approved = isinstance(answer, dict) and answer.get("action") == "approve"
    result = agent.run_sync(
        None,
        message_history=load(message["thread_id"]),
        deferred_tool_results=DeferredToolResults(
            approvals={qid: ToolApproved() if approved else ToolDenied(str(answer))},
            metadata={qid: answer} if isinstance(answer, dict) else {},
        ),
    )
else:
    result = agent.run_sync(message["input"], message_history=load(message["thread_id"]))

store(message["thread_id"], result.all_messages())  # before acknowledging the message
publish_interrupts(result, message["thread_id"], announce)
```

The facts differ here, and the host has to code for one of them:

- **A duplicate answer is refused only if the stored history already holds the tool's
  return** (`UserError`). Resuming twice from the pre-resume history runs the tool twice.
  Store, then acknowledge.
- **`ToolDenied` never calls the tool**; the model receives the denial message. So the
  "anything else is returned in its place" rule is the framework's own.
- **A `CallDeferred` question** is answered with the call's *result*, in
  `DeferredToolResults.calls={qid: value}`, not `approvals`.

Async mode: nothing is parked and nothing is resumed. The decision arrives as a new
message and the host invokes the agent with it however the agent is designed to react.
The framework has no memory of the question; if you want one, the `DynamoDbAnnounce` row
(or anything else you keep) is it.

---

## 5. What an async `@wait` call returns

In `mode="async"` the decorated function does not run. The call returns this to whatever
called it — the node, or the model via the tool result:

```json
{ "status": "pending_approval", "question_id": "7f3e…", "function": "issue_refund" }
```

The `question_id` is the same one on the envelope, so a graph that keeps its own record
can match the decision when it arrives.
