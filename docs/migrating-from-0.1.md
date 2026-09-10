# Migrating from 0.1

v0.2 is a smaller library that does less. This page says exactly what less, so you can
decide whether the trade suits you before you take it.

## The one-paragraph version

v0.1 owned the whole round trip: it minted a token, published the question, received the
answer, verified it, decided a race with a conditional write, held a lease, ran a timer,
and resumed the graph. v0.2 publishes the question and stops. Everything after the
envelope leaves the process is yours.

## The API

| v0.1 | v0.2 |
|---|---|
| `ask(question, policy)` | unchanged |
| `WaitRuntime(adapter=, store=, tokens=, announce=, entry_point=)` | `WaitPublisher(adapter, announce=, reply_to=)` |
| `runtime.dispatch(message)` → `Start`/`Resume`/`Ignore` | gone — you decide, on `interrupt_id` |
| `runtime.register(result, config, thread_id)` | gone — folded into `invoke()` |
| `graph.invoke(...)` in your handler | `agent.invoke(value, thread_id)` |
| `runtime.sweep()` | `agent.republish(thread_id)` |
| — | `agent.pending(thread_id)` — new, and load-bearing |
| `AnnounceAdapter` | unchanged |

A handler that was

```python
outcome = runtime.dispatch(message)
if isinstance(outcome, Ignore):
    return
try:
    result = graph.invoke(outcome.input if isinstance(outcome, Start) else outcome.command,
                          outcome.config)
finally:
    runtime.register(result, outcome.config, outcome.thread_id)
```

becomes

```python
thread_id = message["thread_id"]
if is_answer(message):
    if not any(p.interrupt_id == message["interrupt_id"] for p in agent.pending(thread_id)):
        return                                    # already answered
    agent.invoke(resume_command(message), thread_id)
elif agent.pending(thread_id):
    agent.republish(thread_id)                    # a redelivered start; do not re-ask
else:
    agent.invoke(message["input"], thread_id)
```

Shorter, and every line of it is now visible to you rather than happening inside a method.
That visibility is the point and also the cost.

## What was deleted

* **`dispatch()` and its eleven `Ignore` reasons.** `token_invalid`, `expired`,
  `binding_mismatch`, `duplicate`, `already_answered`, `not_pending`, `action_not_allowed`,
  `parked`, `lease_held`, `unknown_payload`, `failed`.
* **Tokens.** `TokenCodec`, `StaticKeyProvider`, `EnvKeyProvider`,
  `SecretsManagerKeyProvider`, the `aw1.` format, key rotation, the binding hash.
* **The store.** `WaitStore`, `InMemoryWaitStore`, `SqliteWaitStore`, `DynamoWaitStore`,
  the `Wait` record, its six statuses, the conformance suite that ran against all three.
* **Leases, idempotency keys, parked answers, applied-message records.**
* **The sweeper**, and the EventBridge rule that ran it every minute.
* **The timeout.** `SchedulerAnnounce`, the schedule group, the scheduler IAM role,
  `on_timeout`.
* **`make_run_handler`**, the visibility heartbeat, the partial-batch-failure mapping.

The CDK stack loses a table, a secret, a schedule group, two IAM roles, a Lambda and a
rule. What remains is a queue, a topic, a Lambda, the checkpointer's table, and an
approvals table that only your UI reads.

## What you take on

### 1. Exactly-once on the answer side

This is the real one.

v0.1 resolved two simultaneous approvals with a compare-and-set: exactly one won, the
other came back `already_answered`, and the guarantee held regardless of transport. v0.2
has no store, so it has no conditional write.

What replaces it is `pending()` plus your transport's ordering:

```python
def is_still_open(thread_id, interrupt_id):
    return any(p.interrupt_id == interrupt_id for p in agent.pending(thread_id))
```

That reads the graph's own checkpoint, so it correctly rejects an answer for a question
the graph has moved past — a double click, a redelivered message, a second approver
clicking a minute later. It does **not** close the window between two answers that arrive
within the same instant; both can see the question open.

On SQS FIFO with `MessageGroupId = thread_id` that window does not exist, because the
queue delivers one message per thread at a time. If your entry point is an HTTP endpoint
with concurrent handlers, you need a conditional write of your own — one row per
`interrupt_id`, written before you invoke.

**Judge this against your own setup.** Behind an authenticated internal UI on a FIFO
queue, the guarantee holds and you have deleted a table for it. On a public approval link
with an HTTP entry point, v0.1's model was doing real work for you.

### 2. The timeout

The envelope carries `expires_at` (an absolute instant, already resolved from `P3D`) and
`default`. Nothing fires. You need a sweep:

```python
for row in open_questions_past(now):       # e.g. a query on the approvals table's GSI
    post_to_reply_to({**row["reply_with"], "answer": row["default"]})
```

`scenario_b` in `examples/refund_agent/demo_scenarios.py` is a working one, and
`test_the_timeout_is_the_consumers_to_enforce` is the same idea in eleven lines.

Two things v0.1 did here that your sweep should also do: check the question is still open
before sending (or you race a human answering at the deadline), and send the author's
`default` unchanged — it is published verbatim precisely so that nothing rewrites it.

### 3. Authentication

There is no token, so there is nothing to verify and nothing to leak. The flip side is
that anyone who can put a message on `reply_to` can answer any question on it. The queue
policy, the authoriser, the IAM role — those are now the entire security boundary.

If you were relying on tokens to let an approver answer from an email link without any
other credential, that capability is gone and you will need to build it: a small
authenticated endpoint that checks who the caller is and then posts to the queue.

### 4. Not re-asking a parked thread

v0.1 kept a record of applied message ids, so a redelivered start message replayed from
the checkpoint instead of re-applying the input. v0.2 has no such record, and re-invoking
a parked thread with its original input makes LangGraph ask the question a *second* time
under a new `interrupt_id` — a duplicate no consumer can detect.

The replacement is one branch in your router, shown above and in the example. It is easy
to get right and easy to forget, which is why `pending()` is public and why it is called
out here rather than buried.

## What got better

Not everything is a loss, or there would be no reason to move.

* **The answer reaches the node verbatim.** v0.1 merged the envelope's `action` into the
  payload and had a rule about which won — a rule that had a smuggling hole in its first
  draft, then an amendment (§18.1), then an amendment to the amendment (§18.1a). v0.2 has
  no merge, so it has no rule: `answer` is what `ask()` returns.
* **`expires_at` no longer drifts.** It is anchored to the checkpoint timestamp rather
  than to the clock at publish time, so republishing a question does not push its deadline
  out.
* **An announcer can be anything.** Nothing reads state back through this library, so
  "announce" just means "put the question where whoever answers it will find it" — a
  table, a Redis key, a Postgres row. `DynamoDbAnnounce` is thirty lines and gives an
  approvals UI a `Query` with no broker involved.
* **Far less to operate.** No table to provision, no secret to rotate, no sweeper to
  monitor, no lease to explain.

## Staying on 0.1

`v0.1.0` is tagged and its `TEST_REPORT.md` stands. If the exactly-once guarantee on the
answer side is load-bearing for you and your entry point cannot serialise per thread,
staying there is a defensible choice, not a fallback.
