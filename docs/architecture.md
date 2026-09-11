# Architecture

## The shape

```mermaid
flowchart LR
    subgraph host["your process"]
        Q[["entry point"]] --> R{"question_id<br/>in message?"}
        R -->|no| I["graph.invoke(input, cfg)"]
        R -->|yes| RS["graph.invoke(Command(resume=...), cfg)"]
        I --> G[["LangGraph"]]
        RS --> G
        G -.->|"ask() parks a node"| CP[(checkpointer)]
        G --> P["publish_interrupts(result, thread_id, announce)"]
    end
    P -->|"one envelope per Interrupt"| A[["announcers"]]
    A --> W(["topic · queue · webhook · table"])
    W --> H([a human, or a system])
    H -->|"reply_with + answer"| Q
```

The library is the `P` box. Everything else in the host is yours, and it is one `if`.

## What `publish_interrupts` reads, and why it needs nothing else

`graph.invoke()` returns the run's output. If a node called `interrupt()`, the output
carries `__interrupt__`: a sequence of `Interrupt` objects. Per the LangGraph reference,
an `Interrupt` has two attributes — `value` (what was passed to `interrupt()`) and `id`.
`ns`, `when` and `resumable` were removed in 0.6.

That is the entire input. `ask()` packed the question and the policy into `value`;
`id` is what the answer has to carry back; `thread_id` is the one thing the result does
not echo, so the host passes it. No graph handle, no `get_state()`, no checkpointer —
the library never touches persistence, and it is not in the process when the answer
arrives.

```python
for item in result["__interrupt__"]:
    question, policy = unwrap(item.value)
    envelopes.append(build_envelope(thread_id, Question(item.id, question, policy)))
```

## Why not `get_state()`

It would work, and it over-reports. After one of two parallel interrupts is resumed,
`get_state().tasks[*].interrupts` still lists the finished task's id (langgraph #4796 /
#6792; reproduced on 1.2.11 in `test_spike_langgraph.py`). `result["__interrupt__"]` lists
only what *this run* raised, which is exactly the set to publish. Reading the result
instead of the state removed a filter, a caveat, and a whole class of bug.

## Three interrupt shapes, one envelope

| raised by | `Interrupt.value` | published as |
|---|---|---|
| `ask(question, policy)` | `{"question": …, "__wait__": policy}` | one envelope, that policy |
| bare `interrupt(value)` | `value` | one envelope, default policy |
| `HumanInTheLoopMiddleware` | `{"action_requests": […], "review_configs": […]}` | **one envelope for the batch**; policy from `@hitl` on the first tool |

The middleware batches every tool call needing review into one interrupt, and its answer
is `{"decisions": […]}` in batch order. That also sidesteps the ToolNode same-id problem
below, because there is only one interrupt.

## The two modes of `@hitl`

**Interrupt** (default): the wrapper calls `ask({"tool", "args"}, policy)` before the
tool body. The thread parks. `publish_interrupts` after the run announces it.
`{"action": "approve"}` runs the tool; `{"action": "approve", "args": {…}}` runs it with
those; anything else is returned to the model as the tool's result, unexecuted.

**Async**: the wrapper *is* the publisher. It announces through the decorator's own
announcers, with `question_id = sha256(thread | tool | args)`, and returns
`{"status": "pending_approval", "question_id"}` without running the body. Nothing is
parked; nothing is called after the run; the graph carries on. The decision arrives as a
new message and the graph acts on it however it was designed to.

The difference is *where* publishing happens — inside the tool at call time, or after
the run from the result — and that in async mode LangGraph remembers nothing. The
envelope, the announcers and the recommended answer shape are identical.

## Identity

| | value | stable? |
|---|---|---|
| `question_id` | `Interrupt.id` (interrupt mode) or `sha256(thread\|tool\|args)` (async) | for as long as the question stands |
| `event_id` | a fresh ULID | no — one per publish |
| `dedupe_key` | `"wait.created:<question_id>"` | yes |

Consumers deduplicate on `dedupe_key`. The same question is published again whenever a
start message is redelivered (LangGraph hands back the same `Interrupt.id` on re-entry —
verified) or an async tool is called again with the same arguments. `SqsAnnounce` sends
the key as the FIFO deduplication id; `DynamoDbAnnounce` uses it as the item key;
`WebhookAnnounce` sends it as a header.

## What LangGraph does with a duplicate answer

Nothing. A `Command(resume=…)` for a question the thread has already moved past re-runs
no node and returns the current state — verified on 1.2.11. So the host does not check
for duplicates, and the library does not offer a way to. The same holds for a *different*
answer arriving after the first: the first stands.

## LangGraph 1.2.x limitation: two interrupting tools in one `ToolNode`

Recorded by the spike, not worked around:

```text
first invoke raised      = [('<id-L>', {'which': 'a', 'x': '1'})]
after resuming it        = [('<id-L>', {'which': 'b', 'x': '2'})]
same id for both?        = True
```

Only one surfaces per run (#6624), and the second carries the *same id* as the first
(#6626): a different question under an identical `dedupe_key`, which a consumer would
discard. `Interrupt.id` is a hash of the checkpoint namespace alone; the counter that
routes resume values back to the right `interrupt()` call is not part of it. The rule is
**one `interrupt()` per node**, or `HumanInTheLoopMiddleware`, which batches. The spike
asserts the bug is present so a LangGraph fix fails the test and this section gets
removed.

## Failure isolation

`CompositeAnnounce` catches everything an adapter raises and logs it; `BaseAnnounce` does
the same for its subclasses' `deliver()`. A Slack outage must not fail a run that has
already parked. There is no retry: the next redelivery republishes with the same
`dedupe_key`, and that is the retry.

## What is not here, on purpose

No store, no tokens, no leases, no inbound validation, no timers, no `get_state()`.
Each existed in an earlier version and each put the library on the receive path, where
every team's own opinions live. The receive path is documented in
`message-formats.md` as a recommended shape; it is not implemented.
