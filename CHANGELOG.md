# Changelog

## 0.4.1 — 2026-09-11

Documentation only, after review. A dedicated Announcers page with every shipped
announcer's constructor and behaviour; the configurable `decision` parameter name shown
next to its example; the announcer guide describes one way to write one. No code changes.

## 0.4.0 — 2026-09-11

**`@hitl` is the only way to declare a question.** `ask()` is gone; the decorator makes
the function it sits on interruptible. A function with a `decision` parameter (name
configurable with `decision=`) always runs and receives the answer; one without runs only
on `{"action": "approve"}`. `question` is `{"function", "args"}`; `source` is
`{"function"}`.

**`HumanInTheLoopMiddleware` support removed.** It interrupts before the tool, `@hitl`
inside it; both on one tool is two interrupts. Documented as incompatible. The `langchain`
dev dependency and the policy registry went with it.

Public surface of `langgraph-wait` is now `hitl` and `publish_interrupts`.

## 0.3.0 — 2026-09-11

**Publish only.** The `WaitPublisher` wrapper, `pending()`, `republish()`, `is_answer()`
and `resume_command()` are gone. The library is `ask()` / `@hitl` in the graph and
`publish_interrupts(result, thread_id, announce)` after the run — it reads
`result["__interrupt__"]` and nothing else. No graph handle, no `get_state()`.

**`@hitl`.** A tool decorator. `mode="interrupt"` calls `ask()` before the tool runs;
`mode="async"` publishes from inside the tool through the decorator's own announcers and
returns pending without parking the thread. Both register the policy by tool name.

**`HumanInTheLoopMiddleware`** (langchain ≥ 1.0) batches are understood: one envelope
for the batch, the answer is `{"decisions": [...]}`.

**Schema.** `PendingInterrupt` → `Question`; `interrupt_id` → `question_id`; single
transition `wait.created`; policy gains `answer_ttl`; envelope gains `source`. The
recommended answer message is documented, not received.

**Removed from the receive path entirely.** `DynamoDbAnnounce` writes the row and never
reads it. Nothing in the library checks, dedupes or expires an answer — LangGraph ignores
a duplicate resume on its own (verified), and the rest is the consumer's.


## 0.2.1 — 2026-09-11

Documentation only. The README and package description now say what the library is for
without underselling it: LangGraph hands an interrupt to whatever called `invoke()` and
stops there — there is no built-in way to get it to anyone else, or for anyone else to
answer, regardless of whether the process sticks around. Also adds the
[Writing an announcer](https://skamalj.github.io/agent-wait/writing-an-announcer/) guide.
No code changes.

## 0.2.0 — 2026-09-11

First public release.

**The library publishes interrupts and stops.** One call — `WaitPublisher.invoke()` —
runs the graph and publishes an envelope for each question it parked on, by diffing the
framework's own pending set before and after. `pending()` reads that set; `republish()`
re-announces it. `ask()` declares a question and its advisory policy at the interrupt
site. There is no store, no inbound handling, no credential, no timer.

**Announcers.** `BaseAnnounce` implements the never-raise contract once; subclasses write
`deliver()`. Shipped: `WebhookAnnounce` (signed, stdlib), `LogAnnounce`,
`InMemoryAnnounce`, and in `agent-wait-aws`: `SnsAnnounce`, `SqsAnnounce`,
`EventBridgeAnnounce`, `DynamoDbAnnounce`.

**Verified against langgraph 1.2.11**, with the observations recorded in
`reports/langgraph-spike-observations.txt` and pinned by tests: interrupt ids are stable
across re-entry; `tasks[*].interrupts` over-reports after a partial parallel resume
(`task.result` is the discriminator); two interrupting tools in one `ToolNode` share an id.

### Coming from 0.1

0.1 (tagged, never published) owned the whole round trip — tokens, a wait store, leases,
a scheduler-driven timeout, an inbound `dispatch()`. All of it was removed.
