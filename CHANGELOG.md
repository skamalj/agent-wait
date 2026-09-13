# Changelog

## 0.7.0 — 2026-09-13

**Strands Agents.** `pip install agent-wait[strands]` adds `agent_wait.strands` with the
same two names, `wait` and `publish_interrupts`, bound to Strands' tool interrupts:
`@wait` on a `@tool(context=True)` function calls `tool_context.interrupt("agent_wait",
reason=<packed question>)`; the run stops with `stop_reason == "interrupt"`;
`publish_interrupts` reads `result.interrupts`; `question_id` is the interrupt id (one
per tool call). The answer goes back verbatim through
`agent([{"interruptResponse": {"interruptId": id, "response": answer}}])` and the
wrapper applies its usual rules. A bare `tool_context.interrupt(...)` from a tool that
never heard of the library is published with the default policy. Async mode reads
`agent.session_id`.

No change to the core or the `Framework` interface. The implementor is 40 lines.

Documented per framework: a session manager is what keeps the parked question alive
between the return and the resume; a duplicate answer, an unknown id, or a plain prompt
to a parked run all raise and run nothing. Verified against 1.55.1, including resume
from a fresh `Agent` on the same `FileSessionManager` session.

## 0.6.0 — 2026-09-12

**Pydantic AI.** `pip install agent-wait[pydantic-ai]` adds `agent_wait.pydantic_ai`
with the same two names, `wait` and `publish_interrupts`, bound to Pydantic AI's deferred
tools: `@wait` raises `ApprovalRequired` with the packed question as metadata; the run
ends with `DeferredToolRequests`; `publish_interrupts` reads it; `question_id` is the
`tool_call_id`. On the approved re-run the answer arrives through
`DeferredToolResults.metadata`, so a `decision` parameter and edited `args` work exactly
as on LangGraph; `ToolDenied` is the framework's own "returned in its place".
`requires_approval=True` tools and `CallDeferred` calls are published with the default
policy. Async mode reads `thread_id` from `deps`.

No change to the core, the `Framework` interface, the envelope or the answer shape. The
implementor is 50 lines; the contract test in `tests/core` did not change.

Documented per framework: Pydantic AI has no checkpointer, so the host stores
`message_history` under `thread_id` and must store the post-run history before
acknowledging an answer — a duplicate is refused only if the stored history already holds
the tool's return. The `RunContext` parameter must be named `ctx`.

## 0.5.0 — 2026-09-12

**One package.** `langgraph-wait` and `agent-wait-aws` are folded into `agent-wait` as
subpackages behind extras. `pip install agent-wait` is the framework-free core and
depends on nothing; `agent-wait[langgraph]` adds `agent_wait.langgraph`;
`agent-wait[aws]` adds `agent_wait.aws`. Importing a subpackage without its extra raises
an `ImportError` that names the extra to install.

**`@hitl` is `@wait`.** The decorator makes a function wait for something outside the
process; a person is the common case, not the only one — a vendor callback, a payment
processor, another agent are the same shape. The rename says what it does; the metadata
still says "human-in-the-loop" first, because that is what people search for.

**Interface and implementors.** `@wait` and `publish_interrupts` are now written once in
the core against a three-method `Framework` interface (`interrupt`, `interrupts_in`,
`current_thread_id`); `agent_wait.langgraph` is `LangGraphFramework` plus two lines of
binding. Announcers keep the same split: `BaseAnnounce` in the core, providers as
implementors. A `strands` extra follows the same pattern. Core exports `Framework`,
`make_wait`, `make_publish_interrupts`, `question_id_for`.

Migration is two lines: `from langgraph_wait import hitl, publish_interrupts` →
`from agent_wait.langgraph import wait, publish_interrupts`; `from agent_wait_aws import …`
→ `from agent_wait.aws import …`. The envelope, the answer shape, and every behaviour are
unchanged; the decorated function's tag is `__agent_wait__`.

The `langgraph-wait` and `agent-wait-aws` distributions are not updated past 0.4.1.

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
