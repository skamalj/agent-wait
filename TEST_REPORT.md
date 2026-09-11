# TEST_REPORT — agent-wait v0.3.0

Supersedes the v0.2 report. v0.1's is preserved at tag `v0.1.0`.

## 0. Read this first

**The library is now one call after the run.** `publish_interrupts(result, thread_id,
announce)` reads `result["__interrupt__"]` and hands envelopes to announcers. It holds no
graph, reads no state, receives nothing. `ask()` and `@hitl` declare questions in the
graph; `@hitl(mode="async")` publishes from inside the tool without parking the thread.

**Verified:** local unit and integration levels including real `create_agent` +
`HumanInTheLoopMiddleware`; every announcer against moto or a real local HTTP server;
the smoke test against the built wheels from a clean venv. **Not verified:** the deployed
AWS run (§5), and the PyPI round trip until 0.3.0 is published — the release workflow
does that on the tag.

---

## 1. Summary

| | v0.2 | v0.3 |
|---|---|---|
| Host calls | `agent.invoke()` on a wrapper; `pending()`, `republish()`, `is_answer()`, `resume_command()` | `graph.invoke()` (yours) then `publish_interrupts()` |
| Reads framework state | yes — `get_state()` twice per invoke | never |
| Receive path | guards in the router, using library calls | none; documented recommended shape only |
| Ways to raise a question | `ask()` | `ask()`, `@hitl` (interrupt), `@hitl` (async), middleware batches |
| Transitions | `created`, `resumed` | `created` |
| Source lines | 1,302 | 1,398 (the `@hitl` tag and middleware support are new; the wrapper and adapter are gone) |
| Tests | 147 | 133 |
| Coverage | 99% | 98% |

## 2. What was built

**`agent_wait`** — `Question`, `WaitPolicy` (+ `answer_ttl`), `WaitEnvelope`
(+ `answer_ttl`, `source`), `build_envelope()`, `publish()`, `BaseAnnounce` and the four
core announcers. No dependencies. pyright strict.

**`langgraph_wait`** — `ask(question, policy, source=)`, `@hitl(policy, mode=,
announce=)`, `questions_in(result)`, `publish_interrupts(result, thread_id, announce)`.
Three interrupt shapes: `ask()`, bare `interrupt()`, `HumanInTheLoopMiddleware`.

**`agent_wait_aws`** — SNS, SQS, EventBridge, DynamoDB announcers. `DynamoDbAnnounce`
writes one row per question and never reads it.

**`examples/refund_agent`** — the graph, a host that is one `if` plus two calls, and
three deployed scenarios (not run; see §5).

## 3. What was removed since 0.2, and why

| Removed | Reason |
|---|---|
| `WaitPublisher`, `FrameworkAdapter`, `LangGraphAdapter` | A wrapper the host had to learn, to do one thing |
| `pending()` | Read `get_state()`. Duplicate resumes are no-ops in LangGraph (verified), so it guarded nothing |
| `republish()` | A redelivered start re-runs the thread and gets the same `Interrupt.id` back; publishing again is the same as publishing |
| `is_answer()`, `resume_command()` | Two lines, documented instead |
| `wait.resumed` | Needed a before/after state diff; the library no longer reads state |
| `DynamoDbAnnounce.get/close/overdue` | Added and removed within this release: a ledger on one adapter is the receive path by the back door. On SQS-only there is no ledger, and the library must not assume one |

## 4. How it was tested

**Core (36 tests).** `publish()` and `build_envelope()` against `Question`s: one envelope
per question to every announcer; nothing in, nothing out; the documented field set
asserted key by key; `reply_to` optional; `expires_at` from `asked_at` when known and
from publish time otherwise; `dedupe_key` stable across publishes with fresh `event_id`s;
a raising announcer contained. `BaseAnnounce`, `LogAnnounce` (question never at INFO),
`WebhookAnnounce` against a real `http.server` — headers, body, HMAC verification, a 503
and an unreachable host both becoming log lines.

**LangGraph (52 tests).**
- `publish_interrupts` against real `graph.invoke()` results: `ask()` shape with policy
  and `source`; bare `interrupt()`; no interrupt; parallel nodes; a `stream()` chunk; the
  round trip through `reply_with` with the answer verbatim.
- `@hitl` interrupt mode on a real graph: parks and publishes `{tool, args}` with
  `source`; approve runs; approve with edited args runs with them; reject returns the
  decision unexecuted. Async mode: publishes through the decorator's announcers, returns
  pending, the next node runs in the same invoke, nothing parked; deterministic ids across
  calls and distinct across threads; `announce=` required at decoration time;
  `publish_interrupts` afterwards is a no-op.
- `HumanInTheLoopMiddleware` via `create_agent` and a fake tool-calling model: two tool
  calls → one interrupt → one `Question` with `actions`/`review`, the `@hitl` policy of
  the first tool, `source.via`; decisions resumed in batch order — approved ran, rejected
  did not.
- Integration (refund agent, in-memory and SQLite checkpointers): a host of one `if`;
  duplicate answer → one refund; different answer after the first → first stands;
  the consumer sending `default` as an ordinary answer; parallel; subgraph.
- The spike: 12 recorded observations against 1.2.11, byte-stable artifact.

**AWS-local (29 tests).** All four announcers over moto; the DynamoDB row shape; the
GSI query; overwrite on republish; the example's checkpointer.

### Results

```
133 passed, 4 skipped (the e2e level, opt-in)
ruff check      clean
ruff format     clean
pyright strict  0 errors
coverage        98% (497 statements, 9 missed)
```

## 5. What is not verified

**The deployed AWS run.** `examples/refund_agent/demo_scenarios.py` is rewritten for 0.3
(three scenarios; the consumer decides the deadline in B) and has not been run against a
real account. The stack synthesises; the bundle builds. What only a deployment settles:
`DynamoDbAnnounce` against real DynamoDB, the Lambda's IAM grants, and the SQS
redelivery paths under a real queue.

**Async mode with a real model.** The middleware and tag tests drive a fake tool-calling
model. The tag's behaviour does not depend on the model, but "the model sees pending and
says something sensible" is not something a fake can show.

## 6. Deviations

1. **Two guards were dropped from the host, not moved.** v0.2's router checked
   `pending()` before resuming and before re-invoking a parked thread. A probe showed
   LangGraph ignores a `Command(resume=…)` for a question the thread has moved past — it
   re-runs no node and returns the state — and that a redelivered start reproduces the
   same `Interrupt.id`. Both guards were guarding against things that do not happen. The
   facts are asserted in `test_integration_refund.py` rather than coded around.

2. **`get/close/overdue` on `DynamoDbAnnounce` were added and then removed** in this
   release, at the owner's direction. A ledger reachable only through one announcer is a
   receive-side dependency in disguise; a host on SQS alone would have nothing. The
   adapter writes the row and stops. The row shape and the `by_status` GSI are documented
   so a host can build its own ledger if it wants one.

3. **The example host lost its answer checks** for the same reason. It is one `if` and
   two calls. The recommended answer shape and the three facts a host can rely on are in
   `message-formats.md` §4.

4. **`expires_at` is measured from publish time** unless the caller supplies
   `asked_at`. `Interrupt` carries no timestamp, and reading the checkpoint for one would
   reintroduce `get_state()`. Documented; a republished question therefore gets a later
   deadline. The v0.2 "drift" fix required state; v0.3 accepts the drift and says so.

5. **Async announcers are decorator-only.** A first cut also read them from
   `config["configurable"]["announce"]`; removed. One place, checked at decoration time,
   so a missing announcer fails at import rather than three days later in a Lambda.

6. **`langchain >= 1.0` is a dev dependency only**, for the middleware tests.
   `langgraph_wait` recognises the `HITLRequest` shape structurally and does not import
   `langchain`.

7. **The spike's over-report finding is kept but no longer load-bearing.** The library
   reads `result["__interrupt__"]`, which does not over-report. The observation stays in
   the artifact for anyone who does read state, with the wording updated.

## 7. Open questions

None for the library. Whether to deploy and run the three AWS scenarios before calling
0.3.0 verified end to end is the owner's call, as before.
