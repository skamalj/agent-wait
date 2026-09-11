# TEST_REPORT — agent-wait v0.2

Supersedes the v0.1 report, which is preserved at tag `v0.1.0` and still describes what
was delivered there.

## 0. Read this first

**The end-to-end level has not been run for v0.2.** Everything below the AWS-local line is
verified; the deployed run is written and not yet executed against a real account. §5 says
exactly what that leaves unproven. v0.1's e2e evidence in `reports/` describes a system
that no longer exists.

---

## 1. Summary

v0.2 removes roughly two thirds of the library. Source across the three packages went from
3,438 lines to 1,302. What was removed is not abstraction — it is features, listed in §3,
with the guarantees they carried moved explicitly to the caller.

| | v0.1 | v0.2 |
|---|---|---|
| Calls a host makes | `dispatch()`, `graph.invoke()`, `register()` | `invoke()` |
| Public types | 30 exported | 22 exported, and most are types not machinery |
| Source lines | 3,438 | 1,302 |
| Durable state the library owns | a single-table store, 5 key prefixes, 3 implementations | none |
| AWS resources for the example | 2 tables, secret, schedule group, 2 roles, 2 Lambdas, rule, topic, bus, 2 queues | 2 tables, 1 Lambda, topic, 2 queues |
| Tests | 367 | 137 |

The test count falling is not a regression in rigour; it is 114 conformance tests for a
store that no longer exists, plus the dispatch decision table for a method that no longer
exists. Coverage went **up**, from 96%/100%/95% to 99% overall.

## 2. What was built

**`agent_wait`** — `WaitPublisher` with three methods (`invoke`, `pending`, `republish`),
`WaitPolicy`, `WaitEnvelope`, the `AnnounceAdapter` and `FrameworkAdapter` protocols, and
three announce adapters (log, in-memory, failing). pyright strict, no dependencies.

**`langgraph_wait`** — `ask()`, `LangGraphAdapter` (three methods), and the pure functions
`is_answer()` / `resume_command()`.

**`agent_wait_aws`** — four announce adapters and the CDK stack. `DynamoDbAnnounce` is new.

**`examples/refund_agent`** — the graph unchanged from v0.1 bar the policy fields, plus a
router that is now the interesting part, because it is where the returned guarantees live.

### The design, in one paragraph

`invoke()` runs the graph and diffs the framework's pending set before and after: what
appeared is `wait.created`, what vanished is `wait.resumed`. There is no record to keep in
step with anything, because there is no record. The envelope carries a filled-in
`reply_with` stub, so the consumer echoes back an `interrupt_id` by construction, which is
what makes the host's start-vs-resume rule a one-line check rather than a convention.

## 3. What was removed, and who now owns it

| Removed | Who owns the guarantee now |
|---|---|
| `dispatch()` and 11 `Ignore` reasons | the host's router — a dozen lines, in the example |
| Tokens, key rotation, binding hash | the queue policy in front of your entry point |
| The wait store (memory / SQLite / DynamoDB) | nobody — LangGraph's checkpoint is the state |
| Leases, idempotency keys, parked answers | the transport (SQS FIFO keyed by thread) |
| The sweeper | `republish()`, called by the router on a redelivery |
| `SchedulerAnnounce` and the timeout | a consumer sweep; a working one is `scenario_b` |
| `make_run_handler` | ~12 lines in `examples/refund_agent/handler.py` |

`docs/migrating-from-0.1.md` is the full version, including the case for staying on v0.1.

## 4. How it was tested

**Level 1 — unit (38 tests).** `WaitPublisher` against a `StubAdapter`, so the awkward
cases are three-line tests: a run that closes one question and opens another, a crash
between run and announce, an announcer that raises, an adapter that opts out of a
transition. Policy parsing, the envelope's exact field set, and `LogAnnounce` never putting
the question at INFO.

**Level 2 — LangGraph, for real (26 tests).** A real graph, a real checkpointer, real
interrupts. `test_adapter.py` covers `pending()` including the over-report filter;
`test_spike_langgraph.py` records what langgraph 1.2.11 actually does into
`reports/langgraph-spike-observations.txt`, on every run, pass or fail.

**Level 3 — the refund agent (26 tests, run twice: in-memory and SQLite checkpointers).**
Every scenario ends on `PAYMENTS_CALLED == ["order-4471"]`. Where a v0.1 test asserted "the
library refused this", the v0.2 test asserts "the router refused this, using `pending()`",
and the docstring says so — that substitution is the thing most worth checking, and it is
checked for the double click, the second decision, the redelivered answer after a crash,
and an answer for an unknown interrupt.

**Level 4 — AWS-local over moto (32 tests).** All four announce adapters: what lands where,
the routing metadata, and that every one of them logs rather than raises when its backend
is broken. Plus the example's DynamoDB checkpointer.

**Level 5 — the deployed run. Not executed.** See §0 and §5.

### Results

```
137 passed, 4 skipped (the e2e level, opt-in)   in 21s
ruff check      clean
ruff format     clean, 57 files
pyright strict  0 errors
coverage        99% (434 statements, 6 missed)
```

The six missed statements are `supports()` early-returns in adapters whose failure paths
are covered by other tests.

## 5. What is not verified

**The deployed run.** `wait_stack.py`, `demo_scenarios.py` and `test_e2e_aws.py` are
rewritten for v0.2 and have never been run against AWS.

Two of the cheap checks were done rather than assumed:

- **the stack synthesises** — `cdk synth` produces the expected template, with the wait
  store, the secret, the schedule group, both extra roles and the sweeper Lambda gone;
- **the Lambda bundle builds**, 46.7 MiB, and its copy of our four packages is
  byte-for-byte the file set in `packages/*/src` and `examples/` — checked because v0.1
  shipped a half-deleted bundle once, and the symptom was a `ModuleNotFoundError` at cold
  start. The pure-Python half also passes `compileall`. It cannot be import-tested here:
  the bundle carries manylinux wheels on purpose, so `pydantic_core` will not load on
  Windows.

Still unproven, and only a deployment settles them:

- that `DynamoDbAnnounce` behaves against real DynamoDB as it does against moto — the v0.1
  report records a double-serialisation bug in this exact area that moto did not catch;
- that `scenario_b`'s consumer-side timeout sweep works against a real deadline;
- that the router's `pending()` guard holds under a real FIFO queue with real redeliveries;
- that the Lambda's IAM grants are sufficient, now that four of them were deleted.

The third is the one I would want run before anyone relies on this. `pending()` is the
replacement for v0.1's conditional write, and the local suite exercises it under a driver
that serialises by construction. A real queue does not.

**The same-instant race.** `pending()` narrows the window between two different answers to
one question; it does not close it. On SQS FIFO keyed by thread the transport closes it. On
an HTTP entry point with concurrent handlers it is open, and the host needs its own
conditional write. This is stated in the README, the migration guide, `handler.py` and
§19.3 — four places, because it is the one guarantee that genuinely left the building.

**Crashes are simulated at the boundary, not induced.** `driver.py` raises between the
graph returning and the publisher announcing, which is the real window. It is not a killed
process. v0.1 had a `CrashStore` that killed at every store write; there is no store now, so
there is nothing equivalent to build.

## 6. Deviations

Decisions taken while building, recorded rather than escalated.

1. **`republish()` was added, and is not optional.** The design as agreed had `invoke()`
   and nothing else. A failing test showed that a redelivered start message re-invokes a
   parked thread, which makes LangGraph ask the question a *second* time under a new
   interrupt id — a duplicate no consumer can detect. My claim that the crash window was
   "self-healing" was wrong until this existed. It is four lines, it publishes without
   running the graph, and the router branch that calls it is in the example and in three
   docs.

2. **`pending()` is public.** It was going to be internal to the adapter. It is the tool
   for every guarantee handed back to the caller — is this answer still live, is this thread
   already parked — so hiding it would have made the trade unworkable.

3. **`PendingInterrupt.asked_at`, and `expires_at` anchored to it.** Computing `now +
   timeout` at publish time makes the deadline walk forward on every republish, so a thread
   retried often enough never expires. The anchor comes from LangGraph's checkpoint
   `created_at`. Found while writing the publisher, not by a test.

4. **Deduplication moved from `event_id` to `type` + `interrupt_id`.** Republishing is the
   only recovery mechanism now, so the key has to survive a republish. `event_id` is minted
   per publish and cannot. `SqsAnnounce` sends `dedupe_key` as the FIFO
   `MessageDeduplicationId` for the same reason.

5. **`on_timeout` was dropped entirely** rather than kept as an advisory field. `"fail"`
   described a thing the library did to a record it no longer keeps; publishing it would
   have been publishing an instruction nobody could follow. `timeout`, `default`,
   `allowed_actions` and `tags` are kept and published, and are documented as advisory in
   the policy's own docstring.

6. **The answer is returned verbatim.** No `action` field is merged in. This deletes
   §18.1, §18.1a and §18.2 outright — a rule, its amendment, and the amendment's amendment,
   all of which existed to govern a merge that no longer happens. `test_the_answer_reaches_
   the_node_verbatim` pins it.

7. **Two transitions, not five.** `answered`, `expired` and `cancelled` described a
   record's state, not the graph's. A wait is parked or it is not.

8. **`DynamoDbAnnounce` closes rows rather than deleting them**, and refuses to create a
   row when closing one it never opened — otherwise a `resumed` for a question announced
   before this adapter existed would leave a phantom in the approvals history.

9. **The spike test's wording was updated** so the evidence artifact names `pending()`
   rather than `still_pending()`. The observed behaviour is byte-identical to v0.1's
   artifact; only the sentence naming our own method changed.

10. **`driver.py` duplicates the example's router** rather than importing it, because the
    example builds a DynamoDB checkpointer and an SNS client at module scope. The
    duplication is nine lines and is called out in the file: if the two disagree, the
    example is authoritative, because it is the one people copy.

11. **`reply_to` made optional, at the owner's direction.** It had been a required
    constructor argument out of v0.1 habit. The library builds no return leg and never
    reads the value, so requiring it was requiring the caller to describe a mechanism that
    does not exist. It stays as an optional hint published on the envelope for a consumer
    somebody else writes; absent, the envelope says `null`.

12. **One spike-test failure in twenty runs, not reproduced.** `test_known_bug_tasks_over_
    report_after_a_partial_parallel_resume` failed once during the `reply_to` change and
    then passed 19 consecutive times, including 15 in a tight loop. That test asserts the
    LangGraph over-report *is present*; an intermittent absence would matter, so it was
    hammered. The module-scoped fixture writes `reports/langgraph-spike-observations.txt`
    on teardown and OneDrive file locks have caused exactly this kind of one-off in this
    repository before. Recorded, not chased.

## 7. Open questions

**One, for the owner.**

Should the v0.2 stack be deployed and the four scenarios run against real AWS, as v0.1 was?
It is the only way to close the rest of §5. v0.1's three most interesting defects — a Lambda that
logged nothing, a `find()` that read one page, a flaky forged-token test — were all found
by running the delivered script rather than by reading it, and there is no reason to think
this version is different. The scripts are ready; `scripts/deploy_and_e2e.ps1` builds,
deploys, runs and tears down.

Until that happens, this report claims a working library and a *written* deployment, and
those are not the same claim.
