# TEST_REPORT — agent-wait v0.1

Developer: Claude Code (local session, owner's Windows machine)
Owner: Kamaljeet Singh · Project manager: Claude (Cowork) session `claude-3e`
Date: 2026-09-09
Repository: [skamalj/agent-wait](https://github.com/skamalj/agent-wait) (private)

**Status: all five test levels implemented and green, including the real-AWS
end-to-end run. The stack was deployed to the owner's account, all four scenarios of
REQUIREMENTS §13 passed, and the stack was torn down.**

---

## 1. Environment

| | |
|---|---|
| Python | 3.12.13 (pinned via `.python-version`; uv otherwise selected 3.14) |
| Package manager | uv 0.11.19, workspace monorepo, three members |
| LangGraph | 1.2.11 · langgraph-checkpoint 4.2.0 |
| boto3 | 1.42.x · moto 5.2.3 |
| Lint / format | ruff 0.16.6 — clean |
| Types | pyright 1.1.x, **strict** on `agent_wait` — 0 errors |
| Tests | pytest 9.1.1 |
| CDK | aws-cdk-lib 2.x, CLI 2.1140.0 (Node 24.18) |
| AWS region | `ap-south-1` |
| AWS account | reached via SSO profile `AdministratorAccess-<redacted>`, role `AWSReservedSSO_AdministratorAccess`, user `skamalj_gmail`. Account id deliberately omitted. |
| Stack | `agent-wait-poc-ks`, every resource tagged `project=agent-wait` |
| Host | Windows 11, repository inside a OneDrive-synced folder |

**Which credential path was taken (§14):** the first one. AWS SSO was already live on this
machine, `aws sts get-caller-identity` succeeded, and the session deployed and ran the
end-to-end suite directly. The owner-run fallback scripts were written anyway
(`scripts/deploy_and_e2e.ps1` / `.sh`) because they are the reproduction path, and they
are what was actually used.

---

## 2. Results by level

| Level | Tooling | Tests | Result |
|---|---|---|---|
| **Unit** | pytest | 105 | ✅ all pass |
| **Conformance** | pytest, parametrised over `InMemoryWaitStore`, `SqliteWaitStore`, `DynamoWaitStore` (moto) | 114 (38 × 3 stores) | ✅ all pass |
| **LangGraph integration** | real langgraph 1.2.11, `InMemorySaver` + `SqliteSaver` | 59 (incl. 11 spike) | ✅ all pass |
| **AWS local** | moto — DynamoDB, SQS, SNS, EventBridge, Scheduler, Secrets Manager | 85 | ✅ all pass |
| **AWS end-to-end** | real account, `agent-wait-poc-ks` | 4 scenarios / 28 checks | ✅ all pass |
| | | **367 pytest tests, 0 failures** | (4 e2e tests skip unless `AGENT_WAIT_E2E=1`) |

### Coverage

| Package | Coverage | Target |
|---|---|---|
| `agent_wait` | **96%** | ≥ 85% ✅ |
| `langgraph_wait` | **100%** | ≥ 85% ✅ |
| `agent_wait_aws` | 95% | — |
| Total | 96% | |

JUnit XML and the coverage XML are committed under `reports/`.

### The correctness rules (§10, extended by §18.2)

Fourteen rules now: the twelve of §10, plus rule 13 from §18.2 and rule 14 from §18.5.
Every one has a test named after it, and every one runs against all three stores.

| Rule | Test | ×3 stores |
|---|---|---|
| 1 Idempotent create | `test_rule_01_idempotent_create` + `_crash_injection_across_register` | ✅ |
| 2 One conditional write per transition | `test_rule_02_one_conditional_write_per_transition`, `_concurrent_writers_exactly_one_wins` | ✅ |
| 3 Thread lease | `test_rule_03_thread_lease_blocks_a_second_worker` (+3 more) | ✅ |
| 4 Duplicate answers | `test_rule_04_duplicate_answer_is_a_duplicate`, `_a_different_answer_is_a_conflict` | ✅ |
| 5 Parked answers | `test_rule_05_parked_answer_is_stored_not_dropped`, `_register_applies_the_parked_answer` | ✅ |
| 6 Late timer | `test_rule_06_late_timer_is_a_noop` (+2 more) | ✅ |
| 7 Resume idempotency | `test_rule_07_resume_idempotency`, `_register_marks_answered_waits_resumed` | ✅ |
| 8 Token integrity | `test_rule_08_tampered_token_is_rejected_without_a_store_read` (+2) | ✅ |
| 9 Binding (step 2.2a, §18.6) | `test_rule_09_binding_mismatch` | ✅ |
| 10 Announce isolation | `test_rule_10_announce_isolation`, `_sweeper_reannounces_unnotified_waits` | ✅ |
| 11 Parallel interrupts | `test_rule_11_parallel_interrupts_are_independent` | ✅ |
| 12 Cancel | `test_rule_12_cancel`, `_cancel_is_not_repeatable` | ✅ |
| **13 `on_timeout="fail"`** (§18.2) | `test_rule_13_on_timeout_fail` (+2 more) | ✅ |
| **14 first-run crash keeps the input** (§18.5) | `test_rule_14_first_run_crash_before_checkpoint` (+2, crash-injected) | ✅ |

Rule 2's concurrency test is not a simulation: eight threads meet at a barrier and race
the store's real compare-and-set, and the test asserts exactly one returns `True`. It
passes against moto's DynamoDB as well as the two local stores.

**Crash injection.** `CrashStore` wraps any store and kills the process at the *N*th
mutating call. The suite discovers how many writes `register()` and `dispatch()` actually
make (rather than hardcoding a number that would rot), then walks every one of them: kill,
redeliver, assert one wait, at most one replayed announce, and `build_resume` called at
most once. That is what tests rule 1 and rule 7 honestly.

---

## 3. The LangGraph spike (§9.2)

REQUIREMENTS asked for empirical verification rather than assumption, because the
idempotency key depends on it. Findings against **langgraph 1.2.11**, all pinned as tests
in `packages/langgraph-wait/tests/test_spike_langgraph.py`:

1. **`Interrupt.id` is stable** across `invoke(None, config)` re-entry and resume from
   checkpoint. ✅ Confirmed — this is what makes
   `sha256(thread_id|interrupt_id|checkpoint_id)` survive a crash and a redelivery.
2. **`checkpoint_id` is stable** while a thread is parked, across repeated reads and
   re-invocations. ✅ Confirmed.
3. **`get_state().tasks[*].interrupts` over-reports.** ⚠️ **Bug reproduced**
   (langgraph #4796 / #6792). After resuming one of two parallel interrupts, the
   *finished* task still advertises its interrupt id. A naive `still_pending()` built on
   `task.interrupts` therefore returns `True` for an interrupt the graph has already moved
   past.

   **The discriminator is `task.result`** — populated for a finished task, `None` for a
   parked one. `LangGraphAdapter.still_pending()` checks both, and
   `test_known_bug_tasks_over_report_after_a_partial_parallel_resume` pins the behaviour so
   that a LangGraph fix arrives as a failing test rather than as silence.
4. **Replaying a resume does not repeat the side effect.** ✅ Good news, also pinned.
   LangGraph will not re-run a node whose resume it has already applied. This is a second
   line of defence; the first is the status compare-and-set in `dispatch()`, which rejects
   a redelivered answer before the graph is ever invoked.
5. **Subgraph interrupts** surface on the parent's `__interrupt__` with a stable id, and
   the parent's `get_state()` reports them against the subgraph node's task. `subgraphs=True`
   was not needed. Resuming by id through the parent works. ✅ One adapter handles both.

### Observed behaviour, verbatim

Requested by the project manager: the parallel and subgraph observations recorded exactly
as seen, pass or fail, since these are the items most likely to differ from the design.

The spike suite writes this file itself, from a module-scoped fixture whose teardown runs
whether the assertions held or not — so if a future LangGraph changes the behaviour, the
record of *what it actually did* survives the failure. Committed at
`reports/langgraph-spike-observations.txt`; reproduce with `uv run pytest -m spike`.

```text
langgraph 1.2.11 · langgraph-checkpoint 4.2.0
Recorded by packages/langgraph-wait/tests/test_spike_langgraph.py, verbatim.
Interrupt ids are truncated to 8 chars; they are regenerated every run.

== PARALLEL: two interrupts in one superstep ==
  result['__interrupt__']  = [('c4a1248b', {'which': 'a'}), ('259568a8', {'which': 'b'})]
  get_state().tasks        = [('na', ['c4a1248b'], None), ('nb', ['259568a8'], None)]
  get_state().next         = ('na', 'nb')
  checkpoint_id            = 1f1ac443
  side-effect log          = []

== PARALLEL: after resuming ONE of the two ==
  resumed                  = db5baed8 with {'ok': 1}
  result keys              = ['__interrupt__', 'a']
  result['__interrupt__']  = ['db3b5266']
  the other interrupt      = db3b5266
  state values             = {'a': {'ok': 1}}
  side-effect log          = ['a']

== PARALLEL: the #4796/#6792 bug, as observed ==
  resumed                  = dc856799 (node 'na'); still parked = 03239521 (node 'nb')
  task 'na'   interrupts=['dc856799'] result={'a': {'ok': 1}} error=None
  task 'nb'   interrupts=['03239521'] result=None error=None
  get_state().next         = ('nb',)
  -> tasks[*].interrupts over-reports: 'na' has finished and still lists its id.
  -> task.result is the discriminator, and is what still_pending() reads.

== PARALLEL: replaying an already-applied resume ==
  applied fb6c536e twice; side-effect log = ['a']
  -> LangGraph does not re-run a node whose resume it already applied.

== SUBGRAPH: an interrupt raised two levels down ==
  result['__interrupt__']  = [('244d5b98', {'inner': True})]
  get_state().tasks        = [('sub', ['244d5b98'], None)]
  get_state(subgraphs=True) = [('sub', ['244d5b98'])]
  get_state().next         = ('sub',)
  checkpoint_id            = 1f1ac443
  -> it surfaces on the PARENT's __interrupt__, against the subgraph node's task.
  -> subgraphs=True was not needed, so one adapter handles both shapes.

== SUBGRAPH: resuming it through the parent ==
  resumed 08d668ef via the parent graph
  final state values       = {'v': {'done': True}}
  get_state().tasks        = []
  -> resuming by id through the parent works; no subgraph-specific path needed.
```

Reading it against the design: `get_state().next` names only `nb`, and `task.result` is
populated for `na` and `None` for `nb` — both agree that only `nb` is genuinely parked.
`tasks[*].interrupts` is the single field that disagrees, and it is the one the obvious
implementation would have used. The subgraph case matches the design exactly: one
interrupt on the parent, one task, `subgraphs=True` unnecessary, and resume-by-id works
through the parent.

---

## 4. End-to-end run (§13)

Deployed to `agent-wait-poc-ks` in `ap-south-1`, driven by
`examples/refund_agent/demo_scenarios.py`. Everything below is real: real SQS FIFO, a real
Lambda running a real LangGraph graph against a real DynamoDB checkpointer, a real
EventBridge schedule, and a refund counter read from DynamoDB *outside* the Lambda —
because an in-memory list proves nothing about a process you are not inside.

### Scenario A — crash before `register()`, then every wrong answer at once

Thread `order-a-d09dd0`, refund of 41,000 (over the 25,000 threshold, so a human is asked).

| Time (UTC) | | |
|---|---|---|
| 11:13:39 | start message onto the FIFO queue | |
| 11:13:43 | wait `01M22Y1D95…` created and announced | ✅ one wait, one announcement |
| 11:13:44 | envelope's `reply_to` is the agent's own queue | ✅ |
| 11:13:44 | EventBridge schedule armed, named after the wait | ✅ |
| 11:13:44 | **identical start message redelivered** (what the crash produces) | |
| 11:13:56 | still exactly one wait | ✅ rule 1 |
| 11:14:06 | no second announcement | ✅ `notified_at` suppressed it |
| 11:14:06 | approve, `answer_id=click-9f1` | |
| 11:14:08 | refund counter in DynamoDB reads **1** | ✅ |
| 11:14:08 | **the same click again** | |
| 11:14:20 | counter still **1** | ✅ rule 4 — `duplicate` |
| 11:14:20 | a different decision (`reject`, new `answer_id`) | ✅ `already_answered` |
| 11:14:20 | a tampered token | ✅ `token_invalid` |
| 11:14:20 | a timeout arriving after the answer | ✅ `already_answered`, rule 6 |
| 11:14:35 | counter **still 1**; wait `resumed`; recorded action `approve` | ✅ |
| 11:14:35 | schedule deleted on answer; dead-letter queue empty | ✅ |

**13/13.** Four wrong answers against a live thread, in sequence, and the refund counter
never moved off 1.

### Scenario B — timeout, with nobody answering

Thread `order-b-a54b24`, `AGENT_WAIT_TIMEOUT=PT2M` (two minutes standing in for three days).

| Time (UTC) | | |
|---|---|---|
| 11:14:35 | start message; wait parked | |
| 11:14:37 | schedule holding the timeout, `expires_at 11:17:43Z` | ✅ |
| 11:18:08 | **EventBridge Scheduler fired on its own** — the timeout arrived at the agent's queue as an ordinary answer | ✅ |
| 11:18:08 | recorded action `timeout`; the declared default applied | ✅ |
| 11:18:08 | no refund issued | ✅ |
| 11:18:08 | approve, after the fact | |
| 11:18:23 | still no refund; wait `resumed` on the default | ✅ |

**7/7.** Under §18.1 the graph now receives `{"action": "timeout", "reason": …}` rather
than the default's own `"action": "reject"`. `route()` tests for `approve`, so the outcome
is unchanged: the thread ends `rejected` and no refund is issued.

### Scenario C — crash after the resume, before the ack

Thread `order-c-065bf0`.

| Time (UTC) | | |
|---|---|---|
| 11:18:24 | start; wait parked | |
| 11:18:28 | approve → refund issued, counter **1** | ✅ |
| 11:18:28 | **the same approval redelivered three times** (the un-acked message) | |
| 11:18:48 | counter still **1** | ✅ rule 7 |
| 11:18:48 | dead-letter queue empty — every redelivery was acknowledged, not retried | ✅ |

**4/4.**

### Scenario D — the sweeper

Thread `order-d-687d3a`. `notified_at` was stripped from a live wait and its schedule
deleted: the state a crash between the DynamoDB write and the announce leaves behind. A
wait nobody knows about, that no timer will fire for.

| Time (UTC) | | |
|---|---|---|
| 11:18:51 | wait made invisible: no `notified_at`, no schedule | ✅ |
| 11:19:09 | **the one-minute sweeper found and re-announced it** (18s) | ✅ |
| 11:19:09 | the world was told after all | ✅ |
| 11:19:09 | and the timeout was re-armed | ✅ |
| 11:19:12 | the repaired wait is answerable — approved | ✅ |
| 11:19:12 | and it refunded exactly once | ✅ |

**6/6.**

### Total

**28 / 28 checks passed**, 2026-09-09 11:13:39Z → 11:19:12Z, on the post-§18 build.
Machine-readable evidence: `reports/e2e-20260909T111912Z.json`.

This is the **second** full end-to-end run. The first (10:08:02Z, also 28/28) ran against
the pre-§18 build; because the amendments change `dispatch()` and the run handler, the
stack was rebuilt, redeployed and re-run rather than assuming the earlier evidence still
applied. Only the second run's evidence is committed.

**§18.1a and §18.5 landed after that run, and were not redeployed** — on the project
manager's instruction, since neither touches `make_run_handler` and the deployed topology
is identical. Being precise about what that leaves uncovered:

* **§18.1a** changes what the graph receives on a timeout, which is exactly scenario B.
  That path is covered locally against a **real** LangGraph graph over both `InMemorySaver`
  and `SqliteSaver` (`test_scenario_b_timeout_applies_the_default`), which asserts the
  graph now sees `{"action": "reject", …}` while the wait record still reads
  `action="timeout"`, `actor="system:timer"`. What the deployed run additionally proved —
  that EventBridge Scheduler fires unattended and the message arrives at the queue — is
  unchanged by the ruling.
* **§18.5** only fires when a *first* run crashes before its checkpoint, which the
  end-to-end suite never induced (see "what was simulated" below). It is covered by rule 14
  across three stores and by
  `test_a_first_message_that_crashes_is_retried_with_its_input`, which drives the real
  Lambda handler and a real graph.

Neither is unverified; both are verified one layer down from the deployed run.

### Two defects the re-run caught

Both were found by *using* the delivered script rather than by reading it, which is the
argument for running it at all:

1. **A flaky test of my own making.** `scripts/deploy_and_e2e.sh` runs the local suite
   first and refuses to deploy on a failure — and it refused.
   `test_a_forged_token_is_acknowledged` forged a token with `token[:-1] + "Z"`, which is
   not a forgery at all when the MAC already ends in `Z`. base64url does that about one run
   in 64, and on that run the "forged" token verified, the approval went through, and the
   refund fired. Every other forging site in the codebase already used the safe
   `("A" if token[-1] != "A" else "B")` form; this one did not. Fixed — and the gate did
   exactly what it exists to do.
2. **`deploy_and_e2e.sh` was broken under Git Bash.** It passed `$ROOT/build/lambda`, a
   POSIX path, to a CDK CLI that is a *Windows* process, which resolved it against the
   drive root and reported the Lambda asset missing. It now converts the path with
   `cygpath` where that exists, and is unchanged on Linux and macOS. The original
   end-to-end run had used the PowerShell script, which passes a native path, so the bug
   had never surfaced.

### What was simulated, and what was not

Being straight about this matters more than the pass count.

* **The crashes are simulated.** You cannot kill a managed Lambda mid-invocation from
  outside. The e2e produces the *same input a crash produces* — a redelivered start
  message, an approval applied twice — and asserts the same invariant. Genuine mid-process
  kills are induced properly in the local conformance suite, where `CrashStore` stops the
  process at every individual store write and the flow is then retried.
* **Scenario D's failed announce is simulated** by stripping `notified_at` from a live wait
  and deleting its schedule — the exact state a crash between the create and the announce
  leaves behind — rather than by breaking SNS.
* **Everything else is real**: idempotent create under redelivery, the double click, the
  stale reject, the forged token, the late timer, and the timeout genuinely firing from
  EventBridge Scheduler with no human involved.

### CloudWatch evidence

`reports/cloudwatch-run-lambda.txt` holds the run Lambda's own account of it — the
`wait.created` / `wait.answered` / `wait.resumed` transitions, and the dispatch decisions
in sequence:

```
ignoring message: duplicate (already applied; wait is resumed)
ignoring message: already_answered (wait is resumed)
ignoring message: token_invalid (bad signature)
```

That is the double click, the stale reject and the forged token, refused in order, with the
refund counter still at 1. Across the whole run the Lambda logged 4 `wait.created`,
3 `wait.answered`, 1 `wait.expired` and 4 `wait.resumed` transitions, and refused
4 `duplicate`, 3 `already_answered` and 1 `token_invalid` message.

**Zero tokens appear in the log** (`grep -c 'aw1\.'` returns 0), and no question payload
either — asserted independently by `test_announce_log.py`.

### Teardown

`cdk destroy` ran after the evidence was captured, and the result was verified per service
rather than by trusting the stack status:

```
describe-stacks agent-wait-poc-ks   -> ValidationError: stack does not exist
sqs list-queues        agent-wait   -> (none)
dynamodb list-tables   agent-wait   -> 0
lambda list-functions  agent-wait   -> 0
scheduler list-schedule-groups      -> 0
```

Nothing billable remains. Every resource carried `RemovalPolicy.DESTROY` and the tag
`project=agent-wait`.

One caveat worth writing down rather than rounding away: immediately after the delete,
`resourcegroupstaggingapi get-resources --tag project=agent-wait` still returned **1** — an
event-source mapping. `lambda get-event-source-mapping` on that UUID returns
`ResourceNotFoundException`, so the resource is gone and the tagging index is simply
eventually consistent. The per-service checks above are the ones to trust; the tag API is a
search index, not an inventory.

---

## 5. Deviations & decisions

Recorded per CLAUDE.md: where REQUIREMENTS was silent I decided and logged it; §16 was not
re-opened.

### Rulings applied — REQUIREMENTS §18 (2026-09-09)

The four open questions this report raised were ruled on by the project manager and
appended to REQUIREMENTS as **§18 Amendments**. All four are now implemented and tested;
they are recorded here as rulings, not deviations.

1. **§18.1 — resume value, merge order corrected (answer path).** `payload` confirmed as
   the answer envelope's `payload` *field*. The merge order in §4.1.9 was wrong, and the PM
   caught a real hole: `{"action": action, **payload}` lets a sender smuggle
   `{"payload": {"action": "approve"}}` past a wait whose `allowed_actions` is
   `("reject",)` — the allowed-actions check inspects the envelope's `action`, sees
   `reject` and passes, and the graph then reads `approve`. Now
   `{**payload, "action": action}`: the authorisation-checked field is the one the graph
   sees. Pinned by `test_a_payload_cannot_override_the_envelopes_action`.

2. **§18.1a — a timeout default keeps its own action.** My first implementation applied the
   same merge order to `policy.default`, which was wrong and the PM reversed it. The
   smuggling argument does not transfer: a `default` is written by the graph author, in the
   graph, next to the question, so there is no second party to defend against and
   overriding it hands an author something they did not ask for.

   A mapping default now reaches the graph **exactly as declared**, gaining
   `action: "timeout"` only when it says nothing about an action; a non-mapping default is
   untouched. `default={"action": "reject", …}` therefore arrives as `action="reject"`.

   The audit trail is where "what happened" lives, and is unaffected: the wait record and
   every announce carry `action="timeout"` and `actor="system:timer"`. *What the graph is
   told* and *what happened* are separate questions with separate answers.

   The asymmetry is guarded from both sides —
   `test_a_timeout_default_reaches_the_graph_as_the_author_wrote_it` and
   `test_a_human_answer_still_cannot_smuggle_an_action` — so relaxing the default path
   cannot quietly relax the answer path. The scenario-B assertion is back to `reject`.

3. **§18.2 — `on_timeout="fail"` gets its own reason.** `Ignore.reason` extended with
   `"failed"`. `pending → expired` (so the `expired` announce still fires and the schedule
   is deleted), then `expired → failed`; the graph is never invoked and the lease is
   released. Conformance test `test_rule_13_on_timeout_fail`, plus two more covering later
   answers and the `resume_default` contrast — all three run against all three stores.

4. **§18.3 — holding the lease across an immediate resume: approved**, and §4.2.5 amended
   to say so. `make_run_handler` now runs the follow-up `register()` in a `finally`, so a
   graph that raises still releases the lease. Without it, a lease held by a dead
   invocation would block every redelivery of that thread for its full fifteen minutes and
   a transient model error would look like a permanently stuck thread. Registering an empty
   result on the error path is safe: `extract()` finds no interrupts, so `register()` only
   finalises waits the thread has demonstrably moved past, and a graph that just raised has
   not moved past anything. Three tests in `test_run_handler.py`.

5. **§18.4 — `envelope_for()` accepted as support API.** `sweep()`, `cancel()` and
   `envelope_for()` are now documented under **"API reference — runtime operations"** in
   `docs/architecture.md`, and stay out of the README quickstart, which remains
   `ask()` / `dispatch()` / `register()`. No new entry point, no new interface.

6. **§18.5 — a first-run crash must not lose the input.** The defect §18.3 surfaced, now
   fixed. `dispatch()` records a start message as applied *before* the graph consumes it,
   so "applied" means *we started on this*, not *the framework kept the result*. A first
   run that died before LangGraph wrote a checkpoint left the message marked applied
   against a thread with nothing to resume from; the redelivery invoked with `input=None`
   and raised `EmptyInputError`, and the thread was stuck until the DLQ.

   The PM rejected my candidate fix — moving the write into `register()` — because it opens
   the opposite and worse hole: a crash *after* the checkpoint but *before* `register()`
   would leave the message unrecorded and the redelivery would re-apply the input as a
   second turn. Correct, and I had not seen it.

   The write stays in `dispatch()`. `FrameworkAdapter` gains a fifth method,
   `has_checkpoint(thread_id)`: a message already marked applied against a thread with no
   persisted state means the first run died before anything was kept, so the input goes
   back in. For LangGraph it is a `checkpoint_id` **or** non-empty `values`, because a
   thread that interrupted in its first superstep can have the former without the latter.

   Rule 14, crash-injected, plus `test_rule_14_once_checkpointed_the_input_is_not_reapplied`
   so the fix cannot quietly undo rule 3 — all three across all three stores — and
   `test_a_first_message_that_crashes_is_retried_with_its_input` end-to-end through the
   Lambda handler against a real graph.

7. **§18.6 — the binding check is step 2.2a.** The last thing in this report that was an
   *interpretation* rather than a ruling. §4.1.2 never numbered the binding check, though
   §10.9 has always required it; it is now step 2.2a, immediately after `store.get()` and
   before the allowed-action check — the order that was already implemented. The paper
   matches the code, and rule 9 in §10 cites the step.

   The order is load-bearing, not cosmetic. The binding is what ties a token to the exact
   question it answers, so it has to be settled before anything is decided on the strength
   of that token and before any write. Checking it after the allowed-action check would
   mean reasoning about a policy that may belong to a different question; checking it
   before the store read is impossible, because there is nothing to compare against yet.

### Implementation choices

8. **`Clock` / `SystemClock` / `FakeClock` live in `model.py`** rather than a `clock.py`,
   to keep §15's file list exact. `FakeClock` is shipped rather than kept in a conftest
   because the conformance suite runs against stores in another package.
9. **`announce_refs` is unused in v0.1.** Schedules are named after the `wait_id`, so
   creating and deleting one needs no stored handle. The field is kept for adapters that
   do need one.
10. **The token MAC follows §11 literally**: `base64url(HMAC-SHA256(...))[:27]` — the full
   digest base64-encoded, then truncated. The reference prototype truncated the *digest*
   first; that shape was not copied.
11. **`SchedulerAnnounce` delivers immediately when `expires_at` is already past** instead
   of creating a schedule EventBridge would reject. This is the sweeper's repair path for
   a wait whose schedule was lost while the process was down, and it preserves the
   invariant that timeouts always arrive as ordinary answers at the ordinary entry point.
12. **`SecretsManagerKeyProvider` also accepts a flat `{"current": "k1", "k1": "…"}`.**
    CloudFormation's `generate_string_key` writes one value into a template and cannot
    nest, so this lets the stack generate a real random key at deploy time — no key in
    source, no manual step after deploy.
13. **Python 3.12 pinned** with `.python-version`; uv selected 3.14 otherwise.
14. **`DynamoWaitStore.find()` follows `LastEvaluatedKey`.** Found during the deployment:
    a single query is a *page*, not an answer, and a wait the sweeper never sees is a
    thread parked forever. Fixed with a test that forces multiple pages.
15. **The example handler raises its own loggers to INFO.** Also found during deployment:
    the Lambda runtime leaves the root logger at WARNING, so `LogAnnounce` and the run
    handler were silent in production — exactly when you want to know why a message was
    ignored.

### Things added to the example, not the library

16. **`examples/refund_agent/dynamo_checkpointer.py`** — a ~150-line LangGraph
    `BaseCheckpointSaver` over DynamoDB. agent-wait never touches a checkpoint and is
    deliberately checkpointer-agnostic, but the example *must* have a durable saver or
    "resume on a machine that was not running" is untestable and all four scenarios would
    prove nothing. The published options were a stale `langgraph-checkpoint-dynamodb`
    0.1.0 and `langgraph-checkpoint-aws`, which requires Bedrock Session Management. It has
    its own 9 tests, including a graph parked by one saver instance and resumed through a
    different one.
17. **An `Announcements` SQS queue subscribed to the SNS topic**, and `issue_refund`
    mirroring its side effect into DynamoDB. Both exist so the end-to-end run can observe
    from outside the Lambda — the queue is an ordinary consumer, exactly what an approvals
    UI would be, and it is how the run gets a real token out of a real envelope instead of
    minting one for itself.
18. **`handler.py` is ~50 lines, not the "5 lines" §15 suggests**, because the wiring
    (store, tokens, four announce adapters, entry point) is written out rather than hidden
    behind a helper. The agent-wait-specific part really is two lines:
    `runtime = WaitRuntime(...)` and `handler = make_run_handler(graph, runtime)`.
19. **Files beyond §15's tree**: `scripts/` (bundle build, deploy), a root `conftest.py`
    (puts the shared conformance suite and the example on `sys.path`), and test helper
    modules (`rig.py`, `conformance.py`, `driver.py`, `parallel_graphs.py`).

### Build

20. **No Docker on the host**, so the Lambda bundle is built with
    `uv pip install --python-platform x86_64-manylinux2014`, which resolves Linux wheels
    from Windows. The whole dependency tree is pure Python. boto3/botocore are excluded —
    the runtime ships a recent one, and including them adds ~15 MB to a 50 MB limit.
    Result: 46.8 MiB unpacked.
21. **OneDrive.** The repository lives in a synced folder, which rejects hardlinks
    (`os error 396`) and briefly locks directories it is scanning. The build script passes
    `--link-mode=copy` and retries `rmtree`. Noted because it will bite anyone else on the
    same setup.

### Process

22. **Stacked pull requests**, one per package plus the proof-of-concept, rather than
    independent ones — `agent-wait` has no dependencies, but `langgraph-wait` and
    `agent-wait-aws` both import it and their tests run the core's conformance suite, so
    independent branches into `main` could not have been green on their own. They were
    stacked in dependency order and are reviewable commit by commit. `main` was never
    force-pushed and never left red.

23. **Merging a stack needs one extra step, which I got wrong first.** GitHub only
    auto-retargets a stacked pull request when its base branch is *deleted*. These were
    kept, so merging #2, #3 and #4 landed them on their intermediate base branches and
    only #1 reached `main`. #5 carried the remaining reviewed commits to `main`; no code
    changed and nothing was force-pushed. Worth knowing before anyone stacks again: either
    delete each base on merge, or expect to land the tip explicitly.

24. **The §18 rulings arrived in two rounds.** §18.1–18.4 were implemented on the tip of
    the original stack (#4). §18.1a and §18.5 came after `main` had the lot, and are #6 —
    a single reviewable PR against `main` rather than another stack.

---

25. **Decided after sign-off, recorded rather than escalated.** Four docstrings still
    said "the twelve rules" after §18.2 and §18.5 added rules 13 and 14
    (`store/memory.py`, `tests/rig.py`, and both conformance entry points). Reworded to
    "every conformance rule" so the count cannot go stale again. No behaviour change; the
    rules table in §2 is the count of record.

## 6. Known gaps

1. **Genuine mid-process crashes are only tested locally.** See §4 above. A fault-injection
   layer in the deployed Lambda (an env var that raises after `invoke()`) would close this;
   it was judged not worth adding a production code path that exists only to break things.

2. **Questions over 200 KB fail loudly** (§16.6). Blob-by-reference is a v0.2 item.
3. **Step Functions hosting is designed for, not built** (§16.10). Nothing in the core
   prevents it.
4. **The sweeper's overdue path re-announces every minute** until the wait settles. In
   practice that is one or two extra timeout messages, and the second is a `duplicate`
   because `answer_id` is derived from the `wait_id` — but it is not rate-limited.
5. **An unindexed `find()` is a table `Scan`.** Only reachable from tests and the
   no-filter case; the sweeper and `register()` always use GSI1 or GSI2.
6. **Parked answers are keyed by `wait_id` or correlation key.** Two threads sharing a
   correlation key would collide. Correlation is optional and unused by the example.
7. **No metrics or alarms.** Waits created/answered/expired, and the age of the oldest
   pending wait, are the obvious three.
8. **One framework and one cloud.** Both protocols are clean, but only one implementation
   of each has actually been exercised — which is the honest position for v0.1.
9. **`langgraph-wait` requires `langgraph >= 1.2, < 2`.** The `task.result` workaround is
   version-sensitive by nature; the spike suite is what will catch a change.

---

## 7. Open questions for the project manager

**None.** Every question this report raised has been answered and folded into
REQUIREMENTS §18 — §18.1 through §18.6 — and each is recorded in §5 above with the
reasoning that settled it. Two are worth re-reading before anyone changes this code:

* **§18.1 / §18.1a** are a deliberate *asymmetry*, not an inconsistency. An answer's
  `payload` cannot override the envelope's `action`; a timeout's `default` is not
  overridden at all. One arrives from outside and is authorisation-checked, the other is
  written by the graph author. Tests guard both directions, so collapsing them back into
  one rule fails the suite.
* **§18.5** rests on the distinction between *applied* and *persisted*. Moving the
  applied-message write out of `dispatch()` reopens the hole it closes, in the opposite
  direction. The ruling says why.

Anything found after sign-off was decided here and recorded in §5 rather than escalated.

## 8. Recommended v0.2 items

1. **A second framework adapter** — Strands or Pydantic AI. Four methods; it is the
   cheapest way to prove `FrameworkAdapter` is genuinely a protocol and not a
   LangGraph-shaped hole.
2. **Blob-by-reference for large questions**, with the store holding a pointer and the
   envelope carrying a presigned URL.
3. **A dedicated sweeper index and cursor**, so the repair pass is O(overdue) rather than
   O(pending), plus rate limiting on re-announce.
4. **Step Functions hosting**, which the design already anticipates: a Park state with
   `sqs:sendMessage.waitForTaskToken` and `TimeoutSecondsPath` would announce and time out
   natively and feed `SendTaskSuccess` output back into `dispatch()` unchanged.
5. **Metrics and an alarm on the age of the oldest pending wait.** That single number is
   the health of the whole system.
6. **`cancel()` from outside**, as a control message on the same entry point — explicitly
   out of scope for v0.1, and the obvious next verb.
7. **A key-rotation runbook**, and a `kid` in the wait record so an operator can tell which
   key a live token was signed with.
8. **Publish to PyPI** and list in LangChain's integrations docs, which is the sanctioned
   route since `langchain-ai/*` does not accept integration PRs.

---

## 9. Reproducing this

```bash
# everything local: unit, conformance over three stores, LangGraph, moto
uv sync --all-packages
uv run pytest -q

# with the evidence this report cites
uv run pytest -q --junitxml=reports/junit.xml \
  --cov=agent_wait --cov=langgraph_wait --cov=agent_wait_aws \
  --cov-report=term --cov-report=xml:reports/coverage.xml

# lint and types
uv run ruff check . && uv run ruff format --check . && uv run pyright

# one level at a time
uv run pytest -m conformance -q                       # the twelve rules × three stores
uv run pytest -m spike -q                             # what LangGraph actually does
uv run pytest packages/langgraph-wait/tests -q        # the four scenarios, locally
uv run pytest packages/agent-wait-aws/tests -q        # moto
```

The end-to-end level needs a real account and is opt-in:

```bash
# deploy, run the four scenarios, tear down
./scripts/deploy_and_e2e.sh --profile <sso-profile> --stack agent-wait-poc-<initials> --destroy

# or, against a stack that is already up
AGENT_WAIT_E2E=1 AGENT_WAIT_E2E_STACK=agent-wait-poc-<initials> uv run pytest -m e2e -q
uv run python examples/refund_agent/demo_scenarios.py --stack agent-wait-poc-<initials>
```

On Windows: `pwsh scripts/deploy_and_e2e.ps1 -Profile <sso-profile> -StackName agent-wait-poc-<initials> -Destroy`.

Both scripts run the local suite first and refuse to deploy if it fails.

---

## 10. A closing note on the design

The thing worth defending in review is the decision that removes components rather than
adds them: **a timeout is an answer, delivered late, to the agent's own entry point.**

Because `SchedulerAnnounce` is an announce adapter rather than a timer service, there is no
timer Lambda, no answer API, and no receiver tier. And because the scheduler's message goes
through exactly the same `dispatch()` path as a human clicking Approve, the case everybody
dreads — the three-day timer firing in the same second somebody clicks — needs no handling
at all. It is one conditional write, and exactly one party wins.

Scenario A exercises that path with a double click, a stale reject, a forged token and a
late timer arriving in sequence against a live thread. The refund counter reads 1.
