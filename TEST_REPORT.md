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
| **Unit** | pytest | 98 | ✅ all pass |
| **Conformance** | pytest, parametrised over `InMemoryWaitStore`, `SqliteWaitStore`, `DynamoWaitStore` (moto) | 96 (32 × 3 stores) | ✅ all pass |
| **LangGraph integration** | real langgraph 1.2.11, `InMemorySaver` + `SqliteSaver` | 55 (incl. 11 spike) | ✅ all pass |
| **AWS local** | moto — DynamoDB, SQS, SNS, EventBridge, Scheduler, Secrets Manager | 77 | ✅ all pass |
| **AWS end-to-end** | real account, `agent-wait-poc-ks` | 4 scenarios / 28 checks | ✅ all pass |
| | | **332 pytest tests, 0 failures** | (4 e2e tests skip unless `AGENT_WAIT_E2E=1`) |

### Coverage

| Package | Coverage | Target |
|---|---|---|
| `agent_wait` | **96%** | ≥ 85% ✅ |
| `langgraph_wait` | **100%** | ≥ 85% ✅ |
| `agent_wait_aws` | 95% | — |
| Total | 96% | |

JUnit XML and the coverage XML are committed under `reports/`.

### The twelve correctness rules (§10)

Every rule has a test named after it, and every one runs against all three stores.

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
| 9 Binding | `test_rule_09_binding_mismatch` | ✅ |
| 10 Announce isolation | `test_rule_10_announce_isolation`, `_sweeper_reannounces_unnotified_waits` | ✅ |
| 11 Parallel interrupts | `test_rule_11_parallel_interrupts_are_independent` | ✅ |
| 12 Cancel | `test_rule_12_cancel`, `_cancel_is_not_repeatable` | ✅ |

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

---

## 4. End-to-end run (§13)

Deployed to `agent-wait-poc-ks` in `ap-south-1`, driven by
`examples/refund_agent/demo_scenarios.py`. Everything below is real: real SQS FIFO, a real
Lambda running a real LangGraph graph against a real DynamoDB checkpointer, a real
EventBridge schedule, and a refund counter read from DynamoDB *outside* the Lambda —
because an in-memory list proves nothing about a process you are not inside.

### Scenario A — crash before `register()`, then every wrong answer at once

Thread `order-a-3c4b91`, refund of 41,000 (over the 25,000 threshold, so a human is asked).

| Time (UTC) | | |
|---|---|---|
| 10:08:02 | start message onto the FIFO queue | |
| 10:08:06 | wait `01M22T97WZ…` created and announced | ✅ one wait, one announcement |
| 10:08:06 | envelope's `reply_to` is the agent's own queue | ✅ |
| 10:08:07 | EventBridge schedule armed, named after the wait | ✅ |
| 10:08:07 | **identical start message redelivered** (what the crash produces) | |
| 10:08:19 | still exactly one wait | ✅ rule 1 |
| 10:08:29 | no second announcement | ✅ `notified_at` suppressed it |
| 10:08:29 | approve, `answer_id=click-9f1` | |
| 10:08:31 | refund counter in DynamoDB reads **1** | ✅ |
| 10:08:31 | **the same click again** | |
| 10:08:43 | counter still **1** | ✅ rule 4 — `duplicate` |
| 10:08:43 | a different decision (`reject`, new `answer_id`) | ✅ `already_answered` |
| 10:08:43 | a tampered token | ✅ `token_invalid` |
| 10:08:43 | a timeout arriving after the answer | ✅ `already_answered`, rule 6 |
| 10:08:58 | counter **still 1**; wait `resumed`; recorded action `approve` | ✅ |
| 10:08:58 | schedule deleted on answer; dead-letter queue empty | ✅ |

**13/13.** Four wrong answers against a live thread, in sequence, and the refund counter
never moved off 1.

### Scenario B — timeout, with nobody answering

Thread `order-b-a9465a`, `AGENT_WAIT_TIMEOUT=PT2M` (two minutes standing in for three days).

| Time (UTC) | | |
|---|---|---|
| 10:08:58 | start message; wait parked | |
| 10:09:01 | schedule holding the timeout, `expires_at 10:12:07Z` | ✅ |
| 10:12:08 | **EventBridge Scheduler fired on its own** — the timeout arrived at the agent's queue as an ordinary answer | ✅ |
| 10:12:08 | recorded action `timeout`; declared default (`reject`) applied | ✅ |
| 10:12:08 | no refund issued | ✅ |
| 10:12:08 | approve, after the fact | |
| 10:12:23 | still no refund; wait `resumed` on the default | ✅ |

**7/7.** One second of real latency between the scheduled instant and the wait settling.

### Scenario C — crash after the resume, before the ack

Thread `order-c-3de9b0`.

| Time (UTC) | | |
|---|---|---|
| 10:12:24 | start; wait parked | |
| 10:12:28 | approve → refund issued, counter **1** | ✅ |
| 10:12:28 | **the same approval redelivered three times** (the un-acked message) | |
| 10:12:48 | counter still **1** | ✅ rule 7 |
| 10:12:48 | dead-letter queue empty — every redelivery was acknowledged, not retried | ✅ |

**4/4.**

### Scenario D — the sweeper

Thread `order-d-9e8566`. `notified_at` was stripped from a live wait and its schedule
deleted: the state a crash between the DynamoDB write and the announce leaves behind. A
wait nobody knows about, that no timer will fire for.

| Time (UTC) | | |
|---|---|---|
| 10:12:51 | wait made invisible: no `notified_at`, no schedule | ✅ |
| 10:13:09 | **the one-minute sweeper found and re-announced it** (18s) | ✅ |
| 10:13:09 | the world was told after all | ✅ |
| 10:13:09 | and the timeout was re-armed | ✅ |
| 10:13:11 | the repaired wait is answerable — approved | ✅ |
| 10:13:11 | and it refunded exactly once | ✅ |

**6/6.**

### Total

**28 / 28 checks passed**, 2026-09-09 10:08:02Z → 10:13:11Z. Machine-readable evidence:
`reports/e2e-20260909T101311Z.json`.

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

`reports/cloudwatch-run-lambda.log` holds the run Lambda's own account of it — the
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

`cdk destroy` was run after the evidence was captured, and the result verified three ways:

```
describe-stacks agent-wait-poc-ks   -> ValidationError: stack does not exist
get-resources --tag project=agent-wait -> 0
list-schedule-groups | agent-wait   -> (none)
```

Nothing billable remains. Every resource carried `RemovalPolicy.DESTROY` and the tag
`project=agent-wait`.

---

## 5. Deviations & decisions

Recorded per CLAUDE.md: where REQUIREMENTS was silent I decided and logged it; §16 was not
re-opened.

### Interpretations of the contract

1. **§4.1's numbered algorithm omits the binding check** that §10.9 requires. Inserted
   after the store read and before the allowed-actions check, so a token minted for a
   different question is refused with `binding_mismatch` before anything is written.
2. **§4.1.9's `resume_value = {"action": action, **payload}`** is ambiguous — `payload` is
   both the name of `dispatch()`'s parameter and a field of the answer envelope. Read as
   the *field*, per `ask()`'s docstring ("the answer envelope's `payload`, plus `action`").
   A non-mapping payload is wrapped as `{"action": …, "payload": …}` rather than failing.
   **Listed as an open question below.**
3. **`on_timeout="fail"` has no matching `Ignore.reason`** in §4's frozen enum. Returns
   `Ignore("expired", detail="on_timeout='fail': the wait was marked failed, not resumed")`
   as the closest available. **Open question below.**
4. **§4.2.5 says `register()` releases the thread lease.** It does — *except* when it
   returns an `immediate_resume`, where the lease is held because the run has not finished
   and the handler is about to invoke the graph again on that thread. The following
   `register()` releases it. Strictly safer than a literal reading.

### Implementation choices

5. **`Clock` / `SystemClock` / `FakeClock` live in `model.py`** rather than a `clock.py`,
   to keep §15's file list exact. `FakeClock` is shipped rather than kept in a conftest
   because the conformance suite runs against stores in another package.
6. **`WaitRuntime._envelope` is public as `envelope_for()`.** The sweeper lives in its own
   module and needs it. This is one method beyond §4's surface; it is not part of the
   documented contract and consumers never need it.
7. **`announce_refs` is unused in v0.1.** Schedules are named after the `wait_id`, so
   creating and deleting one needs no stored handle. The field is kept for adapters that
   do need one.
8. **The token MAC follows §11 literally**: `base64url(HMAC-SHA256(...))[:27]` — the full
   digest base64-encoded, then truncated. The reference prototype truncated the *digest*
   first; that shape was not copied.
9. **`SchedulerAnnounce` delivers immediately when `expires_at` is already past** instead
   of creating a schedule EventBridge would reject. This is the sweeper's repair path for
   a wait whose schedule was lost while the process was down, and it preserves the
   invariant that timeouts always arrive as ordinary answers at the ordinary entry point.
10. **`SecretsManagerKeyProvider` also accepts a flat `{"current": "k1", "k1": "…"}`.**
    CloudFormation's `generate_string_key` writes one value into a template and cannot
    nest, so this lets the stack generate a real random key at deploy time — no key in
    source, no manual step after deploy.
11. **Python 3.12 pinned** with `.python-version`; uv selected 3.14 otherwise.
12. **`DynamoWaitStore.find()` follows `LastEvaluatedKey`.** Found during the deployment:
    a single query is a *page*, not an answer, and a wait the sweeper never sees is a
    thread parked forever. Fixed with a test that forces multiple pages.
13. **The example handler raises its own loggers to INFO.** Also found during deployment:
    the Lambda runtime leaves the root logger at WARNING, so `LogAnnounce` and the run
    handler were silent in production — exactly when you want to know why a message was
    ignored.

### Things added to the example, not the library

14. **`examples/refund_agent/dynamo_checkpointer.py`** — a ~150-line LangGraph
    `BaseCheckpointSaver` over DynamoDB. agent-wait never touches a checkpoint and is
    deliberately checkpointer-agnostic, but the example *must* have a durable saver or
    "resume on a machine that was not running" is untestable and all four scenarios would
    prove nothing. The published options were a stale `langgraph-checkpoint-dynamodb`
    0.1.0 and `langgraph-checkpoint-aws`, which requires Bedrock Session Management. It has
    its own 9 tests, including a graph parked by one saver instance and resumed through a
    different one.
15. **An `Announcements` SQS queue subscribed to the SNS topic**, and `issue_refund`
    mirroring its side effect into DynamoDB. Both exist so the end-to-end run can observe
    from outside the Lambda — the queue is an ordinary consumer, exactly what an approvals
    UI would be, and it is how the run gets a real token out of a real envelope instead of
    minting one for itself.
16. **`handler.py` is ~50 lines, not the "5 lines" §15 suggests**, because the wiring
    (store, tokens, four announce adapters, entry point) is written out rather than hidden
    behind a helper. The agent-wait-specific part really is two lines:
    `runtime = WaitRuntime(...)` and `handler = make_run_handler(graph, runtime)`.
17. **Files beyond §15's tree**: `scripts/` (bundle build, deploy), a root `conftest.py`
    (puts the shared conformance suite and the example on `sys.path`), and test helper
    modules (`rig.py`, `conformance.py`, `driver.py`, `parallel_graphs.py`).

### Build

18. **No Docker on the host**, so the Lambda bundle is built with
    `uv pip install --python-platform x86_64-manylinux2014`, which resolves Linux wheels
    from Windows. The whole dependency tree is pure Python. boto3/botocore are excluded —
    the runtime ships a recent one, and including them adds ~15 MB to a 50 MB limit.
    Result: 46.8 MiB unpacked.
19. **OneDrive.** The repository lives in a synced folder, which rejects hardlinks
    (`os error 396`) and briefly locks directories it is scanning. The build script passes
    `--link-mode=copy` and retries `rmtree`. Noted because it will bite anyone else on the
    same setup.

### Process

20. **Four stacked pull requests**, one per package plus the proof-of-concept, rather than
    four independent ones — `agent-wait` has no dependencies, but `langgraph-wait` and
    `agent-wait-aws` both import it and their tests run the core's conformance suite, so
    independent branches into `main` could not have been green on their own. They are
    stacked in dependency order and reviewable commit by commit. `main` was never
    force-pushed and never left red.

---

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

1. **§4.1.9 — `resume_value = {"action": action, **payload}`.** Confirmed as the answer
   envelope's `payload` field, not the whole envelope? The current reading matches
   `ask()`'s docstring, and a non-mapping payload is wrapped rather than rejected.
2. **`on_timeout="fail"` has no `Ignore.reason` of its own.** Currently reported as
   `expired` with a detail string. Would you prefer a new reason (`failed`) in v0.2, given
   §4's enum is frozen for v0.1?
3. **Holding the thread lease across an `immediate_resume`** is a deliberate deviation from
   a literal reading of §4.2.5. Confirm it is what you want.
4. **`envelope_for()` is one method beyond the three-function surface.** Acceptable, or
   should the sweeper move into `runtime.py` to keep the surface exactly three?

---

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
