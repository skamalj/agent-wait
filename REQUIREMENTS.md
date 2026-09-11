# agent-wait — Requirements (v0.1 PoC)

Owner: Kamaljeet Singh (skamalj@outlook.com) · Project manager: Claude (Cowork) · Date: 2026-09-09
Status: APPROVED FOR BUILD. This document is the contract for the first Claude Code session. Where it is silent, follow the "Decisions already taken" section, then ask.

---

## 0. One-paragraph summary

Build a small, framework-agnostic Python library that turns an agent framework's *interrupt* into a **durable wait**: a parked question that survives the process exiting, can be answered days later from anywhere, has a timeout with a declared default, rejects duplicate or stale answers, and resumes the agent on whatever machine is alive. The user-facing surface is exactly three functions — `ask()`, `register()`, `dispatch()` — plus two JSON message formats and one pluggable interface, `AnnounceAdapter`. First framework: **LangGraph**. First cloud: **AWS** (DynamoDB, SQS FIFO, SNS, EventBridge, EventBridge Scheduler, Lambda, CDK). Deliverable for this session: the core, the LangGraph adapter, the AWS backends, a CDK stack, an example refund-approval agent, a full test suite (unit, conformance, moto, real-AWS end-to-end), and a **TEST_REPORT.md**.

---

## 1. Why this exists (background the developer should know)

Every agent framework can pause (LangGraph `interrupt()`, Strands interrupts, Pydantic AI `DeferredToolRequests`, ADK long-running tools, OpenAI `RunState`). None of them owns what happens *between* pause and resume when the gap is hours or days and the process that paused is gone. The maintainers have said, in writing, that this is the application's job:

- LangChain forum #2541 — "LangGraph leaves scheduling/timing concerns to the application layer … last write wins."
- LangChain forum #2033 — LangSmith webhooks fire on run *success* only, never on interrupt; "on roadmap" since Nov 2025, unanswered.
- LangChain forum #772 — "How do I ensure only one thread_id is run at a time across many pods?" — unanswered 14 months.
- anthropics/claude-agent-sdk-python #871 — "we don't want to keep the subprocess alive while waiting for the user … decisions can take minutes or hours."
- crewAI #2051 — async HITL declared Enterprise-only; OSS users left with monkeypatches.
- pydantic-ai #3274 — commenters supplied two design constraints we adopt: an approval must be bound to the exact question it answers, and "the message history is not the expensive part to restore, the side effects are."
- langgraph #8039 — under `durability="sync"` post-crash replay-vs-re-execute is host-dependent; we must not rely on checkpoint resume alone for side-effect safety.

We are building the layer they all declined to build, as independent packages (LangChain does not accept integration PRs into `langchain-ai/*`; publishing independently and listing in their docs is the sanctioned route).

---

## 2. Scope of v0.1

### In scope
1. `agent-wait` core package (framework-agnostic, zero cloud dependencies).
2. `langgraph-wait` adapter package.
3. `agent-wait-aws` backend package: DynamoDB store, announce adapters for SQS / SNS / EventBridge / EventBridge Scheduler (timeout), a `make_run_handler` Lambda wrapper, and a CDK stack.
4. Example application: `examples/refund_agent` — a LangGraph refund-approval graph deployed on Lambda behind SQS FIFO.
5. Tests at four levels (see §12) and `TEST_REPORT.md`.
6. Docs: README per package, `docs/message-formats.md`, `docs/integrating-a-consumer.md`.

### Out of scope for v0.1 (do not build; leave clean extension points)
- Other frameworks (Strands, Pydantic AI, ADK, OpenAI SDK) — but `FrameworkAdapter` must be a clean protocol so they can be added.
- Other clouds (Azure, GCP) — `WaitStore` and `AnnounceAdapter` must be cloud-neutral protocols.
- Step Functions hosting mode — designed for (see §9.4) but not implemented or tested in v0.1.
- Human-facing UI, Teams/Slack adapters, format converters.
- Per-node distributed execution, fan-out/join, "worker" tier-2/3 features.
- Cancelling a run from outside.

---

## 3. Concepts (vocabulary — use these names in code and docs)

| Term | Meaning |
|---|---|
| **Wait** | One parked question raised by one interrupt at one checkpoint. Immutable question; mutable status. |
| **Wait envelope** | The outbound JSON describing a wait to the world (§7.1). |
| **Answer envelope** | The inbound JSON the world sends to answer (§7.2). |
| **Token** | A signed, single-wait credential embedded in the wait envelope; authorises answering that wait with given actions before an expiry. Never identifies a person. |
| **Announce** | Telling the world a wait exists / changed. Done by `AnnounceAdapter`s, called only from `register()` and `dispatch()`. |
| **Dispatch** | Interpreting an inbound payload at the agent's own entry point: is this a start, a resume, or something to ignore? |
| **Entry point** | Whatever already triggers the agent (SQS queue, HTTP route, Step Functions task). We do not add a second one. |
| **Thread** | The framework's conversation/run identity (`thread_id` in LangGraph). |
| **Transition** | A status change of a wait: `created`, `answered`, `expired`, `cancelled`, `resumed`. |

There is **no** separate receive service, no answer Lambda, no timer Lambda, no `host` configuration flag. The world answers at the agent's existing entry point; `dispatch()` interprets it. Timeouts are an announce adapter that delivers to the future (§8.4).

---

## 4. The three functions (public API — exact signatures)

```python
# langgraph_wait  (framework adapter package)
def ask(question: Any, policy: WaitPolicy | None = None) -> Any:
    """Call inside a LangGraph node. Wraps langgraph.types.interrupt().
    First pass: raises via interrupt() with value {"question": question, "__wait__": policy.to_dict()}.
    On resume: returns the resume value for this interrupt (the answer envelope's `payload`,
    plus `action`)."""


# agent_wait  (core)
class WaitRuntime:
    def __init__(
        self,
        *,
        adapter: FrameworkAdapter,
        store: WaitStore,
        tokens: TokenCodec,
        announce: Sequence[AnnounceAdapter],
        entry_point: EntryPoint,
        clock: Clock = SystemClock(),
    ): ...

    def dispatch(self, payload: Mapping[str, Any]) -> Start | Resume | Ignore:
        """Call BEFORE invoking the graph, on every inbound payload at the agent's entry point."""

    def register(self, result: Any, config: Any, thread_id: str) -> list[WaitEnvelope]:
        """Call AFTER graph.invoke() returns. Detects interrupts, persists waits, announces, returns envelopes.
        If no interrupt: marks any answered/expired waits on this thread as resumed, announces `resumed`,
        releases the thread lease, returns []."""
```

`Start`, `Resume`, `Ignore` are frozen dataclasses:

```python
@dataclass(frozen=True)
class Start:
    thread_id: str
    input: Any | None
    config: dict  # input None ⇒ invoke(None) (re-applied message)


@dataclass(frozen=True)
class Resume:
    thread_id: str
    command: Any
    config: dict
    wait: Wait  # command = adapter.build_resume(...)


@dataclass(frozen=True)
class Ignore:
    reason: Literal[
        "duplicate",
        "already_answered",
        "token_invalid",
        "expired",
        "not_pending",
        "action_not_allowed",
        "binding_mismatch",
        "parked",
        "unknown_payload",
        "lease_held",
    ]
    detail: str = ""
```

### 4.1 `dispatch()` algorithm (normative)
1. If payload has **no** `token` → it is a **start**. Take the thread lease (§10.3). Consult the applied-message set for `thread_id`: if `message_id` already applied → `Start(input=None)`; else record it and → `Start(input=payload["input"])`.
2. If payload has a `token`:
   1. `tokens.verify(token)` → claims `{wait_id, exp, actions}`; failure → `Ignore(token_invalid)`; past `exp` → `Ignore(expired)`.
   2. `store.get(wait_id)`; missing → **park** the answer under `wait_id` (§10.5) and → `Ignore(parked)`.
   3. `action not in wait.policy.allowed_actions` (timeout is always allowed) → `Ignore(action_not_allowed)`.
   4. `wait.status != pending`: same `answer_id` → `Ignore(duplicate)`; else → `Ignore(already_answered)`.
   5. Take the thread lease; if held by another and not expired → `Ignore(lease_held)` (caller should retry / nack).
   6. Conditional write `pending → answered` (or `pending → expired` when `action == "timeout"`), storing `answer, action, actor, answered_at, answer_id`. If the CAS fails → re-read and apply step 4.
   7. `announce(envelope, "answered" | "expired")` on all adapters (errors logged, never raised).
   8. If `adapter.still_pending(wait)` is false (thread already moved past) → mark `resumed`, release lease, → `Ignore(not_pending)`.
   9. Return `Resume(command=adapter.build_resume(wait, resume_value))` where `resume_value = {"action": action, **payload}` for human answers and `policy.default` for `timeout`.

### 4.2 `register()` algorithm (normative)
1. `interrupts = adapter.extract(result, config)`; if empty → finalize: for each wait on the thread in `answered|expired` with `still_pending == False` → CAS to `resumed`, `announce(..., "resumed")`; release lease; return `[]`.
2. For each interrupt: compute `idempotency_key = sha256(thread_id|question_id|checkpoint_id)`; `store.create()` (idempotent — returns existing on conflict). Compute `expires_at` from policy. Mint token. Build envelope.
3. If the wait was newly created **or** `notified_at` is null → `announce(envelope, "created")` on all adapters; on success set `notified_at`.
4. Check the parked-answers table for `wait_id` / correlation; if one exists, apply it immediately via the same path as `dispatch()` step 2.6–2.9 and return the resulting `Resume` in `register()`'s **second** return position (see below).
5. Release the thread lease. Return envelopes.

To keep the handler simple, `register()` returns `RegisterResult(envelopes: list[WaitEnvelope], immediate_resume: Resume | None)`. `make_run_handler` loops if `immediate_resume` is set.

---

## 5. The `WaitPolicy`

```python
@dataclass(frozen=True)
class WaitPolicy:
    timeout: str | int | None = None  # ISO-8601 duration ("P3D", "PT2H") or seconds
    on_timeout: Literal["resume_default", "fail"] = "resume_default"
    default: Any = None  # the resume value when on_timeout == resume_default
    allowed_actions: tuple[str, ...] = ("resume",)
    tags: Mapping[str, str] = field(default_factory=dict)  # routing hints for announce adapters
    correlation: Mapping[str, str] | None = None  # {"provider": ..., "id": ...} for external-job waits
```

Plain `interrupt(value)` without `ask()` must still work: treated as a wait with default policy and `question = value`.

---

## 6. The Wait record (store schema)

| Field | Type | Notes |
|---|---|---|
| `wait_id` | ULID string | PK |
| `thread_id` | str | GSI |
| `framework` | str | `"langgraph"` |
| `question_id` | str | LangGraph `Interrupt.id` |
| `checkpoint_id` | str | from `graph.get_state(config).config` |
| `idempotency_key` | str | unique; `sha256(thread_id|question_id|checkpoint_id)` |
| `question` | JSON | ≤ 200 KB inline; larger → error in v0.1 (blob-by-reference is a v0.2 item) |
| `binding` | str | `sha256(canonical_json(question)|question_id|checkpoint_id)`; first 16 hex chars go into the token |
| `policy` | JSON | serialized `WaitPolicy` |
| `status` | enum | `pending, answered, expired, cancelled, resumed, failed` |
| `notified_at` | ts? | set after first successful `created` announce |
| `announce_refs` | JSON | adapter-specific handles (e.g. Scheduler name, SNS message id) |
| `answer, action, actor, answered_at, answer_id` | — | set on answer/expiry |
| `resume_attempts, last_error` | int, str? | |
| `created_at, updated_at, expires_at` | ts | |
| `version` | int | optimistic concurrency where the store lacks conditional expressions |
| `ttl` | ts? | on terminal rows |

Additional small tables/prefixes in the same store: **applied messages** (`thread_id` → set of `message_id`, TTL 7d), **thread leases** (`thread_id` → `{owner, expires_at}`), **parked answers** (`wait_id` or `correlation_key` → answer envelope, TTL 7d).

DynamoDB: single table, PK `pk`, SK `sk`; items `WAIT#<id>`, `THREAD#<id>` / `APPLIED#<msg>`, `LEASE#<thread>`, `PARKED#<key>`; GSI1 on `thread_id`, GSI2 on `status#expires_at`. All status changes are `UpdateItem` with `ConditionExpression`.

---

## 7. Message formats (the public contract — put in `docs/message-formats.md`)

### 7.1 Wait envelope (outbound, emitted by announce adapters)
```json
{
  "type": "wait.created",                 // or wait.answered | wait.expired | wait.resumed | wait.cancelled
  "event_id": "01J…",                     // ULID; consumers dedupe on it
  "wait_id": "01J…",
  "thread_id": "order-4471",
  "question": { "...": "opaque to adapters" },
  "allowed_actions": ["approve", "reject"],
  "expires_at": "2026-09-12T09:00:00Z",   // null if no timeout
  "token": "aw1.k1.<wait_id>.<exp>.<binding16>.<actions>.<mac>",
  "reply_to": { "kind": "sqs", "url": "https://sqs…/agent-runs.fifo" },   // the agent's own entry point
  "correlation": { "provider": "…", "id": "…" },      // optional
  "tags": { "approver_group": "finance" },
  "transition_detail": { }                // for answered/expired: action, actor, answered_at
}
```

### 7.2 Answer envelope (inbound, sent by the world to `reply_to`)
```json
{
  "token": "aw1.k1.…",
  "action": "approve",                    // must be in allowed_actions; "timeout" reserved for the scheduler
  "payload": { "note": "within budget" }, // becomes part of the resume value
  "actor": "priya@corp",                  // informational; trust comes from the entry point's own auth
  "answer_id": "click-9f1"                // sender's idempotency id; REQUIRED
}
```

### 7.3 Start message (inbound)
```json
{ "thread_id": "order-4471", "input": { "...": "graph input" }, "message_id": "evt-return-4471" }
```
`message_id` optional; SQS `messageId` is used when absent.

The agent's entry point receives 7.2 and 7.3 (and 7.2 with `action: "timeout"` from the scheduler). `dispatch()` tells them apart by the presence of `token`.

---

## 8. `AnnounceAdapter` — the only pluggable interface

```python
class AnnounceAdapter(Protocol):
    name: str

    def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
        """Fire-and-forget. MUST NOT raise into the caller; log and return. Called for every transition."""

    def supports(self, transition: Transition) -> bool: ...
```

`CompositeAnnounce([...])` fans out and isolates failures. Adapters to implement in v0.1:

| Adapter | Package | Behaviour |
|---|---|---|
| `LogAnnounce` | core | structured log line per transition (default in tests) |
| `InMemoryAnnounce` | core | collects envelopes; for tests |
| `SqsAnnounce(queue_url, group_id=lambda env: env.thread_id)` | aws | send envelope to a queue; `MessageDeduplicationId = event_id` |
| `SnsAnnounce(topic_arn)` | aws | publish envelope; `MessageAttributes` from tags for filter policies |
| `EventBridgeAnnounce(bus_name, source="agent-wait")` | aws | `PutEvents`, `DetailType = wait.<transition>` |
| `SchedulerAnnounce(group_name, target=entry_point, role_arn)` | aws | **the timeout**: on `created` with `expires_at` → create one-time schedule (name = `wait_id`, `ActionAfterCompletion=DELETE`, `FlexibleTimeWindow=OFF`) whose target delivers `{"token": env.token, "action": "timeout", "answer_id": "timeout:"+wait_id}` to the agent's entry point (SQS `SendMessage` universal target, or Lambda invoke). On `answered|cancelled|resumed` → `DeleteSchedule` (ignore NotFound). |

`EntryPoint` is a small value object describing where the agent listens (`kind: sqs|lambda|http`, address). It is used to fill `reply_to` and as the Scheduler target.

---

## 9. Framework adapter and hosting

### 9.1 `FrameworkAdapter` protocol
```python
class FrameworkAdapter(Protocol):
    name: str
    def extract(self, result: Any, config: Any) -> list[Question]     # (question_id, checkpoint_id, question, policy)
    def build_resume(self, wait: Wait, resume_value: Any) -> Any             # LangGraph: Command(resume={question_id: resume_value})
    def still_pending(self, wait: Wait) -> bool                              # LangGraph: question_id in get_state().tasks[*].interrupts
    def config_for(self, thread_id: str) -> dict
```

### 9.2 LangGraph adapter (`langgraph_wait.LangGraphAdapter(graph)`)
- Requires LangGraph ≥ 1.2. Verify empirically (a spike test is part of the suite) that `Interrupt.id` is stable across resume-from-checkpoint and that `get_state().tasks[*].interrupts` reliably reflects pending interrupts, including for parallel interrupts and interrupts inside subgraphs (known open bugs #4796/#6792 — document what you find).
- Resume uses dict-keyed `Command(resume={question_id: value})` so parallel interrupts resume independently.

### 9.3 Hosting for v0.1: Lambda + SQS FIFO (`agent_wait_aws.make_run_handler`)
```python
handler = make_run_handler(graph, runtime)  # returns an AWS Lambda handler (event, context)
```
Responsibilities of the wrapper: iterate SQS records; call `runtime.dispatch(body)`; on `Start` → `graph.invoke(input, config)`, on `Resume` → `graph.invoke(command, config)`, on `Ignore` → ack (or report batch item failure only for `lease_held`); extend the message's visibility timeout on a background thread while the graph runs; call `runtime.register(...)`; loop on `immediate_resume`; return `batchItemFailures` for records that must be retried. Must be safe with batch size 1 and `MessageGroupId = thread_id`.

### 9.4 Designed-for, not built: Step Functions
`register()` already returns envelopes; a Step Functions Park state (`sqs:sendMessage.waitForTaskToken`, `TimeoutSecondsPath`) would announce and time out natively and feed `SendTaskSuccess` output back to the run Lambda where `dispatch()` handles it unchanged. Keep nothing in the core that would prevent this. Do not implement in v0.1.

---

## 10. Correctness rules (each is a conformance test)

1. **Idempotent create.** Re-running the same checkpoint (crash after checkpoint, before `register()`; or SQS redelivery of a start) must find the existing wait, not create a second one, and must not announce twice if `notified_at` is set.
2. **One conditional write per transition.** Human vs timer racing → exactly one wins; the loser is a harmless no-op.
3. **Thread lease.** `dispatch()` always takes a lease on `thread_id` (conditional put with expiry, default 15 min, refreshed by the run handler every 60 s); `register()` releases it. Redundant under SQS FIFO; required under HTTP/direct invoke.
4. **Duplicate answers.** Same `answer_id` after answered → `Ignore(duplicate)` (treated as success by the caller). Different `answer_id` → `Ignore(already_answered)`.
5. **Parked answers.** An answer whose wait does not exist yet is stored, not dropped; `register()` applies it when the wait is created.
6. **Late timer.** A timeout arriving after an answer is a no-op (CAS fails).
7. **Resume idempotency.** A redelivered resume for a thread already past the interrupt is `Ignore(not_pending)`; the graph is not invoked; no side effect repeats.
8. **Token integrity.** Tampered / expired / wrong-key tokens are rejected without a store read.
9. **Binding.** The token carries `binding16`; `dispatch()` rejects an answer whose binding does not match the record (`binding_mismatch`). Performed at step 2.2a of §4.1.2 — see §18.6.
10. **Announce isolation.** An adapter raising must not affect the run or other adapters; the sweeper re-announces unnotified waits.
11. **Parallel interrupts.** Two interrupts in one superstep → two waits; answering one resumes only that node; the other's wait, token and schedule are untouched.
12. **Cancel.** `runtime.cancel(wait_id, reason)` → `pending → cancelled`, schedule deleted, announced; later answers → `already_answered`.

---

## 11. Token specification

`aw1.<kid>.<wait_id>.<exp>.<binding16>.<actions>.<mac>` — `exp` epoch seconds; `actions` joined with `+`; `mac` = base64url(HMAC-SHA256(key[kid], everything before the mac))[:27]. Verification must accept the current and previous `kid`. Default token TTL: 7 days, independent of the wait's timeout (the store is the truth). Keys from a `KeyProvider` (env var for tests; Secrets Manager in `agent-wait-aws`). Native platform tokens (Step Functions) are out of scope for v0.1.

---

## 12. Testing requirements (this is what the TEST_REPORT.md must cover)

| Level | Tooling | Must include |
|---|---|---|
| **Unit** | pytest | token codec (mint/verify/tamper/expiry/rotation); policy parsing (ISO durations); envelope (de)serialisation; `dispatch()` decision table (every `Ignore.reason`); `register()` with in-memory store and in-memory announce |
| **Conformance** | pytest, parametrised over stores (`InMemoryWaitStore`, `SqliteWaitStore`, `DynamoWaitStore` via moto) | all 12 rules in §10, driven with a fake clock; crash injection between each step of `register()` and `dispatch()` |
| **LangGraph integration** | real `langgraph` 1.2.x, `InMemorySaver` and `SqliteSaver` | the example refund graph: park → answer → resume → side effect exactly once; timeout path; parallel-interrupt graph; subgraph-interrupt graph (document behaviour) |
| **AWS local** | moto (DynamoDB, SQS, SNS, EventBridge, Scheduler) | `make_run_handler` with a fake SQS event including redelivery and partial batch failure; every AWS announce adapter |
| **AWS end-to-end** | real account via **AWS SSO already logged in on the owner's machine** (see §14) | deploy the CDK stack to a sandbox stack name `agent-wait-poc-<yourinitials>`; run the four scripted scenarios in §13 against real SQS/DynamoDB/Scheduler with a 2-minute timeout instead of 3 days; capture CloudWatch evidence; tear down |

Coverage target ≥ 85% on `agent_wait` and `langgraph_wait`. All tests run with `uv run pytest` from the repo root; the e2e suite is opt-in via `AGENT_WAIT_E2E=1`.

**TEST_REPORT.md** must contain: environment (versions, region, account alias — no account id), the table above with pass/fail counts per level, per-scenario narrative with timestamps for the e2e run, every deviation from this document and why, known gaps, and the exact commands to reproduce. Attach `pytest` junit XML under `reports/`.

---

## 13. The four end-to-end scenarios (also the example app's demo script)

Graph: `load_order → review → (issue_refund | notify_customer) → END`, `review` calls `ask()` when amount > 25,000 with policy `timeout=…, default={"action":"reject"}, allowed_actions=["approve","reject"]`. `issue_refund` appends to a side-effect log that the tests assert on.

A. **Crash-before-register + double-click.** Start; kill the handler after `invoke()` returns; SQS redelivers; wait created once; approve twice with same `answer_id` (second → duplicate); reject with different `answer_id` (→ already_answered); tampered token (→ token_invalid); resume runs on a fresh invocation; refund issued exactly once; late timeout is a no-op.
B. **Timeout.** No answer; Scheduler delivers `timeout`; default reject applied; graph ends `rejected`; late approve → already_answered.
C. **Crash-after-resume-before-ack.** Approve; handler applies resume and refunds, dies before ack; redelivery → `not_pending`; refund count still 1.
D. **Sweeper.** A wait exists with `notified_at` null (simulated crash); sweeper (`runtime.sweep()`, deployed as a 1-minute EventBridge rule → tiny Lambda) announces it and creates the schedule.

---

## 14. Environment and access — read carefully

- **AWS**: the project owner has an active **AWS SSO login on his Windows machine (`sitl2117`)**. The cloud session does not have AWS credentials of its own. To deploy and run the e2e suite: (1) prefer the linked-device tools if this session has them (`device_bash` on the owner's machine); check `aws sts get-caller-identity` there first — if the AWS CLI or the SSO cache is not reachable from the device VM, **do not** ask the owner to paste credentials; instead (2) produce `scripts/deploy_and_e2e.ps1` / `.sh` that the owner runs locally, and write the TEST_REPORT with the local levels passed and the e2e section marked "awaiting owner run" with exact instructions. Report which path you took.
- **Region**: `ap-south-1` unless the owner's SSO profile defaults elsewhere. Stack name prefix `agent-wait-poc`. Tag every resource `project=agent-wait`. Tear down after the e2e run unless the report says otherwise.
- **GitHub**: repository `skamalj/agent-wait` (private). Commit early and often; conventional commits; one PR per package at minimum; do not force-push.
- **Python**: 3.12. Package manager: **uv** with a workspace monorepo. Lint/format: ruff. Types: pyright strict on `agent_wait`.

---

## 15. Repository layout (create exactly this)

```
agent-wait/
  REQUIREMENTS.md                  ← this file
  CLAUDE.md                        ← working agreements for Claude Code (provided)
  README.md
  pyproject.toml                   ← uv workspace root
  packages/
    agent-wait/                    ← dist: agent-wait · import: agent_wait
      src/agent_wait/{__init__,model,policy,token,store/{base,memory,sqlite},announce/{base,log,memory,composite},runtime,sweep,errors}.py
      tests/
    langgraph-wait/                ← dist: langgraph-wait · import: langgraph_wait
      src/langgraph_wait/{__init__,ask,adapter}.py
      tests/
    agent-wait-aws/                ← dist: agent-wait-aws · import: agent_wait_aws
      src/agent_wait_aws/{__init__,store_dynamo,announce/{sqs,sns,eventbridge,scheduler},entry_point,run_handler,keys_secrets}.py
      cdk/                         ← CDK app (Python): WaitStack construct + PoC stack
      tests/
  examples/refund_agent/           ← graph.py, handler.py (5 lines), demo_scenarios.py
  docs/{message-formats,integrating-a-consumer,architecture}.md
  reference/wait-sim/              ← earlier prototype (provided). Store/CAS/token semantics are reusable;
                                     its Waiter.answer()/answer_handler/on_timer_handler are SUPERSEDED by dispatch() —
                                     do not copy that shape.
  reports/                         ← junit xml, e2e logs
  TEST_REPORT.md
```

Package names are final: `agent-wait`, `langgraph-wait`, `agent-wait-aws` (all confirmed free on PyPI as of 2026-09-08). Do **not** publish to PyPI in v0.1.

---

## 16. Decisions already taken (do not re-open; note disagreement in the report)

1. Three-function API; no receiver service; no `host` flag; timeouts are an announce adapter.
2. Policy rides inside the interrupt value under the `__wait__` key.
3. `dispatch()` always takes the thread lease.
4. Token TTL is independent of wait timeout; the store decides finality.
5. `actor` is informational; authentication is the entry point's concern.
6. Questions > 200 KB fail loudly in v0.1.
7. `register()` returns `RegisterResult(envelopes, immediate_resume)`.
8. Single-table DynamoDB design.
9. Scheduler target is the agent's SQS queue (universal target `sqs:SendMessage`), not a Lambda.
10. Step Functions mode is designed-for but not built.

---

## 17. Definition of done for this session

- All four test levels implemented; local levels green in CI (GitHub Actions workflow included).
- E2E either executed with evidence, or a runnable owner script plus report section explaining why it could not be executed from the session.
- `TEST_REPORT.md` committed at repo root and its contents also sent as the session's final message.
- `docs/` complete enough that a third-party team could write a consumer from `message-formats.md` and `integrating-a-consumer.md` alone.
- Open questions, deviations and recommended v0.2 items listed at the end of `TEST_REPORT.md`.

---

## 18. Amendments (2026-09-09)

Rulings issued by the project manager after the v0.1 build, in response to the open
questions in `TEST_REPORT.md` §7. These amend the sections they name; where they conflict
with the text above, **§18 wins**.

### 18.1 Resume value — merge order corrected

`payload` in §4.1.9 is confirmed to mean the **answer envelope's `payload` field**, not the
whole envelope. But the merge order given there is wrong:

```python
{"action": action, **payload}  # WRONG — a payload key can override the action
{**payload, "action": action}  # CORRECT — the envelope's action always wins
```

Under the original order a sender could smuggle `{"payload": {"action": "approve"}}` past a
wait whose `allowed_actions` is `("reject",)`: the allowed-actions check would inspect the
envelope's `action` (`reject`) and pass, and the graph would then read `approve` out of the
resume value. The envelope's `action` is the authorisation-checked field and must therefore
be the one the graph sees.

> ~~The same rule applies on the timeout path: when `policy.default` is a mapping,
> `{**default, "action": "timeout"}`.~~
>
> **Superseded by §18.1a below.** The timeout path is *not* the same case: a `default` is
> written by the graph author, not received from a third party, so there is nothing to
> defend against and the author's value stands. §18.1 governs the answer path only.

### 18.1a Addendum — a timeout default keeps its own action

The merge-order correction in §18.1 applies to the **answer** path only. On the timeout
path it was wrong, and is hereby reversed.

The argument for `{**payload, "action": action}` is that `payload` arrives from an external
party and is checked against `allowed_actions`, so the authorisation-checked field has to
be the one the graph sees. A `default` is not that. It is declared by the graph author, in
the graph, next to the question. There is no second party to defend against, and overriding
it hands an author something they did not ask for.

Rule for `policy.default` on timeout:

- **a mapping** → the resume value is the default **exactly as declared**;
- **a mapping with no `"action"` key** → add `"action": "timeout"`;
- **not a mapping** → pass through unchanged.

So `default={"action": "reject", "reason": …}` reaches the graph as `action="reject"`.

The audit trail is unaffected, and is where "what happened" is recorded: the wait record
and every announce carry `action="timeout"` and `actor="system:timer"`. *What the graph is
told* and *what happened* are two different questions, and they get two different answers.

The answer path is untouched — an inbound `payload` still cannot override the envelope's
`action`, and `test_a_human_answer_still_cannot_smuggle_an_action` guards that from the
other side.

### 18.2 `on_timeout="fail"` — a reason of its own

The `Ignore.reason` enum in §4 is extended with **`"failed"`**. (The enum was the project
manager's to freeze; this is its correct extension, not a deviation from it.)

When a wait whose policy says `on_timeout="fail"` expires:

1. `pending → expired` by conditional write, **and the `expired` announce fires** — so the
   schedule is deleted and the world is told the wait is over;
2. `expired → failed` by a second conditional write;
3. `dispatch()` returns `Ignore("failed")`;
4. **the graph is never invoked**;
5. the thread lease is released.

Two writes, not one, precisely so that step 1's announce still happens. Conformance test:
`test_rule_13_on_timeout_fail`.

### 18.3 The thread lease across an immediate resume

Holding the lease is correct: releasing it between applying a parked answer and running the
resume would open exactly the window the lease exists to close. §4.2.5 is amended to read:

> Release the thread lease **unless `immediate_resume` is set**; the `register()` call that
> follows the immediate resume releases it.

`make_run_handler` must **guarantee that the follow-up `register()` runs even if the resumed
`invoke()` raises** — `try`/`finally`, so the lease is always released. Otherwise a lease
held by a dead invocation blocks every redelivery of that thread for its full duration, and
a transient error looks like a permanently stuck thread.

### 18.4 `envelope_for()` accepted as support API

The three-function promise is about **what a graph author and a handler must call**. The
sweeper and the announce adapters are ours, and may use more.

`WaitRuntime.envelope_for(wait, transition)`, `WaitRuntime.sweep()` and
`WaitRuntime.cancel(wait_id, reason)` are kept as **documented support methods**, listed in
the API reference under "runtime operations" — *not* in the README quickstart, which stays
`ask()` / `dispatch()` / `register()`.

The constraint that stands unchanged: **no new entry point, and no new pluggable interface.**

### 18.5 A first-run crash must not lose the input

`dispatch()` records a start message as applied in §4.1.1, **before** the graph consumes
it. "Applied" therefore means *we started on this message*, not *the framework kept the
result*. If the first run dies before the framework persists anything, the redelivery is
correctly recognised as a replay and invoked with `input=None` — and the framework has no
state to resume from. On LangGraph that is `EmptyInputError`, and the thread is stuck until
the message reaches the dead-letter queue.

**Do not move the write into `register()`.** That opens the opposite and worse hole: a
crash *after* the checkpoint but *before* `register()` would leave the message unrecorded,
and the redelivery would re-apply the input as a second turn.

The write stays in `dispatch()`. One check is added: when a start message is already marked
applied, `dispatch()` asks the adapter whether the framework has any state for the thread.

- `has_checkpoint(thread_id)` **true** → `Start(input=None)`, exactly as before;
- `has_checkpoint(thread_id)` **false** → the first run died before anything was
  persisted, so nothing was in fact applied → `Start(input=payload["input"])`.

`FrameworkAdapter` gains a fifth method:

```python
def has_checkpoint(self, thread_id: str) -> bool: ...
```

For LangGraph: a `checkpoint_id` on `get_state(config).config`, **or** non-empty
`get_state(config).values`. Both are checked because they fail in different directions — a
thread that interrupted in its first superstep can have the former without the latter.

Conformance test: `test_rule_14_first_run_crash_before_checkpoint`, crash-injected, plus
`test_rule_14_once_checkpointed_the_input_is_not_reapplied` so the fix cannot quietly undo
rule 3.

### 18.6 The binding check is step 2.2a

§4.1.2 never numbered the binding check, though §10.9 has always required it. The paper now
matches the code. §4.1.2 is amended to insert, immediately after `store.get(wait_id)`
(step 2.2) and **before** the allowed-action check (step 2.3):

> **2.2a** Compare the token's `binding16` against the record's `binding`. On mismatch →
> `Ignore(binding_mismatch)`.

The order is load-bearing rather than cosmetic. The binding is what ties a token to the
*exact* question it answers, so it has to be settled before anything is decided on the
strength of that token and before any write. Checking it after the allowed-action check
would mean reasoning about a policy that may belong to a different question; checking it
before the store read is impossible, since there is nothing to compare against yet.

Rule 9 in §10 now cites this step.

---

## 19. v0.2 — the simplification (2026-09-10)

Directed by the owner after v0.1 shipped. This section supersedes §1–§13 wherever they
conflict; §14–§18 describe v0.1 and are kept as the record of what was built and why.

### 19.1 The finding

v0.1 met its contract, and the contract was too big. The owner's judgement, arrived at by
counting what a caller has to know:

> "This is built too complex … we need to simplify even if it means removing feature. Let's
> say we do not handle the incoming messages, let the user handle it how to call
> invoke/resume — we only externalize the interrupt via ask and register."

Two arguments carried it, and both are worth preserving because they are the test any
future addition should be held to.

**The naming problem was a design problem.** `dispatch()` was hard to name because it did
not do one thing: it verified a token, decided a race, wrote to the store, announced, and
*then* handed back an instruction for the caller to execute. `register()` was hard to name
because announcing was only one of its four jobs. Neither name was fixable without fixing
the split behind it.

**The library's value was in one half.** Externalising the interrupt — publishing the
question with everything needed to answer it — is the part that is hard to get right and
that every consumer needs. Receiving the answer is the part every team already has an
opinion about, and the part that dragged in tokens, a store, leases, a scheduler and a
sweeper.

### 19.2 What v0.2 is

One call, plus `ask()`, plus one schema.

```python
agent = WaitPublisher(adapter, announce=[...], reply_to=EntryPoint("sqs", url))
agent.invoke(value, thread_id)
```

`invoke()` runs the graph and publishes what changed, by diffing the framework's own
pending set before and after. `pending()` reads that set; `republish()` re-announces it.
`ask()` is unchanged. `AnnounceAdapter` is unchanged.

Two transitions exist — `wait.created` and `wait.resumed` — because a wait has exactly two
observable states once there is no record of one.

### 19.3 What was removed, and what the caller inherits

Removed: `dispatch()` and its eleven `Ignore` reasons; tokens, key rotation and the binding
hash; the wait store in all three implementations, with leases, idempotency keys and parked
answers; the sweeper; `SchedulerAnnounce` and the whole timeout path; `make_run_handler`.

`timeout`, `default` and `allowed_actions` remain on `WaitPolicy` and are published —
`timeout` resolved to an absolute `expires_at`. **Nothing in the library acts on them.** The
world is told the rules; the world enforces them.

The caller inherits four things, listed in full in `docs/migrating-from-0.1.md`: enforcing
the timeout, authenticating the answer, deciding a race between two answers, and not
re-invoking a thread that is already parked.

The third is the real cost and is stated plainly rather than glossed. v0.1 resolved two
simultaneous answers with a compare-and-set that held regardless of transport. v0.2 offers
`pending()`, which correctly rejects any answer the graph has moved past but does not close
the same-instant window; SQS FIFO keyed by thread closes it, and an HTTP entry point with
concurrent handlers needs a conditional write of its own.

### 19.4 Consequences that were not obvious

Recorded because they were found by building it rather than by designing it.

**A redelivered start message must not re-invoke a parked thread.** LangGraph treats it as
a fresh turn and re-asks the question under a *new* interrupt id — a duplicate no consumer
can detect. v0.1 got this from its store of applied message ids (§18.5). v0.2 requires one
branch in the host's router, and it is why `pending()` is public rather than internal. Found
by a failing test, not by review.

**`expires_at` must be anchored to the checkpoint.** Computed as `now + timeout` at publish
time it walks forward on every republish, so a thread retried often enough would never
expire. `Question.asked_at` carries LangGraph's `created_at` for this.

**Deduplication moves from `event_id` to `question_id`.** Republishing is now the only
recovery mechanism, so the key has to survive a republish; `event_id` is minted per publish
and cannot.

### 19.5 What got simpler in a way that matters

The answer reaches the node verbatim. v0.1 merged the envelope's `action` into the payload,
which needed a rule, which had a smuggling hole in its first draft (§18.1), which then
needed an amendment to the amendment for timeout defaults (§18.1a). v0.2 has no merge and
therefore no rule.

### 19.6 Announce adapters are now the extension point

Because nothing reads state back through the library, an announcer no longer has to be a
message broker: it is anywhere the question can be put where whoever answers it will find
it. `DynamoDbAnnounce` was added to demonstrate this — thirty lines, and an approvals UI is
a `Query` against it with no broker in the system at all.

### 19.7 v0.1 stands

`v0.1.0` is tagged and its `TEST_REPORT.md` is unchanged. Where the answer-side guarantee is
load-bearing and the entry point cannot serialise per thread, staying on it is a defensible
choice rather than a fallback, and the migration guide says so.

---

## 20. v0.3 — publish only (2026-09-11)

Directed by the owner after reviewing the v0.2 host code. Supersedes §19 where they
conflict.

### 20.1 The finding

v0.2 still wrapped the graph (`WaitPublisher`) and still read framework state
(`get_state()` behind `pending()` / `republish()`), and still shipped routing helpers
(`is_answer()`, `resume_command()`). Each was defensible alone; together they put the
library back on the receive path and made a host learn an object to do one thing. The
owner's direction: **announce the interrupt, and stop.**

### 20.2 What v0.3 is

- In the graph: `ask(question, policy)`, or `@hitl(policy)` on a tool.
- After the run: `publish_interrupts(result, thread_id, announce)`. Input is
  `result["__interrupt__"]` — `Interrupt.id` and `Interrupt.value`, per the LangGraph
  reference — plus the `thread_id` the host already has. No graph handle, no state read.
- `@hitl(policy, mode="async", announce=[…])`: the decorator is the publisher; the
  tool call announces and returns pending; the thread does not park; nothing is called
  after the run. For single-thread channels where the approver is not on the thread.
- `HumanInTheLoopMiddleware` batches are understood as one question.
- `Question(question_id, question, policy, asked_at, source)` replaces `PendingInterrupt`.
  Envelope field `question_id`. Single transition `wait.created`. Policy gains
  `answer_ttl`; envelope gains `source`.

### 20.3 The receive side is documentation

The recommended answer message — `{thread_id, question_id, answer, valid_until?}` — and
the host's one `if` are documented in `docs/message-formats.md`. Nothing is implemented:
no validation, no dedupe, no expiry, no ledger reads. The `get/close/overdue` briefly
added to `DynamoDbAnnounce` were removed for exactly this reason; the adapter writes the
row and never reads it.

Two facts, verified on langgraph 1.2.11 and asserted in tests, make that safe: a
duplicate `Command(resume=…)` on a thread that has moved past the interrupt runs nothing,
and a different answer after the first changes nothing. There is therefore no check for
the host to write.

### 20.4 Why `__interrupt__` and not `get_state()`

`get_state().tasks[*].interrupts` over-reports after a partial parallel resume (#4796 /
#6792). `result["__interrupt__"]` lists only what the run raised. Reading the result
removed the filter, the caveat, and the need for a graph handle.
