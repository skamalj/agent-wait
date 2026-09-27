# TEST_REPORT — agent-wait v0.8.0

Supersedes the v0.7.0 report. Earlier reports are at their tags.

## 0. Read this first

**One package, two things in it, three frameworks.** `@wait` makes a function wait for
an answer from outside the process; `publish_interrupts(result, thread_id, announce)`
after the run announces what the framework parked on. Both are written once against the
three-method `Framework` interface. `agent_wait.langgraph` (0.5), `agent_wait.pydantic_ai`
(0.6) and `agent_wait.strands` (0.7) are the implementors; the AWS announcers are the
providers.

**What changed in 0.8.** `@wait(when=...)`: a predicate that decides, per call, whether
this call needs a question at all. Unlike 0.6 and 0.7 this **is** a core change —
`wait.py` grew a parameter and a helper — and it is the first release since 0.5.0 that
touches the decorator itself. The `Framework` interface is unchanged, all three
implementors are untouched, and an unconditional `@wait` behaves exactly as before.

Why it exists: without it, a threshold approval needs the rule to live outside the
function — two tools with a model choosing between them, or a router in front. A model
that picks the wrong tool is a routing mistake; a model that decides whether an approval
applies is an incident. `when=` puts the threshold in the function it guards.

**Verified:** every local level, including seven new tests for `when=` (six against the
stub framework, one end to end on a real graph); **the AWS e2e, re-run at 0.8.0 on
2026-09-27** — 15 / 15, §6; and the published wheel, installed from PyPI on a clean
runner — the `smoke-from-pypi` job of the `v0.8.0` release run passed, including the
smoke script's new `when=` section. **Not verified:** `when=` at the e2e level — the example graph uses an
unconditional `@wait`, so the deployed run exercised the unconditional path. §7.

---

## 1. Environment

REQUIREMENTS §12 asks for this and no earlier report carried it; added here.

| | |
|---|---|
| Package under test | `agent-wait` 0.8.0 (this repo, editable) |
| Python | 3.12.13 |
| uv | 0.11.19 |
| OS | Windows 11 (`win32`); CI additionally runs Ubuntu |
| LangGraph | 1.2.11, `langgraph-checkpoint` 4.2.0 |
| Pydantic AI | `pydantic-ai-slim` 2.43.0 |
| Strands | `strands-agents` 1.55.1 |
| AWS | `boto3` 1.43.93; `moto` 5.2.3 for the local level |
| Test tooling | `pytest` 9.1.1, `ruff` 0.16.7, `pyright` 1.1.414 |
| AWS region | `ap-south-1` |
| AWS account | the owner's sandbox, reached by an existing SSO session; alias withheld along with the id, per CLAUDE.md |
| Stack | `agent-wait-poc`, tag `project=agent-wait`, destroyed after the run |

## 2. Summary

| | v0.7.0 | v0.8.0 |
|---|---|---|
| Extras | `[langgraph]`, `[pydantic-ai]`, `[strands]`, `[aws]` | unchanged |
| Framework implementors | LangGraph, Pydantic AI, Strands | unchanged |
| Public API | `@wait`, `publish_interrupts` | + `when=` on `@wait` |
| Core `wait.py` | unchanged since 0.5.0 | **changed** (one parameter, one helper) |
| `Framework` interface / implementors | unchanged | unchanged |
| Tests passed | 161 | 168 (+6 core, +1 LangGraph) |
| Tests collected | 165 | 172 (71 core, 47 LangGraph, 13 Pydantic AI, 12 Strands, 29 AWS of which 4 e2e skipped) |
| Coverage | 99% (601 statements, 9 missed) | 99% (611 statements, 9 missed); `wait.py` 100% |
| Smoke test | + Strands section | + `when=` section |
| AWS e2e | not re-run | **re-run 2026-09-27, 15 / 15** (unconditional path) |

## 3. What was built

**`when=` on `@wait`, new in 0.8.0.** One keyword, a predicate over the call's published
arguments:

```python
@tool
@wait(FINANCE, when=lambda order_id, amount: amount > 25_000)
def issue_refund(order_id: str, amount: int) -> str:
    payments.refund(order_id, amount)  # under the limit this just runs
    return "refunded"
```

Three decisions in it, each of which a test pins:

- **The predicate sees what the approver sees** — the published `args`, with
  `hidden_params` and the decision parameter removed. What you branch on is what the
  question shows, so a framework's injected `ctx` or `tool_context` can never reach it.
- **A predicate that raises asks anyway.** A refund is not skipped because a lambda had a
  typo; the exception is logged on `agent_wait.wait` at WARNING and the call parks. Fail
  closed, because the failure being guarded is a payment.
- **It must be deterministic on the same arguments**, documented rather than enforced. The
  framework re-runs the node from the top on resume, so `when` is called again; one that
  answered differently the second time would let the body run without the answer. This is
  a constraint we can state and cannot check, so it is stated in the docstring, the README
  and the frameworks page.

`wrapper.__agent_wait__` gained `"conditional": when is not None`, so a host can tell the
two shapes apart without inspecting the closure. `when=None` is the default and that path
is byte-for-byte the 0.7.0 behaviour.

**`agent_wait.strands` (`[strands]`), 0.7.0.** `StrandsFramework`: `interrupt` finds the
`ToolContext` among the call's arguments and returns
`tool_context.interrupt("agent_wait", reason=packed)` — Strands raises
`InterruptException` the first time and returns the human's response on the re-run, so
the wrapper needs no re-run detection of its own; `interrupts_in` reads
`result.interrupts` when `stop_reason == "interrupt"` and yields `(Interrupt.id,
Interrupt.reason)`; `current_thread_id` reads `tool_context.agent.session_id`;
`hidden_params = ("tool_context",)`. A `@wait` tool without a `ToolContext` raises
`TypeError` at the first call (Strands turns it into an error tool result). Then
`wait = make_wait(...)`, `publish_interrupts = make_publish_interrupts(...)`.


**`agent_wait.pydantic_ai` (`[pydantic-ai]`), 0.6.0.** `PydanticAIFramework`:
`interrupt` raises `ApprovalRequired(metadata=packed)` on the first call and, on the
re-run (`ctx.tool_call_approved`), returns `ctx.tool_call_metadata` — the answer the host
attached — or `{"action": "approve"}`; `interrupts_in` reads `result.output` when it is a
`DeferredToolRequests` and yields `(tool_call_id, metadata[id])` for every approval and
deferred call, falling back to `{"function", "args", "metadata"}` for tools that never
heard of the library; `current_thread_id` reads `deps.thread_id` (attribute or key);
`hidden_params = ("ctx",)`. A `@wait` tool without a `RunContext` is refused with a
`TypeError` up front, because without it the re-run is indistinguishable from the first
call. Then `wait = make_wait(...)`, `publish_interrupts = make_publish_interrupts(...)`.

**Everything below is the 0.5.0 build, unchanged.**

**`agent_wait` (core, no deps).** `WaitPolicy`, `Question`, `WaitEnvelope`,
`build_envelope()`, `publish()`, `BaseAnnounce` + `Webhook`/`Log`/`InMemory`/`Composite`
announcers — unchanged. New: `framework.Framework` (abstract: `interrupt`,
`interrupts_in`, `current_thread_id`; `hidden_params` for framework-injected parameters)
and `wait.py` (`pack`/`unpack`, `question_id_for`, `make_wait`, `make_publish_interrupts`).

**`agent_wait.langgraph` (`[langgraph]`).** `LangGraphFramework`: `interrupt` →
`langgraph.types.interrupt`; `interrupts_in` → `result["__interrupt__"]` as
`(Interrupt.id, Interrupt.value)`; `current_thread_id` → `get_config()`. Then
`wait = make_wait(...)`, `publish_interrupts = make_publish_interrupts(...)`,
`questions_in`. Import without LangGraph raises `ImportError: … pip install
'agent-wait[langgraph]'`.

**`agent_wait.aws` (`[aws]`).** `SnsAnnounce`, `SqsAnnounce`, `EventBridgeAnnounce`,
`DynamoDbAnnounce` — unchanged behaviour, relative imports, strict-typed. Same guard for
boto3.

**Elsewhere.** CDK stack moved to `examples/refund_agent/cdk`; the bundle script copies
`src/agent_wait` and the example; CI/release workflows build one wheel and smoke-install
`agent-wait[langgraph,aws]==VERSION` from PyPI; metadata summary and keywords lead with
human-in-the-loop for discoverability while the name stays `agent-wait`.

## 4. What was removed since 0.4.1, and why

- **Two distributions.** Three coupled packages were a pin-drift hazard (bitten once at
  0.3.0) and a naming problem per framework × provider. `langgraph-wait` and
  `agent-wait-aws` stay on PyPI at 0.4.1 and are not updated.
- **The name `hitl`.** A person is the common answerer, not the only one; "wait" is the
  base concept and the package was already called that. Discoverability moved to metadata.
- **Direct LangGraph calls inside the decorator.** Replaced by the `Framework` interface
  so a second framework is a subpackage and an extra, not a fork of the decorator.

## 5. How it was tested

**`when=` (7 tests, new).** Six in `tests/core/test_framework.py` against the stub
framework, so they test the decorator and not a framework: a false predicate runs the
body and publishes nothing; a true one parks exactly as an unconditional `@wait` does;
**the predicate never sees the framework's injected params**; a predicate that raises
parks anyway; `when=` applies in async mode too; and the `__agent_wait__` tag records
whether the wait is conditional. One in `tests/langgraph/test_wait_langgraph.py` drives a
real graph end to end — `test_when_decides_whether_the_graph_parks_at_all` — because the
stub cannot show that the graph itself does not stop.

The determinism requirement has no test, deliberately: it is a property of the caller's
predicate across two invocations, and a test could only pin a predicate I wrote to be
deterministic. It is documented in three places instead. Recorded as a gap in §7.

**Strands (12 tests, 0.7.0).** Real `Agent`s with a scripted `Model` (the SDK's own
`MockedModelProvider`, vendored under `tests/strands_agents/mocked_model.py`), through
`agent(...)` and the framework's own tool, interrupt and session machinery: a `@wait`
tool stops the run with `stop_reason == "interrupt"` and publishes `{function, args}`
with `tool_context` hidden, `question_id` is the per-tool-call interrupt id, policy
fields on the envelope; a finished run publishes nothing; approve runs the body once
with original args; approve with `args` runs it with them; anything else does not run it
and becomes the tool result the model sees; a `decision` parameter receives the answer
verbatim, approve or not; **a parked question survives a new process** — a fresh `Agent`
on the same `FileSessionManager` session resumes and the body runs once; a parked run
refuses a plain prompt (`TypeError`) and an unknown id (`KeyError`); **a duplicate answer
is refused** (`ValueError`, pinned so the doc line stays true); a bare
`tool_context.interrupt(name, reason)` is published with the default policy; async mode
publishes from inside the tool with the derived id and the run carries on; a `@wait`
tool without `@tool(context=True)` is refused at the first call.


**Pydantic AI (13 tests, 0.6.0).** Real `Agent`s with a scripted `FunctionModel`
(first turn calls the tool, the turn after a tool return echoes it), through
`agent.run_sync()` and the framework's own deferred-tool machinery: a `@wait` tool parks
the run on `DeferredToolRequests` and publishes `{function, args}` with `ctx` hidden,
`question_id == tool_call_id`, policy fields on the envelope; a finished run publishes
nothing; approve runs the body once with original args; approve with `args` in the
metadata runs it with them; anything else does not run it and the model sees why
(`ToolDenied`); a `decision` parameter receives the metadata verbatim; `approvals={id:
True}` without metadata is a plain approve; **the host owns idempotency** — a duplicate
from the post-run history is a `UserError`, from the pre-resume history the tool runs
again (pinned so the doc line stays true); `requires_approval=True` and `CallDeferred`
tools are published with the default policy; async mode publishes from inside the tool
with the derived id, the run carries on, and needs `thread_id` in `deps`; a `@wait` tool
without `RunContext` is refused at the first call.

**Everything below is earlier evidence, re-run and still green.**

**Core (47 tests).** As before for `publish()`, `build_envelope()`, the envelope field
set, `dedupe_key`, the announcers (webhook against a real `http.server`). `test_framework.py` (17, six of them the `when=` tests above): a stub `Framework` that parks by raising — `@wait` packs
`{function, args}` with `hidden_params` and the decision parameter removed; approve runs
with original or edited args; anything else is returned unexecuted; a `decision`
parameter always runs; async publishes through its own announcers with the deterministic
id and parks nothing; `announce=` required; `publish_interrupts` reads only what
`interrupts_in` returns; a bare value gets the default policy; `questions_in` exposed;
and importing `agent_wait.langgraph` with LangGraph hidden raises an error naming the extra.

**LangGraph (39 tests).** `test_wait.py` (ex-`test_hitl.py`), `test_publish_interrupts.py`,
`test_integration_refund.py` (in-memory and SQLite checkpointers, host of one `if`,
duplicate answer → one refund), the spike (12 recorded observations against 1.2.11) —
all unchanged in substance, on the new imports.

**AWS-local (29 tests).** Four announcers over moto; the DynamoDB row shape; the GSI
query; overwrite on republish; the example's checkpointer.

**Wheel in a clean venv.** `uv build`, then in a fresh venv outside the repo: bare
install imports the core with no `langgraph`/`boto3` present and both subpackages raise
the documented `ImportError`; `[langgraph,aws]` then passes `scripts/smoke_from_pypi.py`
end to end (both modes, every announcer, signed webhook, the answer round trip).

**Docs.** Every python fence in README and `docs/` parsed and every `agent_wait`/
`langgraph` import resolved by a script; stale vocabulary (`hitl`, `langgraph_wait`,
`agent_wait_aws`, `packages/`) rejected. `mkdocs build --strict` clean.

### Results

```
168 passed, 4 skipped (the e2e level, opt-in)   172 collected
ruff check      clean ("All checks passed!")
ruff format     clean (62 files already formatted — includes python fences in markdown)
pyright strict  0 errors, 0 warnings (src/ — core, langgraph, pydantic_ai, strands, aws)
coverage        99% (611 statements, 9 missed); wait.py 95 statements, 100%
wheel smoke     ok, from PyPI on a clean runner (v0.8.0 release run, smoke-from-pypi job):
                bare = core only; four guards name their extra;
                [langgraph,pydantic-ai,strands,aws] full smoke incl. the when= section
mkdocs --strict ok; every doc snippet parsed and its imports resolved
```

Reproduce, from the repo root:

```bash
uv run pytest                  # 168 passed, 4 skipped
uv run ruff check . && uv run ruff format --check .
uv run pyright
uvx --with mkdocs-material --with pymdown-extensions mkdocs build --strict
AGENT_WAIT_E2E=1 scripts/deploy_and_e2e.sh --destroy   # needs the owner's SSO session
```

`reports/junit.xml` and `reports/coverage.xml` are this run (172 tests, 0 failures,
0 errors, 4 skipped). Both were two releases stale when 0.8.0 was cut — see §8.

## 6. The deployed AWS run (re-run at 0.8.0)

Run on **2026-09-27** from the owner's SSO session, `ap-south-1`, stack `agent-wait-poc`
(tag `project=agent-wait`), same script and same stack shape as below. 0.8.0 changes the
core, so this level was re-run rather than inherited.

```
15 / 15 checks passed              reports/e2e-20260927T130451Z.json
                                   13:03:08Z → 13:04:51Z

A  park → answer → resume      13:03:08  a 41000 refund on order-a-764407 starts
                               13:03:11  the question was announced (1); the consumer
                                         stub names it; no refund yet; the question is a
                                         row in the approvals table
                               13:03:14  answered → exactly one refund
                               13:03:34  the same answer again, then a different one,
                                         then one for a made-up question id
                                         → still exactly one refund (1)
B  nobody answers              13:03:39  announced; expires_at is an absolute instant
                                         (2026-09-27T13:07:18Z); the default
                                         {"action": "reject", "reason": "no response
                                         within PT2M"} is published with it; the consumer
                                         sends it as an ordinary answer
                               13:03:59  no refund (0)
                               13:04:19  a late approval changes nothing
C  a different first answer    13:04:24  announced
                               13:04:26  the refund happened
                               13:04:51  a second answer runs nothing — still one (1)
Z  dead-letter queue           13:04:51  empty
stack destroyed; 0 resources on the project tag and 0 schedules remain
```

**What this run does not settle.** The example graph's tool is decorated
`@wait(FINANCE)` — unconditional. So the run proves 0.8.0's unchanged path is still
correct end to end on real AWS, and says nothing about `when=` above a real queue. The
`when=` evidence is local: seven tests, one of them a real graph. Closing this needs a
threshold on the example's tool and a below-limit scenario that asserts *no* question is
announced; it is a small change and it needs the owner's SSO to re-run. Recorded in §7.

The 0.5.0 run below is kept for the detail it carries.

---

Run on 2026-09-12 from the owner's SSO session, `ap-south-1`, stack `agent-wait-poc`
(tag `project=agent-wait`), via `scripts/deploy_and_e2e.sh --destroy`: local suite →
bundle → `cdk deploy` → `examples/refund_agent/demo_scenarios.py` → `cdk destroy`. The
stack is a Lambda running the refund graph behind an SQS queue (+ DLQ), a DynamoDB
checkpoint table, a DynamoDB approvals table (`DynamoDbAnnounce`), and an SNS topic
(`SnsAnnounce`) with a queue subscribed so the script can read what was announced.

```
15 / 15 checks passed              reports/e2e-20260912T093158Z.json
A  park → answer → resume      the question was announced (SNS) and is a row in the
                                approvals table; no refund before the answer; exactly one
                                refund after it; the same answer again, a different
                                answer, and an answer for an unknown question all run
                                nothing — still exactly one refund
B  nobody answers               expires_at is an absolute instant; the default
                                {"action": "reject", ...} is published with it; the
                                consumer sends the default as an ordinary answer → no
                                refund; a late approval changes nothing
C  a different first answer     the refund happened once; a second answer runs nothing
Z  DLQ                          empty
stack destroyed; no agent-wait queues, topics, tables, functions or log groups remain
```

What only this level settles, and now does: `DynamoDbAnnounce` against real DynamoDB
(the row is there with the documented keys), the Lambda's IAM grants, SQS redelivery
under a real queue, and the host's one `if` on `question_id` end to end. Two runs before
the passing one did not reach the scenarios (a stale `cdk.out` copied into the bundle;
a stale `--timeout-seconds` flag in the deploy script) — both fixed, both torn down.

## 7. What is not verified

**`when=` above a real queue.** The deployed run used the unconditional example (§6).
Local evidence only, including one real graph.

**That a caller's predicate is deterministic.** `when` is called again when the framework
re-runs the node, so a predicate that answered differently the second time would let the
body run without an answer. This is a property of two invocations of someone else's
lambda; the library cannot check it and does not try. Documented in the `wait.py`
docstring, the README and the frameworks page. A test could only pin a predicate written
to be deterministic, which proves nothing, so there is none.

**Async mode with a real model.** The decorator is exercised by calling the tool
directly inside a node. "The model sees pending and says something sensible" needs a model.

**Pydantic AI with a real model.** The 13 tests and the smoke section drive real agents
through a scripted `FunctionModel`. "A real model calls the tool, is denied, and says
something sensible" needs a model.

**Strands with a real model.** The 12 tests and the smoke section use a scripted
model. "A real model calls the tool, is refused, and says something sensible" needs a
model. `S3SessionManager` is not exercised; `FileSessionManager` is, and both implement
the same `SessionManager` hooks.

## 8. Deviations

000. **0.8.0 additions.**
    (a) **This report was missing.** 0.8.0 shipped to PyPI in commit `207ae16` without a
    TEST_REPORT of its own — the file still read v0.7.0 while `pyproject.toml` said
    0.8.0. CLAUDE.md names this file as the deliverable, so the release was incomplete
    until now. Written after the fact, from a re-run of every level rather than from
    memory of the release.
    (b) **`reports/junit.xml` and `reports/coverage.xml` were two releases stale**
    (2026-09-11, 137 tests, against 172 now). REQUIREMENTS §12 asks for the junit XML to
    be attached; an attached artifact that describes a different commit is worse than
    none, because it looks like evidence. Both regenerated from the run in §5. They are
    written by hand, not by CI, which is why they drifted — noted as a real weakness.
    (c) **0.8.0 changes the core**, which 0.6.0 and 0.7.0 deliberately did not. The
    owner's constraint on those two was "no change to the framework"; here the owner
    asked for the feature *in* `@wait` ("shouldn't the `@wait` be conditional, so the
    user does not have to write two tools"), so the constraint does not apply. What was
    held to instead: no new entry point, no change to the `Framework` interface, no
    change to any implementor, and the `when=None` path unchanged.
    (d) The AWS e2e was re-run for this release and passed 15 / 15, but with the
    unconditional example, so it is not evidence about `when=`. Stated plainly in §6 and
    §7 rather than allowed to read as coverage of the new feature.
    (e) **The committed `uv.lock` said `agent-wait 0.6.0`.** It had not been refreshed
    for 0.7.0 or 0.8.0, so the lock and `pyproject.toml` disagreed by two releases in
    `main`. Nothing was built from it — the wheel comes from `pyproject.toml` and the
    workspace install is editable, which is why no job noticed — but it is a committed
    file stating a wrong version. Refreshed by the first `uv run` of this session and
    included here. The same drift class as (b): a file nothing reads goes stale silently.
    (f) **The `ci` run on the release commit failed.** `ruff format --check` formats the
    python fences inside `README.md` as well as real source, and the `when=` snippet added
    there had two spaces before a comment. The release workflow was unaffected and
    published a correct wheel; `ci` went green on the follow-up commit `ad3f28e`, which is
    a formatting-only change. Recorded because the tag's own `ci` run is red in the
    history and a reader will find it.

00. **0.7.0 additions.** (a) The tests live in `tests/strands_agents/`, not
    `tests/strands/`: a directory named `strands` on `sys.path` shadows the real package.
    (b) The 0.6.0 report said 152 tests; the collected count was 149 (I added instead of
    counting). Corrected here. (c) Two doc lines were corrected by the tests: a duplicate answer on Strands
    *raises* (`ValueError`) rather than becoming a stray user message as I first wrote
    from the source; and a fresh `Agent` on the same session needs a model whose next
    turn is the *final* one, since the tool call is already in the restored history.


0. **0.6.0 additions.** (a) `tests/langgraph/test_hitl.py` → `test_wait_langgraph.py`
   and the new file is `test_wait_pydantic_ai.py`: pytest refuses two `test_wait.py`
   basenames without `__init__.py` files. (b) The smoke script imports Pydantic AI at
   module level rather than inside the section: with `from __future__ import annotations`
   Pydantic AI cannot resolve a *locally* imported `RunContext` annotation — reproduced
   on a plain `@agent.tool` with no `@wait`, so not ours, but worth knowing. (c) CrewAI
   was analysed and declined; Strands remains designed-against only (REQUIREMENTS §23).

1. **Bare install is the core, not everything.** The owner asked whether one package
   should install all frameworks and providers; my recommendation was that a bare install
   pull nothing, so a LangGraph user never carries boto3 and vice versa. Agreed.

2. **AWS announcers came under strict pyright.** They were outside the old `include`.
   Bringing them in cost a handful of `Any` annotations on injected clients (a caller may
   hand in any client-shaped object) and one `pyright: ignore` per `boto3.client(...)`
   call, because the stubs' overloads return `Unknown` for services not installed.

3. **The bundle script excludes `cdk/`.** With the CDK app now inside the example, the
   first bundle build copied a stale `cdk.out` into the zip and hit Windows path limits.
   `cdk` and `cdk.out` are pruned; that is the only reason the first deploy attempt
   failed, before anything was created.

4. **`verify_docs.py` stale-word check narrowed** from `republish` to `republish(` /
   `.republish` — the English word is legitimately in four pages; the removed method is
   what the check is for.

5. **Deviations 1–10 of the 0.4.1 report** stand and are not repeated here.

## 9. Open questions

None about the design. Three calls that are the owner's, not mine:

1. **Close the `when=` e2e gap?** It needs a threshold on the example's tool and a
   below-limit scenario asserting that *nothing* is announced, then a deploy and a
   teardown on the owner's SSO session. I have not done it because it spends the owner's
   account on a path already covered by a real-graph test, and because 0.8.0 is published
   — it would be evidence added after the fact. Worth doing before 0.9.0 touches the core
   again.
2. **Should `reports/junit.xml` and `reports/coverage.xml` come from CI?** They are
   written by hand today, which is exactly why they sat two releases stale (§8). The CI
   workflow already runs the suite; having it commit or attach the artifacts would remove
   the failure mode rather than remind me about it.
3. **Yank `langgraph-wait` and `agent-wait-aws` 0.4.1 from PyPI, or leave them as
   history?** They have no downloads. Unchanged from the 0.7.0 report.
