# TEST_REPORT — agent-wait v0.7.0

Supersedes the v0.6.0 report. Earlier reports are at their tags.

## 0. Read this first

**One package, two things in it, three frameworks.** `@wait` makes a function wait for
an answer from outside the process; `publish_interrupts(result, thread_id, announce)`
after the run announces what the framework parked on. Both are written once against the
three-method `Framework` interface. `agent_wait.langgraph` (0.5), `agent_wait.pydantic_ai`
(0.6) and, new in 0.7, `agent_wait.strands` are the implementors; the AWS announcers are
the providers.

**What changed in 0.7.** The `[strands]` extra. Owner's constraint, as for 0.6: no change
to the core or the interface — met; `wait.py`, `framework.py` and the core contract test
are untouched. The implementor is 40 lines.

**Verified:** every local level, including 12 tests that drive real Strands agents (a
scripted model vendored from the SDK's own test fixture, no network) through park →
publish → approve / edit / deny → re-run, plus resume from a *new* `Agent` on the same
`FileSessionManager` session; the built wheel in a clean venv (bare = core; four guards
name their extra; `[langgraph,pydantic-ai,strands,aws]` passes the smoke test, which now
has a Strands section). **Not re-run:** the AWS e2e (LangGraph path unchanged since
0.5.0).

---

## 1. Summary

| | v0.6.0 | v0.7.0 |
|---|---|---|
| Extras | `[langgraph]`, `[pydantic-ai]`, `[aws]` | + `[strands]` (`strands-agents>=1.55,<2`) |
| Framework implementors | LangGraph, Pydantic AI | + Strands Agents |
| Core / `Framework` interface | unchanged | unchanged |
| Tests | 149 (65 core, 46 LangGraph, 13 Pydantic AI, 25 AWS) | 161 (+12 Strands) |
| Coverage | 99% | 99% (601 statements, 9 missed) |
| Smoke test | + Pydantic AI section | + Strands section |
| AWS e2e | not re-run | not re-run (LangGraph path unchanged) |

## 2. What was built

**`agent_wait.strands` (`[strands]`), new.** `StrandsFramework`: `interrupt` finds the
`ToolContext` among the call's arguments and returns
`tool_context.interrupt("agent_wait", reason=packed)` — Strands raises
`InterruptException` the first time and returns the human's response on the re-run, so
the wrapper needs no re-run detection of its own; `interrupts_in` reads
`result.interrupts` when `stop_reason == "interrupt"` and yields `(Interrupt.id,
Interrupt.reason)`; `current_thread_id` reads `tool_context.agent.session_id`;
`hidden_params = ("tool_context",)`. A `@wait` tool without a `ToolContext` raises
`TypeError` at the first call (Strands turns it into an error tool result). Then
`wait = make_wait(...)`, `publish_interrupts = make_publish_interrupts(...)`.


**`agent_wait.pydantic_ai` (`[pydantic-ai]`), new.** `PydanticAIFramework`:
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

## 3. What was removed since 0.4.1, and why

- **Two distributions.** Three coupled packages were a pin-drift hazard (bitten once at
  0.3.0) and a naming problem per framework × provider. `langgraph-wait` and
  `agent-wait-aws` stay on PyPI at 0.4.1 and are not updated.
- **The name `hitl`.** A person is the common answerer, not the only one; "wait" is the
  base concept and the package was already called that. Discoverability moved to metadata.
- **Direct LangGraph calls inside the decorator.** Replaced by the `Framework` interface
  so a second framework is a subpackage and an extra, not a fork of the decorator.

## 4. How it was tested

**Strands (12 tests, new).** Real `Agent`s with a scripted `Model` (the SDK's own
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


**Pydantic AI (13 tests, new).** Real `Agent`s with a scripted `FunctionModel`
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

**Everything below is the 0.5.0 evidence, re-run and still green.**

**Core (47 tests).** As before for `publish()`, `build_envelope()`, the envelope field
set, `dedupe_key`, the announcers (webhook against a real `http.server`). New
`test_framework.py` (11): a stub `Framework` that parks by raising — `@wait` packs
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
161 passed, 4 skipped (the e2e level, opt-in)
ruff check      clean
ruff format     clean
pyright strict  0 errors (src/ — core, langgraph, pydantic_ai, strands, aws)
coverage        99% (601 statements, 9 missed)
wheel smoke     ok (bare = core only; four guards; [langgraph,pydantic-ai,strands,aws] full smoke)
mkdocs --strict ok; every doc snippet parsed and its imports resolved
```

## 5. The deployed AWS run (0.5.0; not re-run for 0.6.0 — the LangGraph path is unchanged)

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

## 6. What is not verified

**Async mode with a real model.** The decorator is exercised by calling the tool
directly inside a node. "The model sees pending and says something sensible" needs a model.

**Pydantic AI with a real model.** The 13 tests and the smoke section drive real agents
through a scripted `FunctionModel`. "A real model calls the tool, is denied, and says
something sensible" needs a model.

**Strands with a real model.** The 12 tests and the smoke section use a scripted
model. "A real model calls the tool, is refused, and says something sensible" needs a
model. `S3SessionManager` is not exercised; `FileSessionManager` is, and both implement
the same `SessionManager` hooks.

## 7. Deviations

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

## 8. Open questions

None for the library. Whether to yank `langgraph-wait` and `agent-wait-aws` 0.4.1 from
PyPI or leave them as history is the owner's call; they have no downloads.
