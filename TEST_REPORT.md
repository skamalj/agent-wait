# TEST_REPORT — agent-wait v0.5.0

Supersedes the v0.4.1 report. Earlier reports are at their tags.

## 0. Read this first

**One package, two things in it.** `@wait` makes a function wait for an answer from
outside the process — on a tool the body runs only on approve; on a node with a
`decision` parameter it always runs and gets the answer; in `mode="async"` the decorator
publishes and returns pending without parking. `publish_interrupts(result, thread_id,
announce)` after the run reads what the framework returned and hands envelopes to
announcers. No graph handle, no state read, nothing received.

**What changed in 0.5.** `langgraph-wait` and `agent-wait-aws` are subpackages of
`agent-wait` behind extras (`[langgraph]`, `[aws]`); bare install is the dependency-free
core. `@hitl` is renamed `@wait`. The decorator and `publish_interrupts` are written once
against a three-method `Framework` interface; `agent_wait.langgraph` is one implementor.
Nothing in the envelope, the answer shape, or the behaviour changed.

**Verified:** every local level; the core contract against a stub framework with no
LangGraph installed; the built wheel in a clean venv (bare = core only, guards name the
extra, `[langgraph,aws]` runs the full smoke test); `cdk synth`; the deployed AWS run
(§5). The PyPI round trip is verified by the release workflow on every tag.

---

## 1. Summary

| | v0.4.1 | v0.5.0 |
|---|---|---|
| Distributions | `agent-wait`, `langgraph-wait`, `agent-wait-aws` | `agent-wait` with extras `[langgraph]`, `[aws]`, `[all]` |
| Bare install | core | core, no dependencies |
| Decorator | `@hitl` | `@wait` |
| Imports | `from langgraph_wait import hitl, publish_interrupts` / `from agent_wait_aws import …` | `from agent_wait.langgraph import wait, publish_interrupts` / `from agent_wait.aws import …` |
| Framework coupling | `langgraph_wait` written directly against LangGraph | `Framework` ABC in core; `LangGraphFramework` + two bindings |
| Core exports added | — | `Framework`, `make_wait`, `make_publish_interrupts`, `question_id_for` |
| pyright strict scope | core only | all of `src/` (AWS announcers now typed) |
| Tests | 125 | 139 (+11 core contract, +3 moved/renamed) |
| Coverage | 98% | 99% |
| AWS e2e | not run | run, see §5 |

## 2. What was built

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
139 passed, 4 skipped (the e2e level, opt-in)
ruff check      clean
ruff format     clean
pyright strict  0 errors (src/ — core, langgraph, aws)
coverage        99% (513 statements, 7 missed)
cdk synth       ok (examples/refund_agent/cdk)
wheel smoke     ok (bare = core only; [langgraph,aws] full smoke)
mkdocs --strict ok
```

## 5. The deployed AWS run

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

**A second framework.** The `Framework` interface is exercised by the stub and by
LangGraph. Strands is designed against (its `tool_context.interrupt`, per-tool-call ids,
`result.interrupts`) but not written; the docs say "planned".

## 7. Deviations

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
