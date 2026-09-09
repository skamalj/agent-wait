# Kickoff — agent-wait v0.1 (local Claude Code session)

You are the developer on this project. Your project manager is a Claude Cowork session named `claude-3e`; the owner is Kamaljeet Singh.

## Read first, in this order
1. `CLAUDE.md` — working agreements.
2. `REQUIREMENTS.md` — the full contract. Every section is normative. §16 lists decisions already taken; do not re-open them.
3. `reference/wait-sim/` — an earlier prototype. Reuse store CAS semantics, token format and scenario narratives; do NOT reuse its answer-Lambda / timer-Lambda / `Waiter.answer()` shape (superseded by `dispatch()` and `SchedulerAnnounce`).

## Environment facts for this session
- You are running **locally on the owner's Windows machine**, in `Documents\dev\agent-wait`, which is already a git repository with one commit on `main`.
- **AWS SSO is already logged in** on this machine. Verify with `aws sts get-caller-identity` before deploying. Use region `ap-south-1` unless the SSO profile defaults elsewhere. Stack name prefix `agent-wait-poc`, tag everything `project=agent-wait`, tear down after the e2e run.
- **GitHub**: `gh` is available. First task: `gh repo create skamalj/agent-wait --private --source=. --remote=origin --push`. Then work on branches and open PRs into `main`; conventional commits; never force-push.
- Python 3.12 and `uv` are expected. If a tool is missing, install it and note it in the report.

## Plan of work (suggested order; adjust and record deviations)
1. Repo scaffold: uv workspace, three packages per REQUIREMENTS §15, ruff/pyright/pytest config, GitHub Actions running the local test levels.
2. `agent-wait` core: model, policy, token codec, in-memory + SQLite stores, `LogAnnounce`/`InMemoryAnnounce`/`CompositeAnnounce`, `WaitRuntime.dispatch()` and `.register()` exactly per §4, sweeper. Unit + conformance tests for all 12 rules in §10 with a fake clock and crash injection.
3. `langgraph-wait`: `ask()`, `LangGraphAdapter`; the spike test on `Interrupt.id` stability and `get_state().tasks[*].interrupts`; integration tests on the refund graph, a parallel-interrupt graph, and a subgraph-interrupt graph.
4. `agent-wait-aws`: `DynamoWaitStore` (single table, conditional expressions), `SqsAnnounce`, `SnsAnnounce`, `EventBridgeAnnounce`, `SchedulerAnnounce` (the timeout), `EntryPoint`, `make_run_handler`, Secrets Manager key provider; moto tests.
5. CDK stack + `examples/refund_agent`; deploy; run the four e2e scenarios in §13 with a 2-minute timeout; capture CloudWatch evidence; tear down.
6. Docs (`docs/message-formats.md`, `docs/integrating-a-consumer.md`, `docs/architecture.md`), package READMEs.
7. `TEST_REPORT.md` at repo root per §12, committed and pushed. Then, if peer messaging to other Claude sessions is available, send the report's summary to the session named `claude-3e`; otherwise just finish with the report as your final message.

## Rules of engagement
- Ask the owner only when REQUIREMENTS is genuinely silent and §16 does not settle it. Otherwise decide, and log the decision in `TEST_REPORT.md → Deviations & decisions`.
- Never print AWS account ids, tokens or question payloads into logs or the report.
- Commit small and often. Keep `main` green.

Start now. Begin by reading the three files above, then run the GitHub step.
