# CLAUDE.md — working agreements for this repository

You are the developer. The project manager's requirements are in `REQUIREMENTS.md`; it is the contract. Read it fully before writing code.

## Ground rules
- Python 3.12, `uv` workspace, `ruff`, `pyright` (strict on `agent_wait`), `pytest`. Run `uv run pytest` before every commit.
- Public API is exactly `ask()`, `WaitRuntime.dispatch()`, `WaitRuntime.register()`, the `AnnounceAdapter` protocol, and the two message formats. Do not add a second entry point, a receiver service, or a `host` flag. If a requirement seems to need one, stop and write the question in `TEST_REPORT.md` → Open questions.
- Every correctness rule in REQUIREMENTS §10 has a named conformance test. Name tests after the rule (`test_rule_01_idempotent_create`).
- Never log tokens or question payloads at INFO. Never print AWS account ids into reports.
- Conventional commits. Small PRs. No force-push.

## Reference material
- `reference/wait-sim/` is an earlier prototype. Reuse its DynamoDB CAS semantics, token format, and scenario narratives. Do **not** reuse its `Waiter.answer()` / answer-Lambda / timer-Lambda shape — those were replaced by `dispatch()` and `SchedulerAnnounce`.
- `docs/architecture.md` should be written by you from REQUIREMENTS §3–§10, with one Mermaid diagram of ask → register → announce → world → entry point → dispatch → resume.

## AWS
- Credentials: see REQUIREMENTS §14. Try the linked device first (`aws sts get-caller-identity`). If unavailable, produce the owner-run scripts and mark e2e as "awaiting owner run". Never ask for or handle pasted credentials.
- Region `ap-south-1`, stack prefix `agent-wait-poc`, tag `project=agent-wait`, tear down after e2e.

## Deliverable
`TEST_REPORT.md` at repo root, committed, and its full contents as your final message. Structure per REQUIREMENTS §12.
