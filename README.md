# agent-wait

Durable waits for agent frameworks. Turns a framework interrupt into a parked,
addressable, expirable wait that survives the process exiting and can be answered days
later from anywhere.

```python
from langgraph_wait import ask
from agent_wait import WaitPolicy


def review(state):
    decision = ask(
        {"kind": "refund_approval", "order_id": state["order_id"], "amount": state["amount"]},
        policy=WaitPolicy(
            timeout="P3D",
            default={"action": "reject", "reason": "no response in 3 days"},
            allowed_actions=("approve", "reject"),
            tags={"approver_group": "finance"},
        ),
    )
    return {"decision": decision}
```

That node now pauses for three days. The process it was running in can exit. The approval
can arrive from Slack, an email link, or a ticketing system, onto a machine that was not
running when the question was asked. If nobody answers, the declared default is applied.
If two people answer at once, one wins and the other is told so. If the message is
redelivered five times, the refund still goes out once.

## Why

Every framework can pause — LangGraph's `interrupt()`, Strands interrupts, Pydantic AI's
`DeferredToolRequests`, ADK long-running tools. None of them owns what happens *between*
the pause and the resume when the gap is days and the process is gone. The maintainers
have said as much in writing: scheduling is "left to the application layer"
([LangChain #2541](https://forum.langchain.com/t/2541)), and "how do I ensure only one
`thread_id` runs at a time across many pods?"
([#772](https://forum.langchain.com/t/772)) has been unanswered for fourteen months.

So everyone rebuilds the same thing badly. This is that layer, built once.

## The whole API

```python
decision = ask(question, policy)  # in your node

outcome = runtime.dispatch(payload)  # before graph.invoke()
result = runtime.register(out, config, thread_id)  # after it
```

Three functions, one pluggable interface (`AnnounceAdapter`), and
[two JSON message formats](docs/message-formats.md). No receiver service, no answer API,
no timer Lambda. **The world answers where your agent already listens**, and `dispatch()`
works out whether an inbound payload is a start, a resume, or something to ignore.

Timeouts are not a special case: `SchedulerAnnounce` is an announce adapter that delivers
an ordinary answer envelope — `action: "timeout"` — to that same entry point, three days
late. Which is why a human clicking at the moment the timer fires is not a race condition
but a single conditional write that exactly one party wins.

## Packages

| Package | Import | What it is |
|---|---|---|
| `agent-wait` | `agent_wait` | The core. No cloud SDK, no framework import. |
| `langgraph-wait` | `langgraph_wait` | `ask()` and the LangGraph adapter. |
| `agent-wait-aws` | `agent_wait_aws` | DynamoDB store, SQS/SNS/EventBridge/Scheduler adapters, Lambda wrapper, CDK stack. |

## Documentation

- [docs/architecture.md](docs/architecture.md) — how it fits together, and what is deliberately absent
- [docs/message-formats.md](docs/message-formats.md) — the wire contract
- [docs/integrating-a-consumer.md](docs/integrating-a-consumer.md) — building the thing a human clicks
- [REQUIREMENTS.md](REQUIREMENTS.md) — the v0.1 contract
- [TEST_REPORT.md](TEST_REPORT.md) — what was tested, what was found, what is missing

## Running it

```bash
uv sync --all-packages
uv run pytest                 # unit, conformance over three stores, LangGraph, moto
uv run ruff check . && uv run pyright
```

The end-to-end level needs a real account and is opt-in:

```bash
./scripts/deploy_and_e2e.sh --profile <your-sso-profile> --stack agent-wait-poc-you --destroy
```

It builds the Lambda bundle without Docker, deploys the CDK stack, runs the four
scenarios against real SQS, DynamoDB and EventBridge Scheduler, and tears everything down.

## Status

v0.1, proof of concept. Not published to PyPI. One framework (LangGraph ≥ 1.2), one cloud
(AWS). `FrameworkAdapter` and `WaitStore` are clean protocols so the second of each is
small — but v0.1 ships one of each that is genuinely tested rather than four that are
plausible.
