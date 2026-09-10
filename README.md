# agent-wait

Publish an agent's interrupts to the outside world, so a human can answer them.

A LangGraph node calls `interrupt()` and the graph stops. In a Lambda the process then
exits, and nothing remembers that a question was asked or knows where to send the answer.
agent-wait takes that pause and puts it somewhere people can see it — a topic, a queue, a
database row — with everything needed to answer it in one envelope.

It does not receive the answer. That part is yours, and it is about a dozen lines.

```bash
uv add agent-wait langgraph-wait
```

## The whole thing

**In the graph** — one line, where the decision belongs:

```python
from agent_wait import WaitPolicy
from langgraph_wait import ask

def review(state):
    if state["amount"] <= 5_000:
        return {"decision": {"action": "approve", "by": "policy:auto"}}

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

`ask()` is a thin wrapper over `interrupt()`. The node pauses exactly as LangGraph pauses;
what `ask()` adds is the policy, which rides along and comes back out in the envelope. A
plain `interrupt(value)` works too, with default policy — so a graph that already
interrupts gets published with no edit at all.

**In the host** — wire it once:

```python
from agent_wait import EntryPoint, WaitPublisher
from agent_wait_aws import SnsAnnounce
from langgraph_wait import LangGraphAdapter

agent = WaitPublisher(
    LangGraphAdapter(graph),
    announce=[SnsAnnounce(topic_arn)],
    reply_to=EntryPoint("sqs", queue_url),
)
```

**Then route each message.** Starts and answers arrive at the same place; `interrupt_id`
tells them apart:

```python
from langgraph_wait import is_answer, resume_command

def route(message):
    thread_id = message["thread_id"]
    if is_answer(message):
        if not is_still_open(thread_id, message["interrupt_id"]):
            return                                       # somebody already answered
        return agent.invoke(resume_command(message), thread_id)
    if agent.pending(thread_id):
        return agent.republish(thread_id)                # a redelivery; don't re-ask
    return agent.invoke(message["input"], thread_id)

def is_still_open(thread_id, interrupt_id):
    return any(p.interrupt_id == interrupt_id for p in agent.pending(thread_id))
```

That is the complete integration. `examples/refund_agent/` is it, deployed.

## What goes out

```json
{
  "type": "wait.created",
  "thread_id": "order-4471",
  "interrupt_id": "a1b2c3d4e5f60718",
  "question": { "kind": "refund_approval", "amount": 41000 },
  "allowed_actions": ["approve", "reject"],
  "expires_at": "2026-09-12T09:00:00Z",
  "default": { "action": "reject", "reason": "no response in 3 days" },
  "reply_to": { "kind": "sqs", "url": "https://sqs…/agent-runs.fifo" },
  "reply_with": { "thread_id": "order-4471", "interrupt_id": "a1b2c3d4e5f60718", "answer": null }
}
```

`reply_with` is a filled-in stub: the consumer copies it, sets `answer`, and posts it to
`reply_to`. Whatever goes in `answer` is what the `ask()` call returns — verbatim, with
nothing merged into it.

A second envelope, `wait.resumed`, goes out when the graph moves past the question, so a
UI knows to retract the button.

Full schema, including how to deduplicate: [docs/message-formats.md](docs/message-formats.md).

## Announcers

An announcer is the only thing you are expected to implement, and the contract is one
line long: **`announce()` must not raise.** If Slack is down, the graph still parked and
the run still completes.

```python
class MyAnnounce:
    name = "mine"

    def supports(self, transition): return True          # "created" / "resumed"
    def announce(self, envelope, transition): ...        # log failures, never raise
```

Because nothing reads state back through this library, "announce" does not have to mean
"publish an event". It means *put the question where whoever answers it will find it*.
That can be a broker — or it can be a table your UI already queries:

| Adapter | Package | What it does |
|---|---|---|
| `LogAnnounce` | `agent-wait` | A structured line per transition. The question never reaches INFO. |
| `InMemoryAnnounce` | `agent-wait` | Collects envelopes. For tests. |
| `SnsAnnounce` | `agent-wait-aws` | Publishes; policy `tags` become message attributes, so subscription filters can route. |
| `SqsAnnounce` | `agent-wait-aws` | Sends to a queue; on FIFO, groups by thread and dedupes on the stable key. |
| `EventBridgeAnnounce` | `agent-wait-aws` | `PutEvents` with the transition as detail-type. Notices partial failures, which return HTTP 200. |
| `DynamoDbAnnounce` | `agent-wait-aws` | **The question is the row.** `created` writes it `open`, `resumed` marks it `closed`. A GSI on `status` gives an approvals UI its query with no broker anywhere. |

Pass as many as you like; `CompositeAnnounce` fans out and contains each one's failures
separately. Redis, Postgres, a Slack webhook, a file on disk are all the same four lines
as `MyAnnounce` above.

## What the library does *not* do

Deliberately. Each of these was in v0.1 and was removed with the machinery behind it:

- **Receive answers.** No `dispatch()`, no inbound validation, no tokens.
- **Enforce the timeout.** `expires_at` and `default` are published; a sweep of yours
  sends the default when the deadline passes. There is a working one in
  `examples/refund_agent/demo_scenarios.py`.
- **Decide a race.** Two answers to one question: `pending()` rejects the late one, and
  SQS FIFO keyed by thread stops them arriving at once. Without ordering, you need your
  own conditional write.
- **Authenticate.** Whoever can write to `reply_to` can answer. The queue policy is the
  boundary.
- **Store anything.** LangGraph's checkpoint is the only state. `pending()` reads it;
  `republish()` re-announces from it.

If you need those guarantees in the library, [`v0.1.0`](docs/migrating-from-0.1.md) is
tagged and its test report stands.

## The one LangGraph bug you inherit

`get_state().tasks[*].interrupts` over-reports (langgraph
[#4796](https://github.com/langchain-ai/langgraph/issues/4796) /
[#6792](https://github.com/langchain-ai/langgraph/issues/6792)). With two parallel
interrupts, resume one, and the finished task still lists its interrupt id. Anything built
naively on that list will republish an approval for a node that has already run.

`pending()` filters on `task.result`, which is `None` only while a task is genuinely
parked. A fixture pins the behaviour, so a LangGraph release that fixes it shows up as a
failing test rather than as silence. Details in
[docs/architecture.md](docs/architecture.md).

## Layout

```
packages/agent-wait        core. No LangGraph, no AWS. pyright strict.
packages/langgraph-wait    ask(), the adapter, resume_command(). The only LangGraph import.
packages/agent-wait-aws    four announce adapters, and the CDK stack.
examples/refund_agent      a graph, a router, and the scenarios against real AWS.
docs/                      the message contract, the architecture, the 0.1 migration.
```

```bash
uv sync
uv run pytest
uv run ruff check . && uv run pyright
```
