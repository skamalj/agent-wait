# agent-wait

[![PyPI](https://img.shields.io/pypi/v/agent-wait.svg)](https://pypi.org/project/agent-wait/)
[![CI](https://github.com/skamalj/agent-wait/actions/workflows/ci.yml/badge.svg)](https://github.com/skamalj/agent-wait/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/skamalj/agent-wait/blob/main/LICENSE)

**Publish a LangGraph agent's interrupts to the outside world, so a human can answer them.**

A LangGraph node calls `interrupt()` and the graph stops. If the agent runs in a Lambda,
a container, or anything else that doesn't stick around, the process exits and nobody
knows a question was asked or where to send the answer. agent-wait takes that pause and
puts it somewhere people can see it — a topic, a queue, a webhook, a database row — with
everything needed to answer it in one envelope.

It does not receive the answer. That part is yours, and it is about a dozen lines.

```bash
pip install agent-wait langgraph-wait          # core + LangGraph
pip install agent-wait-aws                     # SNS / SQS / EventBridge / DynamoDB announcers
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
plain `interrupt(value)` works too, with default policy — a graph that already interrupts
gets published with no edit at all.

**In the host** — wire it once:

```python
from agent_wait import WaitPublisher
from agent_wait_aws import SnsAnnounce
from langgraph_wait import LangGraphAdapter

agent = WaitPublisher(LangGraphAdapter(graph), announce=[SnsAnnounce(topic_arn)])
```

**Then route each message.** Starts and answers arrive at the same place; `interrupt_id`
tells them apart:

```python
from langgraph_wait import is_answer, resume_command


def route(message):
    thread_id = message["thread_id"]
    if is_answer(message):
        if not is_still_open(thread_id, message["interrupt_id"]):
            return  # somebody already answered
        return agent.invoke(resume_command(message), thread_id)
    if agent.pending(thread_id):
        return agent.republish(thread_id)  # a redelivery; don't re-ask
    return agent.invoke(message["input"], thread_id)


def is_still_open(thread_id, interrupt_id):
    return any(p.interrupt_id == interrupt_id for p in agent.pending(thread_id))
```

That is the complete integration. [`examples/refund_agent/`](https://github.com/skamalj/agent-wait/tree/main/examples/refund_agent)
is it, deployed to Lambda behind SQS.

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
  "reply_with": { "thread_id": "order-4471", "interrupt_id": "a1b2c3d4e5f60718", "answer": null }
}
```

`reply_with` is a filled-in stub: the consumer copies it, sets `answer`, and posts it to
wherever your agent listens. Whatever goes in `answer` is what the `ask()` call returns —
verbatim, with nothing merged into it.

A second envelope, `wait.resumed`, goes out when the graph moves past the question, so a
UI knows to retract the button.

Full schema, including how to deduplicate:
[Message formats](https://skamalj.github.io/agent-wait/message-formats/).

## Announcers

An announcer is the only thing you are expected to implement. Subclass `BaseAnnounce`
and write one method:

```python
from agent_wait import BaseAnnounce


class RedisAnnounce(BaseAnnounce):
    name = "redis"

    def __init__(self, client, **kw):
        super().__init__(**kw)
        self.client = client

    def deliver(self, envelope, transition):
        self.client.set(envelope.dedupe_key, envelope.to_json())
```

The contract — **an announcer must never raise into the run** — is enforced by the base
class: an exception from `deliver()` becomes a log line, and the graph that just parked
stays parked.

Because nothing reads state back through this library, "announce" doesn't have to mean
"publish an event". It means *put the question where whoever answers it will find it*:

| Adapter | Package | Where the question lands |
|---|---|---|
| `WebhookAnnounce` | `agent-wait` | A URL. JSON POST, optional HMAC-SHA256 signature in the GitHub/Stripe shape. Stdlib only. |
| `LogAnnounce` | `agent-wait` | A structured log line. The question never reaches INFO. |
| `InMemoryAnnounce` | `agent-wait` | A list. For tests. |
| `SnsAnnounce` | `agent-wait-aws` | A topic; policy `tags` become message attributes for subscription filters. |
| `SqsAnnounce` | `agent-wait-aws` | A queue; on FIFO, grouped by thread and deduplicated on the stable key. |
| `EventBridgeAnnounce` | `agent-wait-aws` | A bus, with the transition as detail-type. Notices partial failures behind a 200. |
| `DynamoDbAnnounce` | `agent-wait-aws` | **A row.** `open` on `created`, `closed` on `resumed`. A GSI on `status` gives an approvals UI its query with no broker anywhere. |

Pass as many as you like; failures are contained per adapter. The full guide — what
`deliver()` receives, why `dedupe_key` is the one field to get right, patterns from the
shipped adapters, and how to test yours:
[Writing an announcer](https://skamalj.github.io/agent-wait/writing-an-announcer/).

## What the library does *not* do

Deliberately — each of these is where teams' own opinions live:

- **Receive answers.** No inbound endpoint, no validation, no tokens. The router above is yours.
- **Enforce the timeout.** `expires_at` and `default` are published; a sweep of yours
  sends the default when the deadline passes. There is a
  [working one](https://github.com/skamalj/agent-wait/blob/main/examples/refund_agent/demo_scenarios.py)
  in the example.
- **Decide a race.** `pending()` rejects an answer the graph has already moved past.
  Two *different* answers in the same instant are your transport's problem — SQS FIFO
  keyed by thread solves it; an HTTP endpoint with concurrent handlers needs a
  conditional write.
- **Authenticate.** Whoever can write to your entry point can answer.
- **Store anything.** LangGraph's checkpoint is the only state.

## Two LangGraph 1.2.x behaviours you should know about

Both verified against 1.2.11, both pinned by tests that fail if LangGraph changes them.

**`get_state().tasks[*].interrupts` over-reports** ([#4796](https://github.com/langchain-ai/langgraph/issues/4796),
[#6792](https://github.com/langchain-ai/langgraph/issues/6792)). Resume one of two parallel
interrupts and the finished task still lists its id. `pending()` filters on `task.result`,
which is `None` only while genuinely parked.

**Two interrupting tools in one `ToolNode` get the same id** ([#6626](https://github.com/langchain-ai/langgraph/issues/6626),
[#6624](https://github.com/langchain-ai/langgraph/issues/6624)). A different question under
an identical id defeats deduplication, and there is no filter for it. The rule is **one
`interrupt()` per node** — give each approval-requiring tool its own node, which is also
the fix for a node re-running its side effects on resume.

Details: [Architecture](https://skamalj.github.io/agent-wait/architecture/).

## Layout

```
packages/agent-wait        core. No LangGraph, no AWS, no dependencies. pyright strict.
packages/langgraph-wait    ask(), the adapter, resume_command(). The only LangGraph import.
packages/agent-wait-aws    four announce adapters, and a CDK stack.
examples/refund_agent      a graph, a router, and four scenarios against real AWS.
docs/                      message contract, architecture, announcer guide, consumer guide.
```

```bash
uv sync
uv run pytest
uv run ruff check . && uv run pyright
```

MIT. Issues and PRs at [github.com/skamalj/agent-wait](https://github.com/skamalj/agent-wait).
