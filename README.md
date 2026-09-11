# agent-wait

[![PyPI](https://img.shields.io/pypi/v/agent-wait.svg)](https://pypi.org/project/agent-wait/)
[![CI](https://github.com/skamalj/agent-wait/actions/workflows/ci.yml/badge.svg)](https://github.com/skamalj/agent-wait/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/skamalj/agent-wait/blob/main/LICENSE)

**Get a LangGraph interrupt out of the process, and the answer back in.**

When a node calls `interrupt()`, the graph pauses and the interrupt is handed to whatever
called `invoke()` — and that is where LangGraph stops. There is no built-in way to tell
anyone *else* that a question was asked, and no built-in way for anyone else to answer
it. The moment the question has to reach a person on Slack, an approvals dashboard, a
ticket queue or another service, you are writing that code yourself — whether your agent
is a server that runs for a year or a Lambda that is gone in seconds.

agent-wait is that code. It takes the interrupt and puts it somewhere people can see it —
a topic, a queue, a webhook, a database row — with everything needed to answer it in one
envelope, and documents the shape of the answer so the return leg is one `if`.

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
    decision = ask(
        {"kind": "refund_approval", "order_id": state["order_id"], "amount": state["amount"]},
        policy=WaitPolicy(
            timeout="P3D",
            default={"action": "reject"},
            allowed_actions=("approve", "reject"),
            tags={"approver_group": "finance"},
        ),
    )
    return {"decision": decision}  # exactly what the approver sent
```

`ask()` is a thin wrapper over `interrupt()`; the policy rides along inside the
interrupt value. A plain `interrupt(value)` works too, with the default policy.

**After the run** — one call:

```python
from agent_wait_aws import SnsAnnounce
from langgraph_wait import publish_interrupts

result = graph.invoke(value, {"configurable": {"thread_id": thread_id}})
publish_interrupts(result, thread_id, announce=[SnsAnnounce(topic_arn)])
```

It reads `result["__interrupt__"]` — `Interrupt.id` and `Interrupt.value`, nothing else —
and hands one envelope per question to the announcers. No graph handle, no `get_state()`,
no checkpointer. If nothing interrupted, it does nothing.

**When the answer comes back** — the one `if` you write:

```python
from langgraph.types import Command

if "question_id" in message:  # an answer
    value = Command(resume={message["question_id"]: message["answer"]})
else:  # a start
    value = message["input"]

result = graph.invoke(value, {"configurable": {"thread_id": message["thread_id"]}})
publish_interrupts(result, message["thread_id"], announce)
```

That is the complete integration. A duplicate answer runs nothing — LangGraph ignores a
resume for a question the thread has moved past — so there is nothing to check.

## Or tag the tool

```python
from langgraph_wait import hitl


@tool
@hitl(WaitPolicy(timeout="P1D", default={"action": "reject"}, tags={"approver_group": "finance"}))
def issue_refund(order_id: str, amount: int) -> str: ...
```

`@hitl` calls `ask()` with `{tool, args}` before the tool runs. `{"action": "approve"}`
runs it (optionally with edited `args`); anything else is returned to the model as the
tool's result. It also registers the policy by tool name, so questions raised by
LangChain's `HumanInTheLoopMiddleware` — which batches several tool calls into one
interrupt — are published with it.

**Async mode — the thread does not park:**

```python
@tool
@hitl(FINANCE, mode="async", announce=[SnsAnnounce(topic_arn), DynamoDbAnnounce(table)])
def issue_refund(order_id: str, amount: int) -> str: ...
```

The decorator *is* the publisher: the tool call announces the question and returns
`{"status": "pending_approval", "question_id": ...}` without running. The graph carries
on; nothing is parked; nothing to call after the run. The decision arrives later as a new
message and your graph acts on it. This is the mode for a single-thread channel like
WhatsApp, where the approver is not the person on the thread.

## What goes out

```json
{
  "type": "wait.created",
  "thread_id": "order-4471",
  "question_id": "a1b2c3d4e5f60718",
  "question": { "kind": "refund_approval", "amount": 41000 },
  "allowed_actions": ["approve", "reject"],
  "expires_at": "2026-09-14T09:00:00Z",
  "default": { "action": "reject" },
  "source": { "tool": "issue_refund" },
  "reply_with": { "thread_id": "order-4471", "question_id": "a1b2c3d4e5f60718", "answer": null }
}
```

`reply_with` is a filled-in stub: copy it, set `answer`, send it back. Whatever goes in
`answer` is what `ask()` returns — verbatim.

Full schema, the recommended answer shape, and how to deduplicate:
[Message formats](https://skamalj.github.io/agent-wait/message-formats/).

## Announcers

Subclass `BaseAnnounce`, write one method:

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

A raise inside `deliver()` becomes a log line; the run completes. Shipped:

| Adapter | Package | Where the question lands |
|---|---|---|
| `WebhookAnnounce` | `agent-wait` | A URL. JSON POST, optional HMAC-SHA256 signature. Stdlib only. |
| `LogAnnounce` | `agent-wait` | A structured log line. The question never reaches INFO. |
| `InMemoryAnnounce` | `agent-wait` | A list. For tests. |
| `SnsAnnounce` | `agent-wait-aws` | A topic; `tags` become message attributes for subscription filters. |
| `SqsAnnounce` | `agent-wait-aws` | A queue; on FIFO, grouped by thread, deduplicated on the stable key. |
| `EventBridgeAnnounce` | `agent-wait-aws` | A bus. Notices partial failures behind a 200. |
| `DynamoDbAnnounce` | `agent-wait-aws` | **A row**, `status=open`, with a GSI an approvals UI can query. Write-only. |

[Writing an announcer](https://skamalj.github.io/agent-wait/writing-an-announcer/).

## What the library does *not* do

- **Receive answers.** No endpoint, no validation, no ledger reads. The `if` above is yours.
- **Enforce anything.** `expires_at`, `default`, `answer_ttl`, `allowed_actions` are
  published so the consumer has the asker's intent. Acting on them is the consumer's.
- **Store anything.** LangGraph's checkpoint is the only state in interrupt mode; in
  async mode there is none unless you keep one.

## One LangGraph 1.2.x behaviour to know

Two tools that each call `interrupt()`, dispatched by one `ToolNode`, get the **same
interrupt id** ([#6626](https://github.com/langchain-ai/langgraph/issues/6626)), and only
one surfaces per run ([#6624](https://github.com/langchain-ai/langgraph/issues/6624)).
A different question under an identical id defeats deduplication. Use one interrupting
tool per node, or `HumanInTheLoopMiddleware`, which batches them into one interrupt.
Pinned by a test that fails if LangGraph changes it.

## Layout

```
packages/agent-wait        core: Question, WaitPolicy, the envelope, publish(), announcers. No deps.
packages/langgraph-wait    ask(), @hitl, publish_interrupts(). The only LangGraph import.
packages/agent-wait-aws    four announce adapters, and a CDK stack for the example.
examples/refund_agent      a graph and a host, deployed to Lambda behind SQS.
docs/                      message contract, architecture, announcer guide, consumer guide.
```

MIT. Issues and PRs at [github.com/skamalj/agent-wait](https://github.com/skamalj/agent-wait).
