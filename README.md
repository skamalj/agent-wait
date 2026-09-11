# agent-wait

[![PyPI](https://img.shields.io/pypi/v/agent-wait.svg)](https://pypi.org/project/agent-wait/)
[![CI](https://github.com/skamalj/agent-wait/actions/workflows/ci.yml/badge.svg)](https://github.com/skamalj/agent-wait/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/skamalj/agent-wait/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-skamalj.github.io%2Fagent--wait-black.svg)](https://skamalj.github.io/agent-wait/)

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

**In the graph** — one decorator, on the function where the decision belongs:

```python
from agent_wait import WaitPolicy
from langchain_core.tools import tool
from langgraph_wait import hitl

FINANCE = WaitPolicy(
    timeout="P3D",
    default={"action": "reject"},
    allowed_actions=("approve", "reject"),
    tags={"approver_group": "finance"},
)


@tool
@hitl(FINANCE)
def issue_refund(order_id: str, amount: int) -> str:
    payments.refund(order_id, amount)  # runs only if the answer is {"action": "approve"}
    return "refunded"
```

Calling `issue_refund(...)` inside a run parks the graph on `{"function": "issue_refund",
"args": {...}}`. On resume, `{"action": "approve"}` runs the body — optionally with edited
`args` — and anything else is returned in its place, so a model sees why it did not run.

The policy is what the asker declares about the question. Every field is published and
none is enforced — the consumer acts on them:

| field | meaning |
|---|---|
| `timeout` | ISO 8601 duration. Published as an absolute `expires_at`. |
| `default` | What to send as the answer if nobody answers by then. |
| `answer_ttl` | How long an answer stays usable after it is given. |
| `allowed_actions` | Which answers are meaningful — the buttons to draw. |
| `tags` | Routing hints; become SNS message attributes. |

A node that needs the answer itself declares a `decision` parameter and always runs,
approve or not, with the answer in it:

```python
@hitl(FINANCE)
def review(state, decision=None):
    return {"decision": decision}  # verbatim: {"action": "approve", "note": "ok"}
```

The parameter name is yours — `@hitl(FINANCE, decision="verdict")` looks for `verdict`.

**After the run** — one call:

```python
from agent_wait_aws import SnsAnnounce
from langgraph_wait import publish_interrupts

result = graph.invoke(value, {"configurable": {"thread_id": thread_id}})
publish_interrupts(result, thread_id, announce=[SnsAnnounce(topic_arn)])
```

It reads `result["__interrupt__"]` — `Interrupt.id` and `Interrupt.value`, nothing else —
and hands one envelope per question to the announcers. No graph handle, no `get_state()`,
no checkpointer. If nothing interrupted, it does nothing. A plain `interrupt(value)` from
a node that never heard of this library is published too, with the default policy.

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

## Async mode — the thread does not park

```python
from agent_wait_aws import DynamoDbAnnounce, SnsAnnounce


@tool
@hitl(FINANCE, mode="async", announce=[SnsAnnounce(topic_arn), DynamoDbAnnounce(table)])
def issue_refund(order_id: str, amount: int) -> str: ...
```

The decorator *is* the publisher: the call announces the question and returns
`{"status": "pending_approval", "question_id": "...", "function": "issue_refund"}`
without running the body. The graph carries on; nothing is parked; nothing to call after
the run. The decision arrives later as a new message and your graph acts on it. This is
the mode for a single-thread channel like WhatsApp, where the approver is not the person
on the thread — and since nothing is parked, LangGraph remembers nothing about the
question; the `DynamoDbAnnounce` row (or whatever you keep) is the only record.

## What goes out

```json
{
  "type": "wait.created",
  "thread_id": "order-4471",
  "question_id": "a1b2c3d4e5f60718",
  "question": { "function": "issue_refund", "args": { "order_id": "order-4471", "amount": 41000 } },
  "allowed_actions": ["approve", "reject"],
  "expires_at": "2026-09-14T09:00:00Z",
  "default": { "action": "reject" },
  "source": { "function": "issue_refund" },
  "reply_with": { "thread_id": "order-4471", "question_id": "a1b2c3d4e5f60718", "answer": null }
}
```

`reply_with` is a filled-in stub: copy it, set `answer`, send it back. Whatever goes in
`answer` is what the `@hitl` function receives — verbatim.

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

A raise inside `deliver()` becomes a log line; the run completes.

Shipped: `WebhookAnnounce` (signed JSON POST, stdlib), `LogAnnounce`, `InMemoryAnnounce`,
and in `agent-wait-aws`: `SnsAnnounce`, `SqsAnnounce`, `EventBridgeAnnounce`,
`DynamoDbAnnounce` (the question as a row an approvals UI can query). Each one, its
constructor and where the question lands: [Announcers](https://skamalj.github.io/agent-wait/announcers/).
Your own: [Writing an announcer](https://skamalj.github.io/agent-wait/writing-an-announcer/).

## What the library does *not* do

- **Receive answers.** No endpoint, no validation, no ledger reads. The `if` above is yours.
- **Enforce anything.** `expires_at`, `default`, `answer_ttl`, `allowed_actions` are
  published so the consumer has the asker's intent. Acting on them is the consumer's.
- **Store anything.** LangGraph's checkpoint is the only state in interrupt mode; in
  async mode there is none unless you keep one.

## Two things to know

**Not compatible with LangChain's `HumanInTheLoopMiddleware`.** It interrupts *before* a
tool is called, in its own shape; `@hitl` interrupts inside the call. Both on one tool
means two interrupts for one approval. Use one or the other.

**One `interrupt()` per node** on langgraph 1.2.x. Two interrupting tools dispatched by
one `ToolNode` get the **same interrupt id**
([#6626](https://github.com/langchain-ai/langgraph/issues/6626)), and only one surfaces
per run ([#6624](https://github.com/langchain-ai/langgraph/issues/6624)). Give each
interrupting tool its own node. Pinned by a test that fails if LangGraph changes it.

## Layout

```
packages/agent-wait        core: Question, WaitPolicy, the envelope, publish(), announcers. No deps.
packages/langgraph-wait    @hitl and publish_interrupts(). The only LangGraph import.
packages/agent-wait-aws    four announce adapters, and a CDK stack for the example.
examples/refund_agent      a graph and a host, written for Lambda behind SQS.
docs/                      message contract, architecture, announcer guide, consumer guide.
```

MIT. Issues and PRs at [github.com/skamalj/agent-wait](https://github.com/skamalj/agent-wait).
