# agent-wait

[![PyPI](https://img.shields.io/pypi/v/agent-wait.svg)](https://pypi.org/project/agent-wait/)
[![CI](https://github.com/skamalj/agent-wait/actions/workflows/ci.yml/badge.svg)](https://github.com/skamalj/agent-wait/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/skamalj/agent-wait/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-skamalj.github.io%2Fagent--wait-black.svg)](https://skamalj.github.io/agent-wait/)

**Make an agent wait for something outside the process — and get the answer back in.**

Human-in-the-loop is the common case: a refund over the limit, a contract clause, a
deploy. But the same shape covers a vendor callback, a payment processor, a KYC check, or
another agent — anything the run cannot continue without, and nothing inside the process
can supply.

LangGraph has `interrupt()` for this; Pydantic AI has `ApprovalRequired`. In both, the
run pauses and the question is handed to whatever called it — and that is where the
framework stops. There is no built-in way to tell anyone *else* that a question was asked,
and no built-in way for anyone else to answer it. The moment the question has to reach
Slack, an approvals dashboard, a ticket queue or another service, you are writing that
code yourself — whether your agent is a server that runs for a year or a Lambda that is
gone in seconds.

agent-wait is that code. It takes the interrupt and puts it somewhere people (or systems)
can see it — a topic, a queue, a webhook, a database row — with everything needed to answer
it in one envelope, and documents the shape of the answer so the return leg is one `if`.

```bash
pip install "agent-wait[langgraph]"        # @wait + publish_interrupts for LangGraph
pip install "agent-wait[pydantic-ai]"      # the same two names for Pydantic AI
pip install "agent-wait[langgraph,aws]"    # + SNS / SQS / EventBridge / DynamoDB announcers
```

The bare `agent-wait` is the framework-free core (policy, envelope, webhook and in-memory
announcers) and depends on nothing. Each extra pulls in exactly one framework or provider.

## The whole thing

**In the graph** — one decorator, on the function where the decision belongs:

```python
from agent_wait import WaitPolicy
from agent_wait.langgraph import wait
from langchain_core.tools import tool

FINANCE = WaitPolicy(
    timeout="P3D",
    default={"action": "reject"},
    allowed_actions=("approve", "reject"),
    tags={"approver_group": "finance"},
)


@tool
@wait(FINANCE)
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
| `correlation` | Your reference for the thing being waited on — a vendor job id, a ticket. |

A node that needs the answer itself declares a `decision` parameter and always runs,
approve or not, with the answer in it:

```python
@wait(FINANCE)
def review(state, decision=None):
    return {"decision": decision}  # verbatim: {"action": "approve", "note": "ok"}
```

The parameter name is yours — `@wait(FINANCE, decision="verdict")` looks for `verdict`.

**After the run** — one call:

```python
from agent_wait.aws import SnsAnnounce
from agent_wait.langgraph import publish_interrupts

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

## The same two names on Pydantic AI

```python
from agent_wait.pydantic_ai import publish_interrupts, wait
from pydantic_ai import Agent, DeferredToolRequests, DeferredToolResults, RunContext, ToolApproved, ToolDenied

agent = Agent("openai:gpt-5.4", output_type=[str, DeferredToolRequests])


@agent.tool
@wait(FINANCE)
def issue_refund(ctx: RunContext[None], order_id: str, amount: int) -> str:
    payments.refund(order_id, amount)
    return "refunded"


result = agent.run_sync("refund order 4471", message_history=history)
publish_interrupts(result, thread_id, announce=[SnsAnnounce(topic_arn)])
store(thread_id, result.all_messages())  # Pydantic AI has no checkpointer; the history is yours
```

The run ends with `DeferredToolRequests` in `result.output`; `question_id` is the
`tool_call_id`. The answer goes back through the framework's own resume, with the answer
itself riding in `metadata` so the `@wait` rules apply unchanged:

```python
approved = answer.get("action") == "approve"
result = agent.run_sync(
    None,
    message_history=load(thread_id),
    deferred_tool_results=DeferredToolResults(
        approvals={question_id: ToolApproved() if approved else ToolDenied(str(answer))},
        metadata={question_id: answer},
    ),
)
store(thread_id, result.all_messages())  # before acknowledging the answer message
```

Two rules the framework leaves to you: name the `RunContext` parameter `ctx`, and store
the post-run history *before* you acknowledge an answer — a duplicate answer is refused
only if the history you pass already holds the tool's return. `requires_approval=True`
tools and `CallDeferred` calls (a job handed to an external system) are published too,
with the default policy.

## Async mode — the thread does not park

```python
from agent_wait.aws import DynamoDbAnnounce, SnsAnnounce


@tool
@wait(FINANCE, mode="async", announce=[SnsAnnounce(topic_arn), DynamoDbAnnounce(table)])
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
`answer` is what the `@wait` function receives — verbatim.

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

Shipped in the core: `WebhookAnnounce` (signed JSON POST, stdlib), `LogAnnounce`,
`InMemoryAnnounce`. With `[aws]`: `SnsAnnounce`, `SqsAnnounce`, `EventBridgeAnnounce`,
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
tool is called, in its own shape; `@wait` interrupts inside the call. Both on one tool
means two interrupts for one approval. Use one or the other.

**One `interrupt()` per node** on langgraph 1.2.x. Two interrupting tools dispatched by
one `ToolNode` get the **same interrupt id**
([#6626](https://github.com/langchain-ai/langgraph/issues/6626)), and only one surfaces
per run ([#6624](https://github.com/langchain-ai/langgraph/issues/6624)). Give each
interrupting tool its own node. Pinned by a test that fails if LangGraph changes it.

## Layout

One distribution, one import root. Frameworks and providers are subpackages behind extras:

```
agent_wait              core: WaitPolicy, Question, the envelope, publish(), BaseAnnounce,
                        Webhook/Log/InMemory announcers, and the Framework interface. No deps.
agent_wait.langgraph    [langgraph]    @wait and publish_interrupts() bound to LangGraph.
agent_wait.pydantic_ai  [pydantic-ai]  the same two names bound to Pydantic AI's deferred tools.
agent_wait.aws          [aws]          SnsAnnounce, SqsAnnounce, EventBridgeAnnounce, DynamoDbAnnounce.
examples/refund_agent   a graph, a host written for Lambda behind SQS, and its CDK stack.
docs/                   message contract, architecture, announcer guide, consumer guide.
```

`@wait` and `publish_interrupts` are written once against a three-method `Framework`
interface; `agent_wait.langgraph` and `agent_wait.pydantic_ai` are implementors of it,
about fifty lines each. Announcers are the same split: `BaseAnnounce` is the interface,
each provider subpackage the implementors. See
[Architecture](https://skamalj.github.io/agent-wait/architecture/).

MIT. Issues and PRs at [github.com/skamalj/agent-wait](https://github.com/skamalj/agent-wait).
