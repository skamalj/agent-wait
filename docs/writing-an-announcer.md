# Writing an announcer

An announcer puts the envelope somewhere a person — or the system that will nudge a
person — can find it. It is the only thing agent-wait expects you to implement, and it
is one method.

## The minimum

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

Pass it wherever announcers go — `publish_interrupts(result, thread_id, [RedisAnnounce(r)])`
or `@hitl(policy, mode="async", announce=[RedisAnnounce(r)])` — and you are done.

`BaseAnnounce` gives you three things so you do not have to think about them:

| You get | What it means |
|---|---|
| **A raise in `deliver()` becomes a log line** | The publish completes; the run completes. This is the one rule in the contract and the base class enforces it rather than trusting you to remember. |
| **`only=`** | A caller can restrict which transitions your adapter sees. Today there is one (`created`), so this is future-proofing at no cost. |
| **`self._log`** | A logger named `agent_wait.announce.<name>`, already set up. |

`name` is a class attribute. It appears in log lines; keep it short and lowercase.

## What you are given

`deliver(envelope, transition)` receives a `WaitEnvelope` and `"created"`. Everything
about the question is on the envelope — `envelope.to_json()` / `.to_dict()` serialise the
whole documented contract — and the field you will reach for first is
**`envelope.dedupe_key`**.

## The one thing to get right: use `dedupe_key`

The same question **will** reach your adapter more than once. A start message redelivered
by the queue re-runs the thread and LangGraph hands back the same `Interrupt.id`; an
async tool called twice with the same arguments derives the same id. Each publish mints a
fresh `event_id`; `dedupe_key` (`"wait.created:<question_id>"`) does not change.

So whatever your backend offers as a natural key — a Redis key, an item key, an SQS FIFO
`MessageDeduplicationId`, an idempotency header — put `dedupe_key` there. The shipped
adapters all do:

| Adapter | Where `dedupe_key` goes |
|---|---|
| `SqsAnnounce` | `MessageDeduplicationId` on a FIFO queue |
| `DynamoDbAnnounce` | the item key, so a republish overwrites its own row |
| `WebhookAnnounce` | the `X-Agent-Wait-Dedupe-Key` header, for the receiver |

If your backend has no natural key, say so in the docstring so the consumer knows
deduplication is on them.

## Patterns from the shipped adapters

**A row, not an event** — `DynamoDbAnnounce`. Writes `status=open` and every envelope
field, and never touches the row again. What the host does with the row afterwards is
the host's. Worth copying for Postgres, Mongo, or any table an approvals UI already reads.

**Routing metadata** — `SnsAnnounce` turns the policy's `tags` into message attributes so
subscription filter policies can route `approver_group=finance` one way and everything
else another. If your transport has headers or attributes, `envelope.tags` goes in them.

**A backend that lies about failure** — `EventBridgeAnnounce` checks `FailedEntryCount`,
because `PutEvents` reports per-entry failures inside an HTTP 200. Raise on it; the base
turns the raise into a log line, which is exactly right.

**A short timeout** — `WebhookAnnounce` defaults to five seconds. `deliver()` runs inside
the agent's own invocation; a slow backend must not turn a two-second run into thirty.

## Do not retry

No shipped adapter retries and yours should not either. A failed delivery is logged and
the run completes; the question is published again — same `dedupe_key` — the next time
the message is redelivered. That is the retry, and it is idempotent by construction.

## Testing yours

```python
from agent_wait import Question, WaitPolicy, publish


def test_my_adapter(my_backend):
    q = Question("q-1", {"kind": "approval"}, WaitPolicy())

    publish([q], "thread-1", [MyAnnounce(my_backend)])
    assert my_backend.received[-1] == "wait.created:q-1"

    my_backend.fail_next()
    publish([q], "thread-1", [MyAnnounce(my_backend)])  # must not raise
```

`publish()` is the core call; it takes `Question`s and needs no graph, so an adapter test
needs no LangGraph.

## Ideas that are one method away

- **Postgres / MySQL** — `INSERT … ON CONFLICT (dedupe_key) DO UPDATE`
- **Slack** — `chat.postMessage` with buttons whose payload is `envelope.reply_with`
- **Email** — the question with a link
- **A file** — one JSON line per envelope; the cheapest audit log
- **Kafka / Pub/Sub / RabbitMQ** — the message key is `dedupe_key`
- **Datadog / OpenTelemetry** — an event per question, so open approvals show on a dashboard

If you write one others could use, the shipped adapters are the reference and a PR is
welcome.
