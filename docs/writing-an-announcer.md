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

Pass it in and you are done:

```python
agent = WaitPublisher(LangGraphAdapter(graph), announce=[RedisAnnounce(redis)])
```

That is a complete, correct adapter. `BaseAnnounce` gives you three things so you do not
have to think about them:

| You get | What it means |
|---|---|
| **A raise in `deliver()` becomes a log line** | The graph that just parked stays parked; the run completes. This is the one rule in the contract and the base class enforces it rather than trusting you to remember. |
| **`only=`** | A caller can pass `RedisAnnounce(redis, only=("created",))` and your adapter never sees a `resumed`. You write no filter. |
| **`self._log`** | A logger named `agent_wait.announce.<name>`, already set up. Use it for anything worth saying; the base uses it when you raise. |

`name` is a class attribute. It appears in log lines and should be short and lowercase.

## What you are given

`deliver(envelope, transition)` receives:

- **`envelope`** — a `WaitEnvelope`. Everything about the question is on it, and
  `envelope.to_json()` / `envelope.to_dict()` serialise the whole documented contract.
  The field you will use most is **`envelope.dedupe_key`** — see below.
- **`transition`** — `"created"` or `"resumed"`. Also available as `envelope.type`
  (`"wait.created"` / `"wait.resumed"`) if you are already serialising the envelope.

`created` means *the graph is parked on this; here is everything needed to answer it*.
`resumed` means *the graph has moved past it; close the ticket, retract the button*. Most
adapters want both. A UI that only draws new questions can take `only=("created",)`.

## The one thing to get right: use `dedupe_key`

The same question **will** be delivered to your adapter more than once. A start message
redelivered by the queue, an operator calling `republish()`, a crash after the graph
parked but before the announce went out — all of them re-announce the same interrupt with
the same `interrupt_id`.

`envelope.dedupe_key` is `"{type}:{interrupt_id}"` and is **stable across republishes**.
`envelope.event_id` is a fresh ULID on every publish and is **not**.

So whatever your backing store or transport offers as a natural key — a Redis key, a
DynamoDB item key, an SQS FIFO `MessageDeduplicationId`, an idempotency header — put
`dedupe_key` there. The shipped adapters all do:

| Adapter | Where `dedupe_key` goes |
|---|---|
| `SqsAnnounce` | `MessageDeduplicationId` on a FIFO queue |
| `DynamoDbAnnounce` | the item key (`THREAD#…` / `WAIT#…`), so a republish overwrites its own row |
| `WebhookAnnounce` | the `X-Agent-Wait-Dedupe-Key` header, for the receiver to dedupe on |

If your backend has no natural key, say so in your adapter's docstring so the consumer
knows deduplication is on them.

## Patterns from the shipped adapters

**A row, not an event** — `DynamoDbAnnounce`. `created` writes `status=open`; `resumed`
sets `status=closed` rather than deleting, so there is something to look at when someone
asks why the button disappeared. The close is conditional on the row existing, so a
`resumed` for a question this table never saw leaves nothing behind. Worth copying for
Postgres, Mongo, or any table an approvals UI already reads.

**A queue with routing metadata** — `SnsAnnounce` turns the policy's `tags` into message
attributes so subscription filter policies can route `approver_group=finance` one way and
everything else another. If your transport has headers or attributes, `envelope.tags` is
what to put in them.

**A backend that lies about failure** — `EventBridgeAnnounce` checks `FailedEntryCount`,
because `PutEvents` reports per-entry failures inside an HTTP 200. Raise on it. The base
class turns the raise into a log line, which is exactly what you want: a delivery that
silently failed is the one failure mode nothing else can catch.

**A short timeout** — `WebhookAnnounce` defaults to five seconds. Your `deliver()` runs
inside the agent's own invocation, after the graph has done its work. A slow backend must
not turn a two-second run into a thirty-second one. Set a timeout; do not retry.

## Do not retry

There is no retry in any shipped adapter, and you should not add one. A failed delivery
is logged, the run completes, and the question is re-announced — with the same
`dedupe_key` — the next time the thread is re-invoked or `republish()` is called. That
is the retry, and it is idempotent by construction. A backoff loop inside `deliver()`
would only be a slower way to do the same thing, while holding the agent's invocation
open.

## Testing yours

`InMemoryAnnounce` and `FailingAnnounce` ship for this. The pattern in the repository's
own tests: wire your adapter into a `WaitPublisher` over a stub, drive one `created` and
one `resumed`, and assert on what your backend received. Then make the backend raise and
assert the run still completed.

```python
from agent_wait import PendingInterrupt, WaitPolicy, WaitPublisher

class StubAdapter:
    """A framework whose next invoke leaves the thread parked on whatever you say."""
    name = "stub"

    def __init__(self):
        self.parked = []
        self.after_invoke = []

    def config_for(self, thread_id):
        return {"configurable": {"thread_id": thread_id}}

    def invoke(self, value, config):
        self.parked = self.after_invoke          # the run changes what is parked
        return {}

    def pending(self, thread_id):
        return list(self.parked)


def test_my_adapter_delivers_and_survives_failure(my_backend):
    stub = StubAdapter()
    agent = WaitPublisher(stub, announce=[MyAnnounce(my_backend)])

    stub.after_invoke = [PendingInterrupt("int-1", {"kind": "approval"}, WaitPolicy())]
    agent.invoke(None, "t")                      # before: [] -> after: [int-1] -> created
    assert my_backend.received[-1] == "wait.created:int-1"

    my_backend.fail_next()
    stub.after_invoke = []
    agent.invoke(None, "t")                      # before: [int-1] -> after: [] -> resumed, and it raises
    # no exception reached us -- the failure is a log line, and the run completed
```

## Without subclassing

`BaseAnnounce` is a convenience, not a requirement. `WaitPublisher` accepts anything with
`name`, `supports(transition)` and `announce(envelope, transition)` — the `AnnounceAdapter`
protocol — and `CompositeAnnounce` contains a failure from a bare class exactly as it
does from a subclass. Use the protocol if you have a reason to; the base class exists so
that the common case is the correct case by default.

## Ideas that are one method away

Each is the `RedisAnnounce` above with a different `deliver()`:

- **Postgres / MySQL** — `INSERT … ON CONFLICT (dedupe_key) DO UPDATE`; a `status` column an approvals UI queries.
- **Slack** — `chat.postMessage` with buttons whose payload is `envelope.reply_with`; on `resumed`, `chat.update` to retract them.
- **Email** — `created` sends the question with a link; `resumed` is usually ignored (`only=("created",)`).
- **A file** — one JSON line per envelope; the cheapest possible audit log.
- **Kafka / Pub/Sub / RabbitMQ** — the message key is `dedupe_key`.
- **Datadog / OpenTelemetry** — an event per transition, so open questions show up on a dashboard.

If you write one that others could use, the shipped adapters are the reference for
shape and tests, and a PR is welcome.
