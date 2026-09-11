# Announcers

An announcer puts the envelope somewhere a person — or the system that will nudge a
person — can find it. Pass any number of them; each is called for every question, and a
failure in one is logged and contained without affecting the others or the run.

```python
from agent_wait import WebhookAnnounce
from agent_wait_aws import DynamoDbAnnounce, SnsAnnounce
from langgraph_wait import hitl, publish_interrupts

# after a run
publish_interrupts(result, thread_id, announce=[SnsAnnounce(topic_arn), DynamoDbAnnounce(table)])


# or on an async function
@hitl(FINANCE, mode="async", announce=[WebhookAnnounce(url, secret=SECRET)])
def issue_refund(order_id: str, amount: int) -> str: ...
```

Every shipped announcer puts `envelope.dedupe_key` — `"wait.created:<question_id>"`,
stable across republishes — wherever its backend has a natural key, so a redelivered
question is one question, not two. `event_id` is fresh per publish and is never the key.

## In `agent-wait` (no dependencies)

### `WebhookAnnounce`

```
WebhookAnnounce(url, *, secret=None, headers=None, timeout=5.0)
```

One JSON `POST` per question, body = the envelope. Three headers:

| header | value |
|---|---|
| `X-Agent-Wait-Event` | `wait.created` |
| `X-Agent-Wait-Dedupe-Key` | `wait.created:<question_id>` |
| `X-Agent-Wait-Signature` | `sha256=<hex>` — only when `secret` is given |

The signature is `HMAC-SHA256(secret, raw body)`, the shape GitHub and Stripe use.
`agent_wait.verify_signature(secret, body, header)` is the receiver's half, constant-time.
It answers *did this come from the agent* and nothing else.

Any non-2xx is a log line. No retry; the timeout is short because the POST runs inside
the agent's own invocation and a slow receiver must not stretch it.

### `LogAnnounce`

```
LogAnnounce(logger=None, *, level=logging.INFO)
```

One structured line per question on `agent_wait.announce`: event, ids, `allowed_actions`,
`expires_at`, `tags`. **The question itself is never logged at INFO** — it may hold
anything the agent was working on. At DEBUG it is included, because by then someone has
opted in.

### `InMemoryAnnounce`

```
InMemoryAnnounce()
```

Appends `(transition, envelope)` to `.events`. For tests. `.of("created")`, `.last()`,
`.clear()`.

## In `agent-wait-aws` (boto3)

### `SnsAnnounce`

```
SnsAnnounce(topic_arn, *, client=None, region_name=None)
```

`Publish` with `Subject = wait.created` and the envelope as the message. The policy's
`tags` become **message attributes**, alongside `transition` and `thread_id`, so a
subscription filter policy can route `approver_group = finance` to one queue and
everything else to another without the agent knowing either exists. Empty tag values are
skipped — SNS rejects them.

### `SqsAnnounce`

```
SqsAnnounce(queue_url, *, group_id=None, client=None, region_name=None)
```

`SendMessage` with the envelope as the body. On a FIFO queue (`.fifo` suffix):
`MessageGroupId` is the thread id (one in-flight question per conversation; override
with `group_id=lambda envelope: ...`) and `MessageDeduplicationId` is `dedupe_key`, so a
republish inside SQS's five-minute window is swallowed by the queue itself.

### `EventBridgeAnnounce`

```
EventBridgeAnnounce(bus_name, *, source="agent-wait", client=None, region_name=None)
```

`PutEvents` with `DetailType = wait.created` and the envelope as `Detail`. Rules on the
bus can fan one question out to Slack, a ticketing system and an audit log. `PutEvents`
reports per-entry failures inside an HTTP 200; this adapter checks `FailedEntryCount`
and logs them, because a delivery that silently failed is the one failure nothing else
can catch.

### `DynamoDbAnnounce`

```
DynamoDbAnnounce(table_name, *, table=None, region_name=None, ttl_seconds=None)
```

The question **is** the row. An approvals UI can `Query` the table for open questions
without building a projection off a topic.

```
pk = "THREAD#<thread_id>"      sk = "WAIT#<question_id>"      status = "open"
```

plus every envelope field. `expires_at` is stored as the string `never` when the policy
has no timeout, so the GSI range key always exists and a range condition never returns
those rows. `ttl_seconds` sets a DynamoDB TTL on the row if you want the table to expire
on its own.

The GSI `by_status` (`status`, `expires_at`) serves both queries a host or UI wants:

```python
from boto3.dynamodb.conditions import Key

table.query(IndexName="by_status", KeyConditionExpression=Key("status").eq("open"))
table.query(
    IndexName="by_status", KeyConditionExpression=Key("status").eq("open") & Key("expires_at").lt(now_iso)
)
```

The adapter writes the row and **never touches it again**. Marking rows answered,
sweeping overdue ones, or ignoring them is the host's — the library is not on the receive
path, and this adapter is no exception. The CDK stack in the package creates the table
and the index.

## Writing your own

One method. See [Writing an announcer](writing-an-announcer.md).
