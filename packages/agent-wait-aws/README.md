# agent-wait-aws

Announce adapters for AWS, and the CDK stack. Nothing else — since v0.2 the library keeps
no state, so there is no store, no key provider and no run handler here.

```python
from agent_wait_aws import DynamoDbAnnounce, EventBridgeAnnounce, SnsAnnounce, SqsAnnounce
```

| Adapter | Where the question lands |
|---|---|
| `SnsAnnounce(topic_arn)` | A topic. Policy `tags` become message attributes, so subscription filter policies can route on them. |
| `SqsAnnounce(queue_url)` | A queue. On FIFO: `MessageGroupId = thread_id`, and `MessageDeduplicationId = dedupe_key`, so a republish is swallowed. |
| `EventBridgeAnnounce(bus)` | A bus, with the transition as detail-type. Notices partial failures, which `PutEvents` reports inside an HTTP 200. |
| `DynamoDbAnnounce(table)` | A row. `created` writes it `open`, `resumed` marks it `closed`; a GSI on `status` gives an approvals UI its query with no broker involved. |

Every one of them is a `BaseAnnounce` subclass with a single `deliver()` method, so
they log and return on failure by construction. None of them raises. They are also
the reference for writing your own: `sqs.py` is twenty lines.

`cdk/` deploys the example: a FIFO queue, a topic, one Lambda, the checkpointer's table
and an approvals table. `scripts/deploy_and_e2e.ps1` builds the bundle, deploys, runs
`examples/refund_agent/demo_scenarios.py` and tears down.
