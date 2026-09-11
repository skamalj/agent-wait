# agent-wait-aws

AWS announce adapters for [agent-wait](https://pypi.org/project/agent-wait/).

```bash
pip install agent-wait-aws          # pulls in agent-wait and boto3
```

```python
from agent_wait_aws import DynamoDbAnnounce, EventBridgeAnnounce, SnsAnnounce, SqsAnnounce
```

| Adapter | Where the question lands |
|---|---|
| `SnsAnnounce(topic_arn)` | A topic. Policy `tags` become message attributes, so subscription filter policies can route on them. |
| `SqsAnnounce(queue_url)` | A queue. On FIFO: `MessageGroupId = thread_id`, and `MessageDeduplicationId` is the stable dedupe key, so a republished question is swallowed. |
| `EventBridgeAnnounce(bus)` | A bus, with the transition as detail-type. Notices partial failures, which `PutEvents` reports inside an HTTP 200. |
| `DynamoDbAnnounce(table)` | A row. `created` writes it `open`, `resumed` marks it `closed`; a GSI on `status` gives an approvals UI its query with no broker involved. |

Every one is a `BaseAnnounce` subclass with a single `deliver()` method, so a failure is a
log line rather than a failed run. They are also the reference for writing your own:
`sqs.py` is twenty lines.

The package also carries a CDK stack for the example agent — a FIFO queue, a topic, one
Lambda, the checkpointer's table and an approvals table.

Full documentation: [skamalj.github.io/agent-wait](https://skamalj.github.io/agent-wait/).
