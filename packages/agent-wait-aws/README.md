# agent-wait-aws

AWS backends for [agent-wait](https://github.com/skamalj/agent-wait):

- `DynamoWaitStore` — single table, every transition a conditional write
- `SqsAnnounce`, `SnsAnnounce`, `EventBridgeAnnounce` — tell the world
- `SchedulerAnnounce` — the timeout, as a one-shot EventBridge schedule that delivers
  the answer to your own entry point
- `make_run_handler(graph, runtime)` — the Lambda handler, SQS FIFO, visibility
  heartbeat and partial batch failures included
- `cdk/` — a deployable stack

Nothing here is a second entry point. The world answers where your agent already listens.
