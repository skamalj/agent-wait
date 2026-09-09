# agent-wait · end-to-end simulation (LangGraph agent on Lambda)

Runs a real LangGraph graph (1.2.x) through the durable-wait design, with in-memory stand-ins
for the AWS services. Every stand-in mimics the one property the design relies on:

| Stand-in            | Real service          | Property relied on                                   |
|---------------------|-----------------------|------------------------------------------------------|
| `DynamoWaitStore`   | DynamoDB              | conditional writes (status CAS), unique idempotency key |
| `SqsFifo`           | SQS FIFO              | one in-flight message per MessageGroupId, visibility timeout, redelivery |
| `EventBridgeScheduler` | EventBridge Scheduler | one-time schedule named by wait_id, deleted on cancel |
| `EventBridgeBus`    | EventBridge           | fan-out to subscribers (SNS/Slack/webhooks)          |
| `run_handler` etc.  | Lambda                | stateless invocations that can die mid-way           |

```
pip install langgraph
python simulate.py
```

Files:
- `agent_wait/core.py`            — the framework-agnostic core: Wait model, 5 ports, HMAC tokens, Waiter, sweeper
- `agent_wait/langgraph_adapter.py` — `ask()` + the LangGraph adapter (extract interrupts, build resume, still_pending)
- `agent_wait/aws_sim.py`         — the AWS stand-ins
- `app/graph.py`                  — the customer's refund graph (knows nothing about AWS)
- `app/lambdas.py`                — three Lambda handlers: run (SQS), answer (API Gateway), on_timer (Scheduler)
- `simulate.py`                   — four scenarios; `RUN_OUTPUT.txt` is a captured run

Scenarios:
- A  crash after checkpoint but before register → redelivery → idempotent; double-click; stale reject; tampered token; resume on another Lambda; refund issued once
- B  no answer for 3 days → Scheduler fires → default reject → late click gets 409
- C  crash after resume applied but before SQS ack → redelivery → still_pending() guard → no second refund
- D  crash after DynamoDB create but before timer/notify → sweeper completes setup
