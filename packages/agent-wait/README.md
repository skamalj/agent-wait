# agent-wait

The core. No LangGraph, no AWS, no boto3. pyright strict.

```python
from agent_wait import EntryPoint, WaitPolicy, WaitPublisher

agent = WaitPublisher(adapter, announce=[...], reply_to=EntryPoint("sqs", url))

agent.invoke(value, thread_id)      # run the graph, publish what it parked on
agent.pending(thread_id)            # what is it parked on right now?
agent.republish(thread_id)          # announce it again -- repairs a lost announce
```

`WaitPolicy` is what a graph author declares at the interrupt site: `timeout`, `default`,
`allowed_actions`, `tags`, `correlation`. Every field is advisory — this library publishes
them and enforces none of them.

Two protocols:

* **`AnnounceAdapter`** — `announce(envelope, transition)` and `supports(transition)`.
  The only thing you are expected to implement. Must not raise.
* **`FrameworkAdapter`** — `config_for()`, `invoke()`, `pending()`. Implemented once, in
  `langgraph-wait`; the protocol is what keeps LangGraph out of this package.

See the repository README, `docs/message-formats.md`, and `docs/architecture.md`.
