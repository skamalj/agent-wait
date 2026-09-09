# agent-wait

Durable waits for agent frameworks. The framework-agnostic core: no cloud SDK, no
framework import, no I/O it did not ask for.

```python
from agent_wait import WaitRuntime, EntryPoint, InMemoryWaitStore, TokenCodec, LogAnnounce

runtime = WaitRuntime(
    adapter=my_framework_adapter,
    store=InMemoryWaitStore(),
    tokens=TokenCodec(key_provider),
    announce=[LogAnnounce()],
    entry_point=EntryPoint("sqs", queue_url),
)

outcome = runtime.dispatch(payload)  # Start | Resume | Ignore
...  # invoke your graph
result = runtime.register(out, config, thread_id)
```

Two calls, either side of your existing invoke. See `docs/integrating-a-consumer.md`
and `docs/message-formats.md` at the repository root.
