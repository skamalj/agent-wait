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

Two calls, either side of your existing invoke.

`WaitRuntime` also carries three **support methods** — `sweep()`, `cancel()` and
`envelope_for()` — for the repair pass and operational tooling. They are not part of the
quickstart; see "API reference — runtime operations" in `docs/architecture.md`.

See also `docs/integrating-a-consumer.md` and `docs/message-formats.md` at the repository
root.
