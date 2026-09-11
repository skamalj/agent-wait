# Migrating

## From 0.2 to 0.3

0.2 wrapped the graph in a `WaitPublisher` that ran it, read `get_state()`, and offered
`pending()`, `republish()`, `is_answer()` and `resume_command()`. 0.3 removes the wrapper
and everything that read state: the library announces the interrupt and stops.

| 0.2 | 0.3 |
|---|---|
| `WaitPublisher(LangGraphAdapter(graph), announce=…)` | gone |
| `agent.invoke(value, thread_id)` | `graph.invoke(value, config)` — yours — then `publish_interrupts(result, thread_id, announce)` |
| `agent.pending(thread_id)` | gone — LangGraph ignores a duplicate resume, so there is nothing to check |
| `agent.republish(thread_id)` | gone — a redelivered start re-runs the thread and republishes the same id |
| `is_answer(message)` / `resume_command(message)` | gone — `"question_id" in message` and `Command(resume={id: answer})`, written out |
| `PendingInterrupt` | `Question` |
| envelope `interrupt_id` | `question_id` |
| envelope `wait.resumed` | gone — one transition, `wait.created` |
| — | `@hitl(policy, mode="interrupt" \| "async")` |
| — | `HumanInTheLoopMiddleware` batch shape understood |
| — | policy `answer_ttl`; envelope `source` |

A 0.2 handler:

```python
agent = WaitPublisher(LangGraphAdapter(graph), announce=[SnsAnnounce(topic)])


def route(message):
    thread_id = message["thread_id"]
    if is_answer(message):
        if not any(p.interrupt_id == message["interrupt_id"] for p in agent.pending(thread_id)):
            return
        return agent.invoke(resume_command(message), thread_id)
    if agent.pending(thread_id):
        return agent.republish(thread_id)
    return agent.invoke(message["input"], thread_id)
```

becomes:

```python
def route(message):
    thread_id = message["thread_id"]
    value = (
        Command(resume={message["question_id"]: message["answer"]})
        if "question_id" in message
        else message["input"]
    )
    result = graph.invoke(value, {"configurable": {"thread_id": thread_id}})
    publish_interrupts(result, thread_id, [SnsAnnounce(topic)])
```

The two guards are gone because they guarded nothing: a duplicate `Command(resume=…)`
is a no-op in LangGraph (verified on 1.2.11), and a redelivered start re-runs the thread
and gets the same `Interrupt.id` back, so the consumer sees the same `dedupe_key`.

Consumers: rename `interrupt_id` → `question_id` in the reply, and stop expecting
`wait.resumed`.

## From 0.1

0.1 (tagged, never published) owned the whole round trip — tokens, a wait store, leases,
a scheduler-driven timeout, an inbound `dispatch()`. All of it was removed in 0.2, and 0.3
removed what 0.2 had kept. If the answer-side guarantees mattered to you, they are now
the host's to provide; `message-formats.md` §4 says what LangGraph already gives you for
free and what it does not.
