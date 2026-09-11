# langgraph-wait

The LangGraph side of [agent-wait](https://pypi.org/project/agent-wait/) — get a LangGraph
interrupt out of the process, and the answer back in.

```bash
pip install langgraph-wait          # pulls in agent-wait
```

```python
from langgraph_wait import ask, hitl, publish_interrupts
```

* **`ask(question, policy)`** — a thin wrapper over `interrupt()`; the policy rides along
  inside the interrupt value. A plain `interrupt(value)` also works, with the default policy.
* **`@hitl(policy)`** — on a tool function, under `@tool`. Calls `ask()` with `{tool, args}`
  before the tool runs. `mode="async"` makes the decorator the publisher: it announces and
  returns pending without parking the thread.
* **`publish_interrupts(result, thread_id, announce)`** — after `graph.invoke()`. Reads
  `result["__interrupt__"]` and hands one envelope per question to the announcers.
  Understands `ask()`, bare `interrupt()`, and `HumanInTheLoopMiddleware` batches.

Requires `langgraph >= 1.2`. One `interrupt()` per node — two interrupting tools in one
`ToolNode` share an id on 1.2.x (langgraph #6626); use the middleware, which batches.

Full documentation: [skamalj.github.io/agent-wait](https://skamalj.github.io/agent-wait/).
