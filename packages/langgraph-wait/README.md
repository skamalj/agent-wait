# langgraph-wait

The LangGraph side of [agent-wait](https://pypi.org/project/agent-wait/) — get a LangGraph
interrupt out of the process, and the answer back in.

```bash
pip install langgraph-wait          # pulls in agent-wait
```

```python
from langgraph_wait import ask, LangGraphAdapter, is_answer, resume_command
```

* **`ask(question, policy)`** — a thin wrapper over `interrupt()`. The node pauses exactly
  as LangGraph pauses; the policy rides along inside the interrupt value and comes back
  out in the published envelope. A plain `interrupt(value)` also works, with the default
  policy.
* **`LangGraphAdapter(graph)`** — three methods. `pending()` reads what a thread is parked
  on and filters LangGraph's `tasks[*].interrupts` over-report on `task.result`.
* **`is_answer(message)`** / **`resume_command(message)`** — pure functions for the host's
  router. `interrupt_id` present means resume; the `Command` is keyed by interrupt id so
  parallel interrupts resume independently.

Requires `langgraph >= 1.2`. One `interrupt()` per node — two interrupting tools in one
`ToolNode` share an id on 1.2.x (langgraph #6626), and this package documents rather
than hides that.

Full documentation: [skamalj.github.io/agent-wait](https://skamalj.github.io/agent-wait/).
