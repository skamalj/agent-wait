# langgraph-wait

The LangGraph side. The only package that imports LangGraph.

```python
from langgraph_wait import ask, LangGraphAdapter, is_answer, resume_command
```

* **`ask(question, policy)`** — a thin wrapper over `interrupt()`. The node pauses exactly
  as LangGraph pauses; the policy rides along inside the interrupt value under `__wait__`.
  A plain `interrupt(value)` also works, with the default policy.
* **`LangGraphAdapter(graph)`** — three methods. `pending()` filters on `task.result`,
  which is the workaround for langgraph #4796/#6792; see the module docstring and
  `docs/architecture.md`.
* **`is_answer(message)`** / **`resume_command(message)`** — pure functions for the host's
  router. `interrupt_id` present means resume; `Command(resume={id: answer})` is keyed so
  parallel interrupts resume independently.

Requires `langgraph >= 1.2`. `tests/test_spike_langgraph.py` records what that version
actually does, into `reports/langgraph-spike-observations.txt`, on every run.
