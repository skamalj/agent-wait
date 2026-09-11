# langgraph-wait

The LangGraph side of [agent-wait](https://pypi.org/project/agent-wait/) — get a LangGraph
interrupt out of the process, and the answer back in.

```bash
pip install langgraph-wait          # pulls in agent-wait
```

```python
from langgraph_wait import hitl, publish_interrupts
```

* **`@hitl(policy)`** — on any function, under `@tool` if it is a tool. Calling it inside a
  run parks the graph on `{"function", "args"}`. `{"action": "approve"}` runs the body;
  anything else is returned in its place. A function with a `decision` parameter always
  runs and receives the answer. `mode="async"` makes the decorator the publisher: it
  announces and returns pending without parking.
* **`publish_interrupts(result, thread_id, announce)`** — after `graph.invoke()`. Reads
  `result["__interrupt__"]` and hands one envelope per question to the announcers.

Requires `langgraph >= 1.2`. Not compatible with LangChain's `HumanInTheLoopMiddleware`
on the same tool (two interrupts for one approval). One `interrupt()` per node on 1.2.x
(langgraph #6626).

Full documentation: [skamalj.github.io/agent-wait](https://skamalj.github.io/agent-wait/).
