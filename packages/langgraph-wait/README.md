# langgraph-wait

The LangGraph adapter for [agent-wait](https://github.com/skamalj/agent-wait).

```python
from langgraph_wait import ask
from agent_wait import WaitPolicy


def review(state):
    decision = ask(
        {"kind": "refund_approval", "order_id": state["order_id"]},
        policy=WaitPolicy(timeout="P3D", default={"action": "reject"}, allowed_actions=("approve", "reject")),
    )
    return {"decision": decision}
```

`ask()` wraps `langgraph.types.interrupt()`; `LangGraphAdapter(graph)` gives the core
the four things it needs to park and resume that interrupt.
