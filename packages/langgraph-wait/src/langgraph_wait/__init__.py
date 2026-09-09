"""langgraph-wait: durable waits for LangGraph.

    from langgraph_wait import ask, LangGraphAdapter

Two pieces. `ask()` goes in your node, in place of `interrupt()`. `LangGraphAdapter`
goes into the `WaitRuntime`, and gives the core the four things it needs to park an
interrupt and resume it days later on another machine.
"""

from .adapter import LangGraphAdapter
from .ask import QUESTION_KEY, WAIT_KEY, ask, unwrap

__version__ = "0.1.0"

__all__ = ["QUESTION_KEY", "WAIT_KEY", "LangGraphAdapter", "ask", "unwrap"]
