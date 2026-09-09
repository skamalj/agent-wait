"""langgraph-wait: ask() at the interrupt site + the adapter the core calls."""
from __future__ import annotations

from dataclasses import asdict
from typing import Any

from langgraph.types import Command, interrupt

from .core import PendingInterrupt, Wait, WaitPolicy


def ask(question: Any, policy: WaitPolicy | None = None) -> Any:
    """Thin wrapper over LangGraph's interrupt(). The policy rides inside the value
    because that is the only channel LangGraph provides; on resume, the raw resume
    value is returned to the caller."""
    policy = policy or WaitPolicy()
    return interrupt({"question": question, "__wait__": asdict(policy)})


class LangGraphAdapter:
    name = "langgraph"

    def __init__(self, graph):
        self.graph = graph

    def _config(self, thread_id: str):
        return {"configurable": {"thread_id": thread_id}}

    def extract(self, result: Any, config: Any) -> list[PendingInterrupt]:
        out = []
        for intr in result.get("__interrupt__", []) if isinstance(result, dict) else []:
            state = self.graph.get_state(config)
            ckpt = state.config["configurable"]["checkpoint_id"]
            val = intr.value
            if isinstance(val, dict) and "__wait__" in val:
                policy = WaitPolicy(**val["__wait__"]); question = val["question"]
            else:
                policy, question = WaitPolicy(), val
            out.append(PendingInterrupt(intr.id, ckpt, question, policy))
        return out

    def build_resume(self, wait: Wait, payload: Any) -> dict:
        # JSON-shaped so it can ride a queue; dict-keyed so parallel interrupts resume independently.
        # The run handler turns it into Command(resume=resume_map).
        return {"resume_map": {wait.interrupt_id: payload}}

    def still_pending(self, wait: Wait) -> bool:
        state = self.graph.get_state(self._config(wait.thread_id))
        for task in state.tasks:
            for intr in getattr(task, "interrupts", ()):
                if intr.id == wait.interrupt_id:
                    return True
        return False
