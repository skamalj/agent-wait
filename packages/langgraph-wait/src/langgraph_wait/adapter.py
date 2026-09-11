"""`LangGraphAdapter` -- three methods, and the one LangGraph bug that matters.

The core never imports LangGraph. Everything framework-specific lives here, which is also
where the framework's rough edges get handled.

## `get_state().tasks[*].interrupts` over-reports

This is the finding the whole adapter is shaped around (langgraph #4796 / #6792,
reproduced against 1.2.11 in `tests/test_spike_langgraph.py`). With two parallel
interrupts, resume one, and the *finished* task still lists its interrupt id. Anyone
building "what is this thread waiting on?" from `tasks[*].interrupts` alone gets an
interrupt the graph has already moved past -- and the failure mode is republishing an
approval button for a node that already ran, or resuming it a second time.

The discriminator is `task.result`: it holds the finished task's return value and is
`None` while the task is genuinely parked. `pending()` filters on it, and the spike test
pins the behaviour so a future LangGraph fix shows up as a failing test rather than as
silence.

## What else was verified empirically, against langgraph 1.2.11

* **`Interrupt.id` is stable** across `invoke(None, config)` re-entry and across
  resume-from-checkpoint. This is what makes `dedupe_key` work: a republished envelope
  carries the same id, so consumers can discard it.
* **Interrupts raised inside a subgraph** surface on the parent's state against the
  subgraph node's task, with a stable id. `subgraphs=True` was not needed, and resuming
  by id works through the parent.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from agent_wait import PendingInterrupt

from .ask import unwrap


class LangGraphAdapter:
    """Wraps a compiled graph. Requires langgraph >= 1.2."""

    name = "langgraph"

    def __init__(self, graph: Any) -> None:
        self.graph = graph

    def config_for(self, thread_id: str) -> dict[str, Any]:
        return {"configurable": {"thread_id": thread_id}}

    def invoke(self, value: Any, config: Any) -> Any:
        """Run the graph. `value` is the caller's -- fresh input, or a `Command`."""
        return self.graph.invoke(value, config)

    def pending(self, thread_id: str) -> list[PendingInterrupt]:
        """What this thread is parked on right now. `[]` if it has never run."""
        state = self.graph.get_state(self.config_for(thread_id))
        asked_at = _epoch(getattr(state, "created_at", None))

        out: list[PendingInterrupt] = []
        for task in getattr(state, "tasks", ()) or ():
            # See the module docstring. A task that has produced a result is finished,
            # whatever its `interrupts` list still claims.
            if getattr(task, "result", None) is not None:
                continue
            for raw in getattr(task, "interrupts", ()) or ():
                question, policy = unwrap(getattr(raw, "value", None))
                out.append(
                    PendingInterrupt(
                        interrupt_id=str(raw.id),
                        question=question,
                        policy=policy,
                        asked_at=asked_at,
                    )
                )
        return out


def _epoch(created_at: Any) -> float | None:
    """LangGraph stamps checkpoints with an ISO-8601 string. Anchor `expires_at` to it
    so a republished question keeps its original deadline."""
    if not isinstance(created_at, str):
        return None
    try:
        return datetime.fromisoformat(created_at).timestamp()
    except ValueError:
        return None
