"""`LangGraphAdapter` -- the five methods the core needs, and nothing else.

The core never imports LangGraph. Everything framework-specific lives here, which is
also where the framework's rough edges get handled.

## What was verified empirically, against langgraph 1.2.11

REQUIREMENTS section 9.2 asks for a spike rather than an assumption, because the
correctness of the whole design rests on two properties. `tests/test_spike_langgraph.py`
asserts all of this, so we find out if a future version changes it.

1. **`Interrupt.id` is stable** across `invoke(None, config)` re-entry and across
   resume-from-checkpoint. Confirmed. This is what lets `sha256(thread|interrupt|
   checkpoint)` be an idempotency key that survives a crash and a redelivery.
2. **`get_state(config).config["configurable"]["checkpoint_id"]` is stable** for a
   parked thread across repeated reads. Confirmed. Same reason.
3. **`get_state().tasks[*].interrupts` over-reports.** This is the bug that matters
   (langgraph #4796 / #6792). With two parallel interrupts, resume one and the finished
   task *still* lists its interrupt id -- so the obvious `still_pending()` returns True
   for an interrupt the graph has already moved past.

   The discriminator is `task.result`: it holds the finished task's return value and is
   `None` while the task is genuinely parked. `still_pending()` below checks both, and
   the spike test pins the behaviour so a LangGraph fix shows up as a failing test
   rather than as silence.

   Replaying a resume for an already-resumed parallel interrupt does *not* repeat the
   node's side effect -- LangGraph is idempotent there -- so this is a second line of
   defence rather than the only one. The first is the status compare-and-set in
   `dispatch()`, which rejects the redelivery before the graph is ever invoked.
4. **Interrupts raised inside a subgraph** surface on the parent's `__interrupt__` with
   a stable id, and the parent's `get_state()` reports them against the subgraph node's
   task. `subgraphs=True` was not needed. Resuming by id works through the parent.
5. **"Applied" is not "persisted"** (section 18.5). `dispatch()` marks a start message
   applied before the graph consumes it, so a first run that dies before LangGraph writes
   a checkpoint leaves a message recorded as applied against a thread with nothing to
   resume from -- and `invoke(None, config)` on it raises `EmptyInputError`.
   `has_checkpoint()` below is what separates that from an ordinary redelivery.
"""

from __future__ import annotations

from typing import Any

from agent_wait import PendingInterrupt, Wait
from langgraph.types import Command

from .ask import unwrap


class LangGraphAdapter:
    """Wraps a compiled graph. Requires langgraph >= 1.2."""

    name = "langgraph"

    def __init__(self, graph: Any) -> None:
        self.graph = graph

    # ------------------------------------------------------------------ config
    def config_for(self, thread_id: str) -> dict[str, Any]:
        return {"configurable": {"thread_id": thread_id}}

    # ------------------------------------------------------------------ extract
    def extract(self, result: Any, config: Any) -> list[PendingInterrupt]:
        """Interrupts raised by the run that just returned.

        `result["__interrupt__"]` is the authoritative list -- unlike `get_state().tasks`
        it does not over-report. The checkpoint id comes from `get_state()`, read once:
        every interrupt in one superstep shares it.
        """
        raw = result.get("__interrupt__") if isinstance(result, dict) else None
        if not raw:
            return []

        checkpoint_id = self.checkpoint_id(config)
        pending: list[PendingInterrupt] = []
        for interrupt_obj in raw:
            question, policy = unwrap(getattr(interrupt_obj, "value", None))
            pending.append(
                PendingInterrupt(
                    interrupt_id=str(interrupt_obj.id),
                    checkpoint_id=checkpoint_id,
                    question=question,
                    policy=policy,
                )
            )
        return pending

    def checkpoint_id(self, config: Any) -> str:
        state = self.graph.get_state(config)
        configurable = (state.config or {}).get("configurable", {})
        return str(configurable.get("checkpoint_id") or "")

    def has_checkpoint(self, thread_id: str) -> bool:
        """Section 18.5. Has the checkpointer kept anything for this thread?

        Both signals are checked because they fail in different directions. A thread that
        has never run returns a snapshot with no `checkpoint_id` *and* empty `values`; a
        thread whose first superstep interrupted before writing any channel has a
        `checkpoint_id` but may still have empty `values`. Either one on its own would
        misclassify a real case.
        """
        state = self.graph.get_state(self.config_for(thread_id))
        configurable = (getattr(state, "config", None) or {}).get("configurable", {})
        return bool(configurable.get("checkpoint_id")) or bool(getattr(state, "values", None))

    # ------------------------------------------------------------------ resume
    def build_resume(self, wait: Wait, resume_value: Any) -> Command:
        """Dict-keyed so parallel interrupts resume independently (section 9.2).

        `Command(resume=value)` -- the non-dict form -- would hand the same value to
        every parked interrupt on the thread. With two approvals outstanding that is one
        click approving both.
        """
        return Command(resume={wait.interrupt_id: resume_value})

    # ------------------------------------------------------------------ guard
    def still_pending(self, wait: Wait) -> bool:
        """Is the thread genuinely still parked on this interrupt?

        See the module docstring: `task.interrupts` alone over-reports after a partial
        parallel resume, so a task that has already produced a result does not count.
        """
        state = self.graph.get_state(self.config_for(wait.thread_id))
        for task in getattr(state, "tasks", ()) or ():
            for pending in getattr(task, "interrupts", ()) or ():
                if str(getattr(pending, "id", "")) == wait.interrupt_id:
                    return getattr(task, "result", None) is None
        return False
