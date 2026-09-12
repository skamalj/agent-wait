"""LangGraph implementor of `agent_wait.framework.Framework`, and the bound names.

    from agent_wait.langgraph import wait, publish_interrupts

Install with `pip install agent-wait[langgraph]`.

## What LangGraph provides, and what we use

- **Park:** `langgraph.types.interrupt(value)` raises on first execution and returns the
  resume value when the node re-runs. The value is stored in the checkpoint verbatim.
- **Read back:** `graph.invoke()` returns the run's output with `__interrupt__`: a sequence
  of `Interrupt` objects, each with `.id` and `.value` and nothing else (`ns`, `when`,
  `resumable` were removed in langgraph 0.6). `stream()` yields the same shape as a chunk.
- **Thread:** `config["configurable"]["thread_id"]`, read via `langgraph.config.get_config()`.
- **Resume** (the host's call, not ours):
  `graph.invoke(Command(resume={question_id: answer}), config)`. A resume for a question
  the thread has already moved past runs nothing -- verified on 1.2.11.

## Two things to know

- **One `interrupt()` per node** on 1.2.x. Two waiting tools dispatched by one `ToolNode`
  get the same `Interrupt.id` (langgraph #6626) and only one surfaces per run (#6624).
  Give each waiting tool its own node.
- **Not compatible with LangChain's `HumanInTheLoopMiddleware`** on the same tool: it
  interrupts before the tool, `@wait` inside it -- two interrupts for one approval.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

try:
    from langgraph.config import get_config
    from langgraph.types import interrupt
except ImportError as _err:  # pragma: no cover - exercised only without the extra
    raise ImportError("agent_wait.langgraph needs LangGraph: pip install 'agent-wait[langgraph]'") from _err

from ..framework import Framework
from ..wait import make_publish_interrupts, make_wait

INTERRUPT_KEY = "__interrupt__"


class LangGraphFramework(Framework):
    name = "langgraph"

    def interrupt(self, value: Mapping[str, Any], call_args: Mapping[str, Any]) -> Any:
        return interrupt(dict(value))

    def interrupts_in(self, result: Any) -> list[tuple[str, Any]]:
        if not isinstance(result, Mapping):
            return []
        items: list[Any] = list(cast(Mapping[str, Any], result).get(INTERRUPT_KEY) or ())
        return [(str(getattr(item, "id", "")), getattr(item, "value", None)) for item in items]

    def current_thread_id(self, call_args: Mapping[str, Any]) -> str:
        thread_id = get_config().get("configurable", {}).get("thread_id")
        if not thread_id:
            raise RuntimeError("@wait(mode='async') needs a thread_id in the run config")
        return str(thread_id)


_framework = LangGraphFramework()
wait = make_wait(_framework)
publish_interrupts = make_publish_interrupts(_framework)
questions_in = publish_interrupts.questions_in  # type: ignore[attr-defined]

__all__ = ["LangGraphFramework", "publish_interrupts", "questions_in", "wait"]
