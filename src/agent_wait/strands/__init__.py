"""Strands Agents implementor of `agent_wait.framework.Framework`, and the bound names.

    from agent_wait.strands import wait, publish_interrupts

Install with `pip install agent-wait[strands]`.

## What Strands provides, and what we use

- **Park:** a tool declared `@tool(context=True)` receives a `ToolContext`;
  `tool_context.interrupt(name, reason=value)` raises `InterruptException` the first
  time and returns the human's response when the tool runs again after resume. The id
  is `v1:tool_call:<toolUseId>:<uuid5(name)>` -- one per tool call, no collisions.
- **Read back:** `result.stop_reason == "interrupt"` and `result.interrupts`, each an
  `Interrupt(id, name, reason, response)`. `reason` is what we parked on, verbatim.
- **Thread:** `agent.session_id` -- the session manager's id when one is attached, a
  random per-instance id otherwise.
- **Resume** (the host's call, not ours):

      agent([{"interruptResponse": {"interruptId": question_id, "response": answer}}])

  The tool runs again, `interrupt()` returns `answer`, and the `@wait` wrapper applies
  its usual rules.

## Three things to know

- **`@tool(context=True)` and name the parameter `tool_context`.** The wrapper hides it
  from the published `args` by that name; Strands injects it by that name.
- **A session manager is what makes the wait durable.** Interrupt state lives on the
  agent instance and is persisted by `FileSessionManager` / `S3SessionManager` (or
  your own) between the return and the resume. Without one, the parked question dies
  with the process.
- **A duplicate answer is refused, not ignored.** Resuming a run that is not parked
  raises `ValueError("... not in interrupt state")`; an unknown id raises `KeyError`;
  a plain prompt to a parked run raises `TypeError`. None of them runs anything. The
  host checks `result.stop_reason` (or its own record) before resuming, or catches.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

try:
    from strands.types.tools import ToolContext
except ImportError as _err:  # pragma: no cover - exercised only without the extra
    raise ImportError("agent_wait.strands needs Strands Agents: pip install 'agent-wait[strands]'") from _err

from ..framework import Framework
from ..wait import make_publish_interrupts, make_wait

__all__ = ["StrandsFramework", "publish_interrupts", "questions_in", "wait"]

INTERRUPT_NAME = "agent_wait"


def _tool_context(call_args: Mapping[str, Any]) -> ToolContext[Any] | None:
    for value in call_args.values():
        if isinstance(value, ToolContext):
            return value  # pyright: ignore[reportUnknownVariableType]
    return None


class StrandsFramework(Framework):
    name = "strands"
    hidden_params = ("tool_context",)

    def interrupt(self, value: Mapping[str, Any], call_args: Mapping[str, Any]) -> Any:
        ctx = _tool_context(call_args)
        if ctx is None:
            raise TypeError(
                "a @wait tool on Strands needs `@tool(context=True)` and a `tool_context` parameter"
            )
        return ctx.interrupt(INTERRUPT_NAME, reason=dict(value))

    def interrupts_in(self, result: Any) -> list[tuple[str, Any]]:
        if getattr(result, "stop_reason", None) != "interrupt":
            return []
        interrupts: Any = getattr(result, "interrupts", None) or ()
        return [(str(item.id), item.reason) for item in interrupts]

    def current_thread_id(self, call_args: Mapping[str, Any]) -> str:
        ctx = _tool_context(call_args)
        session_id: Any = getattr(getattr(ctx, "agent", None), "session_id", None)
        if not session_id:
            raise RuntimeError("@wait(mode='async') on Strands needs the agent's session_id")
        return str(session_id)


_framework = StrandsFramework()

wait = make_wait(_framework)
publish_interrupts = make_publish_interrupts(_framework)
questions_in = publish_interrupts.questions_in  # type: ignore[attr-defined]
