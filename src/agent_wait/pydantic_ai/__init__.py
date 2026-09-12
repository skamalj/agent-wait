"""Pydantic AI implementor of `agent_wait.framework.Framework`, and the bound names.

    from agent_wait.pydantic_ai import wait, publish_interrupts

Install with `pip install agent-wait[pydantic-ai]`.

## What Pydantic AI provides, and what we use

- **Park:** a tool raises `ApprovalRequired(metadata=...)`. The run ends and
  `result.output` is a `DeferredToolRequests` whose `approvals` list the parked calls
  (`ToolCallPart.tool_call_id`, `.tool_name`, `.args`) and whose `metadata[id]` is
  whatever the tool attached. The agent must list `DeferredToolRequests` in its
  `output_type`, or the raise is an error.
- **Read back:** the same `result.output`. `stream()` ends with the same object.
- **Thread:** none. Pydantic AI has no checkpointer; `message_history` is a list the
  host owns and stores under a name of its choosing. For `mode="async"` the host puts
  that name in `deps` as `thread_id` (an attribute or a mapping key).
- **Resume** (the host's call, not ours):

      agent.run_sync(None, message_history=history, deferred_tool_results=DeferredToolResults(
          approvals={question_id: ToolApproved()},   # or ToolDenied(reason)
          metadata={question_id: answer},             # the answer, verbatim, if it is a dict
      ))

  On approval the framework calls the tool again with `ctx.tool_call_approved = True`
  and `ctx.tool_call_metadata = answer`; the `@wait` wrapper then applies the usual
  rules (a `decision` parameter receives the answer; `answer["args"]` edits the call).
  On denial the framework does not call the tool; the model sees the denial message.
  Pass the answer through `metadata`, not `override_args` -- `override_args` replaces
  the whole argument set and skips the wrapper's merge.

## Three things to know

- **The host owns idempotency.** A duplicate answer is rejected only if the
  `message_history` passed on resume already contains the tool's return -- so store
  the post-run history *before* acknowledging the answer message. Resuming twice from
  the pre-resume history runs the tool twice.
- **Name the `RunContext` parameter `ctx`.** The wrapper hides it from the published
  `args` by name, and Pydantic AI finds it by annotation -- both hold under
  `functools.wraps`.
- **`CallDeferred` calls are published too**, with the default policy: they are the
  non-human wait (a job handed to an external system). The answer for those goes in
  `DeferredToolResults.calls`, not `approvals`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

try:
    from pydantic_ai import ApprovalRequired, DeferredToolRequests, RunContext
except ImportError as _err:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "agent_wait.pydantic_ai needs Pydantic AI: pip install 'agent-wait[pydantic-ai]'"
    ) from _err

from ..framework import Framework
from ..wait import WAIT_KEY, make_publish_interrupts, make_wait

__all__ = ["PydanticAIFramework", "publish_interrupts", "questions_in", "wait"]


def _run_context(call_args: Mapping[str, Any]) -> RunContext[Any] | None:
    for value in call_args.values():
        if isinstance(value, RunContext):
            return value  # pyright: ignore[reportUnknownVariableType]
    return None


class PydanticAIFramework(Framework):
    name = "pydantic_ai"
    hidden_params = ("ctx",)

    def interrupt(self, value: Mapping[str, Any], call_args: Mapping[str, Any]) -> Any:
        ctx = _run_context(call_args)
        if ctx is None:
            # Without the context the re-run after approval is indistinguishable from
            # the first call, and the tool would park forever.
            raise TypeError("a @wait tool on Pydantic AI needs a `ctx: RunContext` parameter")
        if ctx.tool_call_approved:
            # The re-run after approval. The answer rides in the metadata the host
            # attached; without one, approval is the whole answer.
            answer: Any = ctx.tool_call_metadata
            return answer if answer is not None else {"action": "approve"}
        raise ApprovalRequired(metadata=dict(value))

    def interrupts_in(self, result: Any) -> list[tuple[str, Any]]:
        output: Any = getattr(result, "output", result)
        if not isinstance(output, DeferredToolRequests):
            return []
        out: list[tuple[str, Any]] = []
        for part in [*output.approvals, *output.calls]:
            attached = output.metadata.get(part.tool_call_id)
            if isinstance(attached, Mapping) and WAIT_KEY in attached:
                out.append((part.tool_call_id, dict(attached)))  # pyright: ignore[reportUnknownArgumentType]
            else:
                # A tool that never heard of this library: `requires_approval=True`, a bare
                # `ApprovalRequired`, or a `CallDeferred`. The call is the question.
                bare: dict[str, Any] = {"function": part.tool_name, "args": part.args_as_dict()}
                if attached:
                    bare["metadata"] = dict(attached)  # pyright: ignore[reportUnknownArgumentType]
                out.append((part.tool_call_id, bare))
        return out

    def current_thread_id(self, call_args: Mapping[str, Any]) -> str:
        ctx = _run_context(call_args)
        deps: Any = ctx.deps if ctx is not None else None
        if isinstance(deps, Mapping):
            thread_id: Any = cast(Mapping[str, Any], deps).get("thread_id")
        else:
            thread_id = getattr(deps, "thread_id", None)
        if not thread_id:
            raise RuntimeError("@wait(mode='async') on Pydantic AI needs `thread_id` in the run's deps")
        return str(thread_id)


_framework = PydanticAIFramework()

wait = make_wait(_framework)
publish_interrupts = make_publish_interrupts(_framework)
questions_in = publish_interrupts.questions_in  # type: ignore[attr-defined]
