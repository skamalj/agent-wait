"""`Framework` -- the interface a framework subpackage implements.

agent-wait needs exactly three things from an agent framework, and nothing else:

1. a way to **park** the current run on a value and get the answer back when it resumes;
2. a way to **read back** what a finished run parked on, from whatever `invoke()` returned;
3. in async mode, a way to know **which thread** the current call belongs to.

Everything users touch -- `@wait`, `publish_interrupts`, the envelope, the announcers --
is built once in `agent_wait.wait` on top of this interface. A framework subpackage
(`agent_wait.langgraph`, `agent_wait.strands`, ...) is one implementor of it and two
lines of binding. Users never see this class; they import the bound names.

The same split holds for announcers: `announce.base.BaseAnnounce` is the interface, and
each provider subpackage (`agent_wait.aws`, ...) is implementors.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any


class Framework(ABC):
    """What one agent framework has to provide. Three methods and a name."""

    name: str = "framework"

    hidden_params: tuple[str, ...] = ()
    """Parameters the framework injects into a tool call that are not part of the
    question -- Strands' `tool_context`, for instance. `@wait` keeps them out of the
    published `args` and passes them through to `interrupt()` and `current_thread_id()`."""

    @abstractmethod
    def interrupt(self, value: Mapping[str, Any], call_args: Mapping[str, Any]) -> Any:
        """Park the run on `value`. On the run that resumes, return the answer.

        `value` is the packed question (see `agent_wait.wait.pack`); the framework stores
        it verbatim and hands it back through `interrupts_in()`. `call_args` are the
        decorated function's bound arguments including any `hidden_params`.
        """

    @abstractmethod
    def interrupts_in(self, result: Any) -> list[tuple[str, Any]]:
        """`(id, value)` for every interrupt a finished run returned. `[]` if none.

        Must be derivable from `result` alone -- no graph handle, no state read. The id
        must be stable across a re-run of the same parked thread, because it is the
        consumer's deduplication key.
        """

    @abstractmethod
    def current_thread_id(self, call_args: Mapping[str, Any]) -> str:
        """The conversation the current call belongs to. Used only in async mode."""
