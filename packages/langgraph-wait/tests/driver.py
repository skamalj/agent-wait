"""A local stand-in for the Lambda run handler.

It is the same three lines the real `make_run_handler` runs -- dispatch, invoke,
register -- without SQS, so the LangGraph integration tests can drive a real graph
through real waits and still kill the process wherever they like.

Keeping this separate from `agent_wait_aws.make_run_handler` is deliberate: if the
integration suite only ever passed through the AWS wrapper, a bug in the wrapper and a
bug in the core would be indistinguishable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from agent_wait import (
    EntryPoint,
    FakeClock,
    Ignore,
    InMemoryAnnounce,
    RegisterResult,
    Resume,
    Start,
    StaticKeyProvider,
    TokenCodec,
    WaitRuntime,
    WaitStore,
)
from agent_wait.store import InMemoryWaitStore
from langgraph_wait import LangGraphAdapter

KEYS = StaticKeyProvider({"k1": b"integration-test-key"}, "k1")


class Died(Exception):
    """The handler stopped existing part-way through. Not an error: a scenario."""


@dataclass
class LocalAgent:
    """One deployed agent: a graph, a runtime, and an entry point that receives both
    start messages and answers."""

    graph: Any
    store: WaitStore = field(default_factory=InMemoryWaitStore)
    clock: FakeClock = field(default_factory=FakeClock)
    announce: InMemoryAnnounce = field(default_factory=InMemoryAnnounce)
    owner: str = "worker-a"
    runtime: WaitRuntime = field(init=False)
    adapter: LangGraphAdapter = field(init=False)
    tokens: TokenCodec = field(init=False)
    invocations: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self.adapter = LangGraphAdapter(self.graph)
        self.tokens = TokenCodec(KEYS, clock=self.clock)
        self.runtime = WaitRuntime(
            adapter=self.adapter,
            store=self.store,
            tokens=self.tokens,
            announce=[self.announce],
            entry_point=EntryPoint("sqs", "https://sqs.local/agent-runs.fifo"),
            clock=self.clock,
            owner=self.owner,
        )

    # ------------------------------------------------------------------ delivery
    def deliver(self, payload: dict[str, Any], *, die_after_invoke: bool = False) -> Any:
        """Handle one inbound message. Returns `Ignore`, or the `RegisterResult`.

        `die_after_invoke=True` models the crash in scenarios A and C: the graph ran,
        its checkpoint was written, and the process vanished before `register()` -- or
        before acking, which amounts to the same thing.
        """
        outcome = self.runtime.dispatch(payload)
        if isinstance(outcome, Ignore):
            return outcome

        result = self._invoke(outcome)
        if die_after_invoke:
            raise Died("handler died after invoke(), before register()")

        registered = self.runtime.register(result, outcome.config, outcome.thread_id)
        return self._drain(registered)

    def _invoke(self, outcome: Start | Resume) -> Any:
        self.invocations += 1
        if isinstance(outcome, Start):
            return self.graph.invoke(outcome.input, outcome.config)
        return self.graph.invoke(outcome.command, outcome.config)

    def _drain(self, registered: RegisterResult) -> RegisterResult:
        """A parked answer can make `register()` hand back a resume immediately; the
        real handler loops on it, so this does too."""
        while registered.immediate_resume is not None:
            resume = registered.immediate_resume
            self.invocations += 1
            result = self.graph.invoke(resume.command, resume.config)
            registered = self.runtime.register(result, resume.config, resume.thread_id)
        return registered

    # ------------------------------------------------------------------ helpers
    def envelopes(self, transition: str = "created") -> list[Any]:
        return self.announce.of(transition)  # type: ignore[arg-type]

    def token(self, index: int = 0) -> str:
        return self.envelopes()[index].token

    def answer(
        self,
        action: str = "approve",
        *,
        answer_id: str = "click-1",
        index: int = 0,
        token: str | None = None,
        payload: dict[str, Any] | None = None,
        actor: str = "priya@corp",
    ) -> dict[str, Any]:
        """Build the answer envelope the world would send to `reply_to`."""
        return {
            "token": token or self.token(index),
            "action": action,
            "payload": payload if payload is not None else {"note": "within budget"},
            "actor": actor,
            "answer_id": answer_id,
        }

    def timeout_message(self, index: int = 0) -> dict[str, Any]:
        """Exactly what `SchedulerAnnounce` puts on the queue when the schedule fires."""
        envelope = self.envelopes()[index]
        return {
            "token": envelope.token,
            "action": "timeout",
            "answer_id": f"timeout:{envelope.wait_id}",
        }

    def wait(self, index: int = 0) -> Any:
        return self.store.get(self.envelopes()[index].wait_id)
