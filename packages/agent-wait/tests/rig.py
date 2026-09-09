"""A test rig: a `WaitRuntime` wired to fakes, plus the small moves every test makes.

The point of the rig is that a conformance test should read like the rule it is proving.
`rig.park()` then `rig.answer(wait, "approve")` should be the whole test, so that when
one fails you are looking at the rule and not at six lines of setup.

The framework here is a stub, not LangGraph. That is deliberate: the rules are
about the *core*, and they must hold for any framework. LangGraph gets its own
integration suite in `langgraph-wait`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from agent_wait import (
    EntryPoint,
    FakeClock,
    InMemoryAnnounce,
    PendingInterrupt,
    RegisterResult,
    StaticKeyProvider,
    TokenCodec,
    Wait,
    WaitPolicy,
    WaitRuntime,
    WaitStore,
)

KEYS = StaticKeyProvider({"k1": b"unit-test-key-one", "k0": b"unit-test-key-zero"}, "k1")
ROTATED = StaticKeyProvider({"k2": b"unit-test-key-two", "k1": b"unit-test-key-one"}, "k2")
FOREIGN = StaticKeyProvider({"k1": b"a-completely-different-key"}, "k1")

DEFAULT_QUESTION = {"kind": "refund_approval", "order_id": "order-4471", "amount": 41000}


def approval_policy(**overrides: Any) -> WaitPolicy:
    base: dict[str, Any] = {
        "timeout": "PT2H",
        "on_timeout": "resume_default",
        "default": {"action": "reject", "reason": "no response"},
        "allowed_actions": ("approve", "reject"),
        "tags": {"approver_group": "finance"},
    }
    base.update(overrides)
    return WaitPolicy(**base)


class StubAdapter:
    """A framework that does exactly what the test tells it to.

    `pending` is the set of interrupt ids the imaginary graph is currently parked on.
    Removing an id models the thread having moved on -- which is how rule 7 (a
    redelivered resume for a thread that already advanced) gets tested without a graph.
    """

    name = "stub"

    def __init__(self) -> None:
        self.pending: set[str] = set()
        self.resumes: list[tuple[str, Any]] = []
        # Section 18.5: threads the imaginary framework has persisted something for. A
        # thread absent from this set models a run that died before its first checkpoint.
        self.checkpointed: set[str] = set()

    def extract(self, result: Any, config: Any) -> list[PendingInterrupt]:
        if isinstance(result, list) and all(isinstance(r, PendingInterrupt) for r in result):
            return list(result)
        return []

    def build_resume(self, wait: Wait, resume_value: Any) -> Any:
        self.resumes.append((wait.interrupt_id, resume_value))
        return {"resume_map": {wait.interrupt_id: resume_value}}

    def still_pending(self, wait: Wait) -> bool:
        return wait.interrupt_id in self.pending

    def has_checkpoint(self, thread_id: str) -> bool:
        return thread_id in self.checkpointed

    def config_for(self, thread_id: str) -> dict[str, Any]:
        return {"configurable": {"thread_id": thread_id}}

    # -- test control --------------------------------------------------------
    def advance_past(self, interrupt_id: str) -> None:
        """The graph moved on: this interrupt is no longer parked."""
        self.pending.discard(interrupt_id)


@dataclass
class Rig:
    store: WaitStore
    clock: FakeClock = field(default_factory=FakeClock)
    adapter: StubAdapter = field(default_factory=StubAdapter)
    announce: InMemoryAnnounce = field(default_factory=InMemoryAnnounce)
    entry_point: EntryPoint = field(
        default_factory=lambda: EntryPoint("sqs", "https://sqs.ap-south-1.amazonaws.com/x/agent-runs.fifo")
    )
    extra_announce: list[Any] = field(default_factory=list)
    keys: StaticKeyProvider = KEYS
    runtime: WaitRuntime = field(init=False)
    tokens: TokenCodec = field(init=False)

    def __post_init__(self) -> None:
        self.tokens = TokenCodec(self.keys, clock=self.clock)
        self.runtime = WaitRuntime(
            adapter=self.adapter,
            store=self.store,
            tokens=self.tokens,
            announce=[self.announce, *self.extra_announce],
            entry_point=self.entry_point,
            clock=self.clock,
            owner="worker-a",
        )

    # -- moves ---------------------------------------------------------------
    def interrupt(
        self,
        *,
        interrupt_id: str = "int-1",
        checkpoint_id: str = "ckpt-1",
        question: Any = None,
        policy: WaitPolicy | None = None,
    ) -> PendingInterrupt:
        return PendingInterrupt(
            interrupt_id=interrupt_id,
            checkpoint_id=checkpoint_id,
            question=DEFAULT_QUESTION if question is None else question,
            policy=policy or approval_policy(),
        )

    def run_register(
        self, interrupts: list[PendingInterrupt], thread_id: str = "order-4471"
    ) -> RegisterResult:
        for pending in interrupts:
            self.adapter.pending.add(pending.interrupt_id)
        # A run that got as far as raising an interrupt has, by definition, been
        # checkpointed (section 18.5).
        self.adapter.checkpointed.add(thread_id)
        return self.runtime.register(interrupts, self.adapter.config_for(thread_id), thread_id)

    def park(self, thread_id: str = "order-4471", **kwargs: Any) -> Wait:
        """Register one interrupt and return the resulting wait."""
        result = self.run_register([self.interrupt(**kwargs)], thread_id)
        wait = self.store.get(result.envelopes[0].wait_id)
        assert wait is not None
        return wait

    def token_for(self, wait: Wait) -> str:
        return self.tokens.mint(wait)

    def answer_payload(
        self,
        wait: Wait,
        action: str = "approve",
        *,
        answer_id: str = "click-1",
        payload: Any = None,
        actor: str = "priya@corp",
        token: str | None = None,
    ) -> dict[str, Any]:
        return {
            "token": token or self.token_for(wait),
            "action": action,
            "payload": {"note": "within budget"} if payload is None else payload,
            "actor": actor,
            "answer_id": answer_id,
        }

    def answer(self, wait: Wait, action: str = "approve", **kwargs: Any) -> Any:
        return self.runtime.dispatch(self.answer_payload(wait, action, **kwargs))

    def timeout(self, wait: Wait) -> Any:
        """Exactly what `SchedulerAnnounce` puts on the queue when the schedule fires."""
        return self.runtime.dispatch(
            {
                "token": self.token_for(wait),
                "action": "timeout",
                "answer_id": f"timeout:{wait.wait_id}",
            }
        )

    def finish(self, thread_id: str = "order-4471") -> RegisterResult:
        """The graph ran to completion: no interrupts left, and state was persisted."""
        self.adapter.checkpointed.add(thread_id)
        return self.runtime.register([], self.adapter.config_for(thread_id), thread_id)

    def crashed(self, thread_id: str = "order-4471") -> RegisterResult:
        """The graph raised before the framework persisted anything.

        This is what `make_run_handler`'s `finally` does on the error path (section 18.3):
        `register()` still runs, so the lease is released -- but no checkpoint exists.
        """
        return self.runtime.register([], self.adapter.config_for(thread_id), thread_id)

    def reload(self, wait: Wait) -> Wait:
        fresh = self.store.get(wait.wait_id)
        assert fresh is not None
        return fresh
