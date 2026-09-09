"""The twelve correctness rules of REQUIREMENTS section 10, written once.

Subclass `WaitStoreConformance`, override `make_store()`, and the whole suite runs
against your store. `agent-wait` runs it over the in-memory and SQLite stores;
`agent-wait-aws` runs the same class over `DynamoWaitStore` on moto. If a rule holds in
one store and not another, the store is wrong -- the rule is not negotiable.

Every test is named for its rule (`test_rule_07_resume_idempotency`) so a failure names
the guarantee that broke, not the mechanism.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Mapping
from typing import Any

import pytest
from agent_wait import (
    FailingAnnounce,
    Ignore,
    InMemoryAnnounce,
    Resume,
    Start,
    Wait,
    WaitStore,
)
from rig import FOREIGN, Rig, StubAdapter, approval_policy


class Boom(Exception):
    """A process death, injected between two store writes."""


class CrashStore:
    """Wraps a store and dies after the Nth mutating call.

    This is the only honest way to test rule 1 and rule 7. The interesting failures in
    this design are not bad inputs -- they are a Lambda that stops existing between the
    checkpoint and the announce, and the redelivery that follows. Counting writes and
    killing the process at each one, then retrying, walks every one of those seams.
    """

    MUTATORS = frozenset(
        {"create", "transition", "set_fields", "record_applied", "park_answer", "take_parked_answer"}
    )

    def __init__(self, inner: WaitStore, crash_at: int | None = None) -> None:
        self.inner = inner
        self.crash_at = crash_at
        self.writes = 0
        self.reads = 0

    def disarm(self) -> None:
        self.crash_at = None

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self.inner, name)
        if name == "get":

            def counted_get(*args: Any, **kwargs: Any) -> Any:
                self.reads += 1
                return attr(*args, **kwargs)

            return counted_get
        if name not in self.MUTATORS:
            return attr

        def guarded(*args: Any, **kwargs: Any) -> Any:
            self.writes += 1
            if self.crash_at is not None and self.writes == self.crash_at:
                raise Boom(f"process died at write #{self.writes} ({name})")
            return attr(*args, **kwargs)

        return guarded


class WaitStoreConformance:
    """Override `make_store` and inherit twelve guarantees."""

    @staticmethod
    def make_store() -> WaitStore:  # pragma: no cover - overridden
        raise NotImplementedError

    @pytest.fixture()
    def rig(self) -> Rig:
        return Rig(store=self.make_store())

    # ------------------------------------------------------------------ rule 1
    @pytest.mark.conformance
    def test_rule_01_idempotent_create(self, rig: Rig) -> None:
        """Same interrupt, same checkpoint, twice: one wait, one announce."""
        first = rig.park()
        assert len(rig.announce.of("created")) == 1

        # SQS redelivers; the handler runs the whole thing again.
        second = rig.park()

        assert second.wait_id == first.wait_id
        assert len(rig.store.find(thread_id="order-4471")) == 1
        assert len(rig.announce.of("created")) == 1, "notified_at must suppress the second announce"

    @pytest.mark.conformance
    def test_rule_01_a_different_checkpoint_is_a_different_wait(self, rig: Rig) -> None:
        rig.park()
        rig.park(interrupt_id="int-1", checkpoint_id="ckpt-2")
        assert len(rig.store.find(thread_id="order-4471")) == 2

    @pytest.mark.conformance
    def test_rule_01_crash_injection_across_register(self) -> None:
        """Kill the process at each write inside register(), then redeliver.

        The number of writes is discovered rather than hardcoded, so this keeps walking
        every seam if `register()` grows another one."""
        probe = CrashStore(self.make_store())
        Rig(store=probe).park()
        writes = probe.writes
        assert writes >= 2, "register() should create the wait and stamp notified_at"

        for crash_at in range(1, writes + 1):
            store = CrashStore(self.make_store(), crash_at=crash_at)
            rig = Rig(store=store)

            with pytest.raises(Boom):
                rig.park()

            store.disarm()
            wait = rig.park()  # the redelivery

            waits = rig.store.find(thread_id="order-4471")
            assert len(waits) == 1, f"crash at write #{crash_at} produced {len(waits)} waits"
            assert wait.notified_at is not None
            created = len(rig.announce.of("created"))
            assert created <= 2, "at most one replayed announce, never more"

            rig.park()  # a third delivery must be completely silent
            assert len(rig.announce.of("created")) == created

    # ------------------------------------------------------------------ rule 2
    @pytest.mark.conformance
    def test_rule_02_one_conditional_write_per_transition(self, rig: Rig) -> None:
        """A human clicking and the scheduler firing in the same second."""
        wait = rig.park()

        approved = rig.answer(wait, "approve", answer_id="click-9f1")
        timed_out = rig.timeout(wait)

        assert isinstance(approved, Resume)
        assert isinstance(timed_out, Ignore)
        assert timed_out.reason == "already_answered"
        settled = rig.reload(wait)
        assert settled.status == "answered"
        assert settled.version == 1, "exactly one write moved the status"

    @pytest.mark.conformance
    def test_rule_02_concurrent_writers_exactly_one_wins(self, rig: Rig) -> None:
        """Hit the store's compare-and-set directly, from eight threads."""
        wait = rig.park()
        results: list[bool] = []
        lock = threading.Lock()
        barrier = threading.Barrier(8)

        def attempt(n: int) -> None:
            barrier.wait()
            won = rig.store.transition(wait.wait_id, expect="pending", to="answered", answer_id=f"click-{n}")
            with lock:
                results.append(won)

        threads = [threading.Thread(target=attempt, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sum(results) == 1, f"expected exactly one winner, got {sum(results)}"
        assert rig.reload(wait).version == 1

    # ------------------------------------------------------------------ rule 3
    @pytest.mark.conformance
    def test_rule_03_thread_lease_blocks_a_second_worker(self, rig: Rig) -> None:
        other = Rig(store=rig.store, clock=rig.clock)
        other.runtime.owner = "worker-b"

        mine = rig.runtime.dispatch({"thread_id": "order-4471", "input": {"x": 1}, "message_id": "m1"})
        assert isinstance(mine, Start)

        theirs = other.runtime.dispatch({"thread_id": "order-4471", "input": {"x": 1}, "message_id": "m2"})
        assert isinstance(theirs, Ignore)
        assert theirs.reason == "lease_held"
        assert theirs.should_retry, "lease_held is the one Ignore the caller must retry"

    @pytest.mark.conformance
    def test_rule_03_register_releases_the_lease(self, rig: Rig) -> None:
        rig.runtime.dispatch({"thread_id": "order-4471", "input": {}, "message_id": "m1"})
        rig.finish()
        assert rig.store.get_lease("order-4471") is None

        other = Rig(store=rig.store, clock=rig.clock)
        other.runtime.owner = "worker-b"
        assert isinstance(
            other.runtime.dispatch({"thread_id": "order-4471", "input": {}, "message_id": "m2"}), Start
        )

    @pytest.mark.conformance
    def test_rule_03_an_expired_lease_is_taken_over(self, rig: Rig) -> None:
        rig.runtime.dispatch({"thread_id": "order-4471", "input": {}, "message_id": "m1"})
        other = Rig(store=rig.store, clock=rig.clock)
        other.runtime.owner = "worker-b"

        rig.clock.advance(rig.runtime.lease_seconds + 1)

        assert isinstance(
            other.runtime.dispatch({"thread_id": "order-4471", "input": {}, "message_id": "m2"}), Start
        )

    @pytest.mark.conformance
    def test_rule_03_a_redelivered_start_does_not_reapply_input(self, rig: Rig) -> None:
        first = rig.runtime.dispatch({"thread_id": "t", "input": {"amount": 41000}, "message_id": "evt-1"})
        rig.finish("t")
        second = rig.runtime.dispatch({"thread_id": "t", "input": {"amount": 41000}, "message_id": "evt-1"})

        assert isinstance(first, Start) and first.input == {"amount": 41000}
        assert isinstance(second, Start) and second.input is None

    # ------------------------------------------------------------------ rule 4
    @pytest.mark.conformance
    def test_rule_04_duplicate_answer_is_a_duplicate(self, rig: Rig) -> None:
        wait = rig.park()
        assert isinstance(rig.answer(wait, "approve", answer_id="click-9f1"), Resume)

        again = rig.answer(wait, "approve", answer_id="click-9f1")

        assert isinstance(again, Ignore)
        assert again.reason == "duplicate", "the same click twice is success, not a conflict"

    @pytest.mark.conformance
    def test_rule_04_a_different_answer_is_a_conflict(self, rig: Rig) -> None:
        wait = rig.park()
        rig.answer(wait, "approve", answer_id="click-9f1")

        other = rig.answer(wait, "reject", answer_id="click-different")

        assert isinstance(other, Ignore)
        assert other.reason == "already_answered"
        assert rig.reload(wait).action == "approve", "the first answer stands"

    # ------------------------------------------------------------------ rule 5
    @pytest.mark.conformance
    def test_rule_05_parked_answer_is_stored_not_dropped(self, rig: Rig) -> None:
        """The approval beats the wait into existence -- a real race when the announce
        path is faster than the checkpoint write."""
        pending = rig.interrupt()
        provisional = Wait(
            wait_id="01JPARKED0000000000000000",
            thread_id="order-4471",
            framework="stub",
            interrupt_id=pending.interrupt_id,
            checkpoint_id=pending.checkpoint_id,
            idempotency_key="unused",
            question=pending.question,
            binding="0" * 64,
            policy=pending.policy,
        )
        early = rig.runtime.dispatch(rig.answer_payload(provisional, "approve", answer_id="click-early"))

        assert isinstance(early, Ignore)
        assert early.reason == "parked"

    @pytest.mark.conformance
    def test_rule_05_register_applies_the_parked_answer(self, rig: Rig) -> None:
        wait = rig.park()
        # Rewind: the wait is pending and an answer is sitting in the parked table.
        rig.store.park_answer(wait.wait_id, rig.answer_payload(wait, "approve", answer_id="click-early"))

        result = rig.run_register([rig.interrupt()], "order-4471")

        assert result.immediate_resume is not None, "register() must apply a parked answer"
        assert isinstance(result.immediate_resume, Resume)
        assert rig.reload(wait).status == "answered"
        assert rig.store.take_parked_answer(wait.wait_id) is None, "consumed exactly once"

    # ------------------------------------------------------------------ rule 6
    @pytest.mark.conformance
    def test_rule_06_late_timer_is_a_noop(self, rig: Rig) -> None:
        wait = rig.park()
        rig.answer(wait, "approve", answer_id="click-9f1")

        rig.clock.advance(3 * 86400)
        late = rig.timeout(wait)

        assert isinstance(late, Ignore)
        assert late.reason == "already_answered"
        settled = rig.reload(wait)
        assert settled.status == "answered" and settled.action == "approve"

    @pytest.mark.conformance
    def test_rule_06_timeout_applies_the_declared_default(self, rig: Rig) -> None:
        wait = rig.park()
        rig.clock.advance(3 * 86400)

        outcome = rig.timeout(wait)

        assert isinstance(outcome, Resume)
        assert outcome.command == {"resume_map": {"int-1": {"action": "reject", "reason": "no response"}}}
        assert rig.reload(wait).status == "expired"
        assert "expired" in rig.announce.transitions()

    @pytest.mark.conformance
    def test_rule_06_on_timeout_fail_does_not_resume(self, rig: Rig) -> None:
        wait = rig.park(policy=approval_policy(on_timeout="fail", default=None))

        outcome = rig.timeout(wait)

        assert isinstance(outcome, Ignore)
        assert rig.reload(wait).status == "failed"

    # ------------------------------------------------------------------ rule 7
    @pytest.mark.conformance
    def test_rule_07_resume_idempotency(self, rig: Rig) -> None:
        """The redelivery that would otherwise refund a customer twice."""
        wait = rig.park()
        first = rig.answer(wait, "approve", answer_id="click-9f1")
        assert isinstance(first, Resume)

        # The handler applied the resume, the graph issued the refund, then the process
        # died before acking. SQS redelivers the same answer.
        rig.adapter.advance_past(wait.interrupt_id)
        rig.store.transition(wait.wait_id, expect="answered", to="pending")  # force the retry path
        redelivered = rig.answer(wait, "approve", answer_id="click-9f1")

        assert isinstance(redelivered, Ignore)
        assert redelivered.reason == "not_pending"
        assert rig.reload(wait).status == "resumed"
        assert len(rig.adapter.resumes) == 1, "build_resume() must not be called a second time"

    @pytest.mark.conformance
    def test_rule_07_register_marks_answered_waits_resumed(self, rig: Rig) -> None:
        wait = rig.park()
        rig.answer(wait, "approve", answer_id="click-9f1")
        rig.adapter.advance_past(wait.interrupt_id)

        rig.finish()

        assert rig.reload(wait).status == "resumed"
        assert "resumed" in rig.announce.transitions()

    # ------------------------------------------------------------------ rule 8
    @pytest.mark.conformance
    def test_rule_08_tampered_token_is_rejected_without_a_store_read(self) -> None:
        store = CrashStore(self.make_store())
        rig = Rig(store=store)
        wait = rig.park()
        reads_before = store.reads

        token = rig.token_for(wait)
        forged = token[:-1] + ("A" if token[-1] != "A" else "B")
        outcome = rig.runtime.dispatch({"token": forged, "action": "approve", "answer_id": "x"})

        assert isinstance(outcome, Ignore)
        assert outcome.reason == "token_invalid"
        assert store.reads == reads_before, "a forged token must never reach the store"

    @pytest.mark.conformance
    def test_rule_08_expired_token_is_rejected(self, rig: Rig) -> None:
        wait = rig.park()
        token = rig.token_for(wait)
        rig.clock.advance(rig.tokens.ttl_seconds + 1)

        outcome = rig.runtime.dispatch({"token": token, "action": "approve", "answer_id": "x"})

        assert isinstance(outcome, Ignore)
        assert outcome.reason == "expired"

    @pytest.mark.conformance
    def test_rule_08_wrong_key_is_rejected(self, rig: Rig) -> None:
        wait = rig.park()
        attacker = Rig(store=rig.store, clock=rig.clock, keys=FOREIGN)

        outcome = rig.runtime.dispatch(
            {"token": attacker.token_for(wait), "action": "approve", "answer_id": "x"}
        )

        assert isinstance(outcome, Ignore)
        assert outcome.reason == "token_invalid"

    # ------------------------------------------------------------------ rule 9
    @pytest.mark.conformance
    def test_rule_09_binding_mismatch(self, rig: Rig) -> None:
        """A token minted for one question cannot answer a different one."""
        wait = rig.park()
        other = rig.park(interrupt_id="int-2", checkpoint_id="ckpt-2", question={"kind": "something_else"})
        borrowed = rig.token_for(other)
        # Point the other wait's token at this wait_id: same signature, wrong binding.
        parts = borrowed.split(".")
        parts[2] = wait.wait_id
        body = ".".join(parts[:-1])
        forged = f"{body}.{rig.tokens._mac('k1', body)}"

        outcome = rig.runtime.dispatch({"token": forged, "action": "approve", "answer_id": "x"})

        assert isinstance(outcome, Ignore)
        assert outcome.reason == "binding_mismatch"
        assert rig.reload(wait).status == "pending"

    # ------------------------------------------------------------------ rule 10
    @pytest.mark.conformance
    def test_rule_10_announce_isolation(self) -> None:
        failing = FailingAnnounce()
        healthy = InMemoryAnnounce()
        rig = Rig(store=self.make_store(), extra_announce=[failing, healthy])

        wait = rig.park()  # must not raise

        assert failing.calls == 1
        assert len(healthy.of("created")) == 1, "a broken adapter must not starve a healthy one"
        assert rig.reload(wait).status == "pending"

    @pytest.mark.conformance
    def test_rule_10_sweeper_reannounces_unnotified_waits(self) -> None:
        """Scenario D: the process died between the create and the announce."""
        failing = FailingAnnounce()
        rig = Rig(store=self.make_store(), announce=InMemoryAnnounce(), extra_announce=[])
        rig.runtime.announce.adapters = [failing]

        wait = rig.park()
        assert rig.reload(wait).notified_at is None, "a failed announce must not set notified_at"

        rig.runtime.announce.adapters = [rig.announce]
        counts = rig.runtime.sweep()

        assert counts["announced"] == 1
        assert len(rig.announce.of("created")) == 1
        assert rig.reload(wait).notified_at is not None
        assert rig.runtime.sweep()["announced"] == 0, "the sweeper must settle"

    # ------------------------------------------------------------------ rule 11
    @pytest.mark.conformance
    def test_rule_11_parallel_interrupts_are_independent(self, rig: Rig) -> None:
        result = rig.run_register(
            [
                rig.interrupt(interrupt_id="int-a", question={"q": "a"}),
                rig.interrupt(interrupt_id="int-b", question={"q": "b"}),
            ]
        )
        assert len(result.envelopes) == 2
        wait_a = rig.store.get(result.envelopes[0].wait_id)
        wait_b = rig.store.get(result.envelopes[1].wait_id)
        assert wait_a is not None and wait_b is not None

        outcome = rig.answer(wait_a, "approve", answer_id="click-a")

        assert isinstance(outcome, Resume)
        assert outcome.command == {"resume_map": {"int-a": {"action": "approve", "note": "within budget"}}}
        assert rig.reload(wait_b).status == "pending", "answering one must not touch the other"
        assert rig.reload(wait_b).notified_at is not None

        second = rig.answer(wait_b, "reject", answer_id="click-b")
        assert isinstance(second, Resume)
        assert second.command == {"resume_map": {"int-b": {"action": "reject", "note": "within budget"}}}

    # ------------------------------------------------------------------ rule 12
    @pytest.mark.conformance
    def test_rule_12_cancel(self, rig: Rig) -> None:
        wait = rig.park()

        assert rig.runtime.cancel(wait.wait_id, "order was withdrawn") is True

        assert rig.reload(wait).status == "cancelled"
        assert "cancelled" in rig.announce.transitions()
        detail = rig.announce.of("cancelled")[0].transition_detail
        assert detail["reason"] == "order was withdrawn"

        late = rig.answer(wait, "approve", answer_id="click-late")
        assert isinstance(late, Ignore)
        assert late.reason == "already_answered"

    @pytest.mark.conformance
    def test_rule_12_cancel_is_not_repeatable(self, rig: Rig) -> None:
        wait = rig.park()
        assert rig.runtime.cancel(wait.wait_id) is True
        assert rig.runtime.cancel(wait.wait_id) is False
        assert rig.runtime.cancel("no-such-wait") is False

    # ------------------------------------------------------------- crash injection
    @pytest.mark.conformance
    def test_dispatch_crash_injection_never_double_resumes(self) -> None:
        """Kill the process at each write inside dispatch(), then redeliver the answer."""
        probe = CrashStore(self.make_store())
        probe_rig = Rig(store=probe)
        probe_wait = probe_rig.park()
        probe.writes = 0
        probe_rig.answer(probe_wait, "approve", answer_id="click-9f1")
        writes = probe.writes
        assert writes >= 2, "dispatch() should settle the wait and count the resume attempt"

        for crash_at in range(1, writes + 1):
            store = CrashStore(self.make_store())
            rig = Rig(store=store)
            wait = rig.park()

            store.writes = 0
            store.crash_at = crash_at
            with contextlib.suppress(Boom):
                rig.answer(wait, "approve", answer_id="click-9f1")
            store.disarm()

            outcome = rig.answer(wait, "approve", answer_id="click-9f1")

            settled = rig.reload(wait)
            assert settled.status in ("answered", "resumed"), f"crash #{crash_at} left {settled.status}"
            if isinstance(outcome, Ignore):
                assert outcome.reason in ("duplicate", "not_pending")
            assert len(rig.adapter.resumes) <= 1, (
                f"crash #{crash_at}: build_resume ran {len(rig.adapter.resumes)} times"
            )

    # ------------------------------------------------------------- store primitives
    @pytest.mark.conformance
    def test_store_applied_messages_are_recorded_once(self, rig: Rig) -> None:
        assert rig.store.record_applied("t", "m1") is True
        assert rig.store.record_applied("t", "m1") is False
        assert rig.store.record_applied("t2", "m1") is True

    @pytest.mark.conformance
    def test_store_parked_answers_are_taken_once(self, rig: Rig) -> None:
        rig.store.park_answer("k", {"answer_id": "a"})
        assert rig.store.take_parked_answer("k") == {"answer_id": "a"}
        assert rig.store.take_parked_answer("k") is None

    @pytest.mark.conformance
    def test_store_find_filters(self, rig: Rig) -> None:
        wait = rig.park()
        assert [w.wait_id for w in rig.store.find(status="pending")] == [wait.wait_id]
        assert rig.store.find(status="answered") == []
        assert [w.wait_id for w in rig.store.find(thread_id="order-4471")] == [wait.wait_id]
        assert rig.store.find(notified=False) == []
        assert [w.wait_id for w in rig.store.find(notified=True)] == [wait.wait_id]
        assert rig.store.find(due_before=rig.clock.now()) == []
        assert [w.wait_id for w in rig.store.find(due_before=rig.clock.now() + 3 * 3600)] == [wait.wait_id]

    @pytest.mark.conformance
    def test_store_isolates_returned_records(self, rig: Rig) -> None:
        """A caller must not be able to reach through a returned Wait into the store."""
        wait = rig.park()
        if isinstance(wait.question, Mapping):
            mutable: Any = wait.question
            mutable["amount"] = 999_999
        assert rig.reload(wait).question["amount"] == 41000


class StubAdapterSanity:
    """Guards the guard: if the stub stops modelling a framework, the suite lies."""

    def test_stub_reports_pending_only_while_parked(self) -> None:
        adapter = StubAdapter()
        wait = Wait(
            wait_id="w",
            thread_id="t",
            framework="stub",
            interrupt_id="i",
            checkpoint_id="c",
            idempotency_key="k",
            question={},
            binding="b" * 64,
            policy=approval_policy(),
        )
        adapter.pending.add("i")
        assert adapter.still_pending(wait) is True
        adapter.advance_past("i")
        assert adapter.still_pending(wait) is False
