"""`WaitRuntime` -- `dispatch()` before the graph, `register()` after it.

Those two calls are the entire integration surface. There is no receiver service, no
answer Lambda, no timer Lambda and no `host` flag (REQUIREMENTS section 16.1). The world
answers at the agent's *existing* entry point, and `dispatch()` works out what the
inbound payload means:

    inbound payload ──► dispatch() ──► Start | Resume | Ignore
                                          │
                          graph.invoke(...)│
                                          ▼
                                      register() ──► envelopes, announced

Both algorithms are normative in REQUIREMENTS sections 4.1 and 4.2, and every branch
below is numbered against them.

Two habits run through the whole file:

* **Nothing here raises for an ordinary bad answer.** A forged token, a double click, a
  timer arriving after a human -- all of them are `Ignore`, because all of them are
  things a healthy system does every day. The caller acknowledges the message and moves
  on. Only `lease_held` asks to be retried.
* **Every state change is a conditional write.** The loser of a race learns it lost from
  a `False` return, re-reads, and reports what actually happened. This is what makes a
  human click and a three-day timer firing in the same second harmless.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any, Protocol, cast

from .announce.base import AnnounceAdapter
from .announce.composite import CompositeAnnounce
from .errors import TokenExpired, TokenInvalid
from .model import (
    Clock,
    EntryPoint,
    Ignore,
    PendingInterrupt,
    RegisterResult,
    Resume,
    Start,
    SystemClock,
    Transition,
    Wait,
    WaitEnvelope,
    binding_hash,
    check_question_size,
    default_owner_id,
    idempotency_key,
    iso,
    new_ulid,
)
from .store.base import APPLIED_TTL, DEFAULT_LEASE_SECONDS, PARKED_TTL, WaitStore
from .token import TokenCodec

_log = logging.getLogger("agent_wait.runtime")


class FrameworkAdapter(Protocol):
    """What the core needs from a framework, and nothing more (section 9.1).

    Five methods is the whole cost of supporting a new framework. Nothing in the core
    imports LangGraph, and nothing here assumes a graph, a checkpointer or a thread that
    lives in memory.
    """

    name: str

    def extract(self, result: Any, config: Any) -> list[PendingInterrupt]:
        """Interrupts raised by the run that just finished."""
        ...

    def build_resume(self, wait: Wait, resume_value: Any) -> Any:
        """The framework-specific command that resumes *this* interrupt."""
        ...

    def still_pending(self, wait: Wait) -> bool:
        """Is the thread genuinely still parked here? The guard that stops a redelivered
        resume from running a side effect twice (rule 7)."""
        ...

    def has_checkpoint(self, thread_id: str) -> bool:
        """Has the framework persisted any state at all for this thread?

        Section 18.5. `dispatch()` marks a start message applied before the graph runs,
        so a run that dies before the framework writes anything leaves a message recorded
        as applied against a thread with nothing to resume from. This is how the core
        tells that case apart from an ordinary redelivery.
        """
        ...

    def config_for(self, thread_id: str) -> dict[str, Any]: ...


class WaitRuntime:
    def __init__(
        self,
        *,
        adapter: FrameworkAdapter,
        store: WaitStore,
        tokens: TokenCodec,
        announce: Sequence[AnnounceAdapter],
        entry_point: EntryPoint,
        clock: Clock | None = None,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        owner: str | None = None,
    ) -> None:
        self.adapter = adapter
        self.store = store
        self.tokens = tokens
        self.announce = CompositeAnnounce(announce)
        self.entry_point = entry_point
        self.clock: Clock = clock or SystemClock()
        self.lease_seconds = lease_seconds
        self.owner = owner or default_owner_id()

    # ==================================================================== dispatch
    def dispatch(self, payload: Mapping[str, Any]) -> Start | Resume | Ignore:
        """Section 4.1. Call on every inbound payload, before invoking the graph."""
        # A trust boundary, not a redundant check: this value came off a queue, and
        # the annotation says what a caller *should* send, not what arrives.
        if not isinstance(payload, Mapping):  # pyright: ignore[reportUnnecessaryIsInstance]
            return Ignore("unknown_payload", "payload is not a mapping")
        if "token" not in payload:
            return self._dispatch_start(payload)
        return self._dispatch_answer(payload)

    # -- 4.1.1 start ---------------------------------------------------------
    def _dispatch_start(self, payload: Mapping[str, Any]) -> Start | Ignore:
        thread_id = payload.get("thread_id")
        if not isinstance(thread_id, str) or not thread_id:
            return Ignore("unknown_payload", "no token and no thread_id")

        if not self._take_lease(thread_id):
            return Ignore("lease_held", f"another worker holds the lease on {thread_id}")

        config = self.adapter.config_for(thread_id)
        message_id = payload.get("message_id")
        if not message_id:
            # Nothing to dedupe on. The caller (or SQS, via make_run_handler) should
            # supply one; without it we can only take the message at face value.
            return Start(thread_id, payload.get("input"), config)

        first_time = self.store.record_applied(thread_id, str(message_id), ttl=self.clock.now() + APPLIED_TTL)
        if not first_time:
            # A redelivery -- but "applied" is recorded here, *before* the graph runs, so
            # it means "we started on this message", not "the framework kept the result".
            # Section 18.5: ask the framework which of the two it is.
            if self.adapter.has_checkpoint(thread_id):
                # Ordinary redelivery. Re-invoke with None so the framework advances from
                # its own checkpoint instead of applying the same input a second time.
                return Start(thread_id, None, config)
            # The first run died before anything was persisted. Nothing was applied, so
            # replaying with None would hand the framework a thread it has never heard of;
            # the input is what has to go back in.
            _log.info(
                "thread %s was marked applied but has no checkpoint; the first run died "
                "before persisting anything, so replaying the input",
                thread_id,
            )
            return Start(thread_id, payload.get("input"), config)
        return Start(thread_id, payload.get("input"), config)

    # -- 4.1.2 answer --------------------------------------------------------
    def _dispatch_answer(self, payload: Mapping[str, Any]) -> Resume | Ignore:
        token = payload.get("token")
        if not isinstance(token, str):
            return Ignore("token_invalid", "token is not a string")

        # 2.1 -- verified before any store read, so forged tokens cost us one HMAC.
        try:
            claims = self.tokens.verify(token)
        except TokenExpired as err:
            return Ignore("expired", str(err))
        except TokenInvalid as err:
            return Ignore("token_invalid", str(err))

        action = payload.get("action") or "resume"
        if not isinstance(action, str):
            return Ignore("unknown_payload", "action must be a string")
        answer_id = payload.get("answer_id")
        if not answer_id or not isinstance(answer_id, str):
            return Ignore("unknown_payload", "answer_id is required (section 7.2)")

        # 2.2 -- the answer may legitimately beat the wait into existence.
        wait = self.store.get(claims.wait_id)
        if wait is None:
            self.store.park_answer(claims.wait_id, dict(payload), ttl=self.clock.now() + PARKED_TTL)
            return Ignore("parked", "answer stored; the wait does not exist yet")

        # 2.2a -- rule 9, numbered by section 18.6. The token is bound to the exact
        # question, so an edited graph invalidates tokens already sitting in inboxes.
        # Settled before the allowed-action check, and before any write: nothing may be
        # decided on the strength of a token that belongs to a different question.
        if wait.binding16 != claims.binding16:
            return Ignore("binding_mismatch", "the question changed since this token was issued")

        # 2.3
        if not wait.policy.permits(action):
            return Ignore("action_not_allowed", f"{action!r} is not permitted for this wait")

        # 2.4
        if wait.status != "pending":
            return self._already_settled(wait, answer_id)

        # 2.5
        if not self._take_lease(wait.thread_id):
            return Ignore("lease_held", f"another worker holds the lease on {wait.thread_id}")

        return self._apply_answer(wait, payload, action=action, answer_id=answer_id)

    def _already_settled(self, wait: Wait, answer_id: str) -> Ignore:
        """Rule 4. The same click twice is success; a different click is a conflict."""
        if wait.answer_id is not None and wait.answer_id == answer_id:
            return Ignore("duplicate", f"already applied; wait is {wait.status}")
        return Ignore("already_answered", f"wait is {wait.status}")

    def _apply_answer(
        self, wait: Wait, payload: Mapping[str, Any], *, action: str, answer_id: str
    ) -> Resume | Ignore:
        now = self.clock.now()
        is_timeout = action == "timeout"
        resume_value = (
            self._timeout_value(wait.policy.default) if is_timeout else self._resume_value(action, payload)
        )

        # 2.6 -- the one conditional write per transition (rule 2). A human click and the
        # scheduler firing in the same second both land here; exactly one returns True.
        won = self.store.transition(
            wait.wait_id,
            expect="pending",
            to="expired" if is_timeout else "answered",
            answer=resume_value,
            action=action,
            # Section 18.1: whatever the graph is told, the record says a timer did this.
            actor="system:timer" if is_timeout else payload.get("actor"),
            answered_at=now,
            answer_id=answer_id,
            updated_at=now,
        )
        if not won:
            fresh = self.store.get(wait.wait_id)
            if fresh is None:  # pragma: no cover - the row cannot vanish mid-flight
                return Ignore("already_answered", "wait disappeared during the write")
            return self._already_settled(fresh, answer_id)

        settled = self.store.get(wait.wait_id) or wait
        # 2.7 -- announce; SchedulerAnnounce takes this as its cue to delete the timer.
        self._announce(settled, "expired" if is_timeout else "answered")

        if is_timeout and settled.policy.on_timeout == "fail":
            # Section 18.2. The author asked for the thread to be abandoned rather than
            # resumed. Two conditional writes, not one: `pending -> expired` above has
            # already fired the `expired` announce (so the schedule is deleted and the
            # world is told), and only then does `expired -> failed` close it out. The
            # graph is never invoked.
            self.store.transition(settled.wait_id, expect="expired", to="failed", updated_at=now)
            self._release_lease(settled.thread_id)
            return Ignore("failed", "on_timeout='fail': the wait expired and was marked failed")

        # 2.8 -- rule 7. The thread may already have moved past this interrupt, in which
        # case invoking the graph again would repeat a side effect that already happened.
        if not self.adapter.still_pending(settled):
            if self.store.transition(settled.wait_id, expect=settled.status, to="resumed", updated_at=now):
                self._announce(self.store.get(settled.wait_id) or settled, "resumed")
            self._release_lease(settled.thread_id)
            return Ignore("not_pending", "the thread already moved past this interrupt")

        # 2.9
        self.store.set_fields(settled.wait_id, resume_attempts=settled.resume_attempts + 1, updated_at=now)
        command = self.adapter.build_resume(settled, resume_value)
        return Resume(
            thread_id=settled.thread_id,
            command=command,
            config=self.adapter.config_for(settled.thread_id),
            wait=settled,
        )

    @staticmethod
    def _resume_value(action: str, payload: Mapping[str, Any]) -> Any:
        """Section 4.1.9, as amended by section 18.1: the answer envelope's `payload`,
        with `action` merged in **last**.

        The merge order is the whole point. `{"action": action, **payload}` would let a
        sender smuggle `{"payload": {"action": "approve"}}` past a wait that only permits
        `reject`: the allowed-actions check would see `reject` and pass, and the graph
        would then read `approve`. The envelope's `action` always wins.
        """
        body = payload.get("payload")
        if isinstance(body, Mapping):
            return {**dict(cast("Mapping[str, Any]", body)), "action": action}
        if body is None:
            return {"action": action}
        return {"action": action, "payload": body}

    @staticmethod
    def _timeout_value(default: Any) -> Any:
        """Section 18.1 as amended: the author's default reaches the graph as written.

        The smuggling argument that fixed the *answer* merge order does not apply here.
        A `payload` arrives from outside and is checked against `allowed_actions`, so the
        envelope's action has to win. A `default` is declared by the graph author, in the
        graph, next to the question -- there is no second party to defend against, and
        overriding it would mean an author who wrote `{"action": "reject"}` gets something
        they did not ask for.

        So: a mapping default is passed through unchanged, gaining `action: "timeout"`
        only if it says nothing about an action; a non-mapping default is untouched.

        The audit trail is unaffected -- the wait record and every announce still carry
        `action="timeout"` and `actor="system:timer"`. What happened and what the graph
        was told are two different questions, and they get two different answers.
        """
        if not isinstance(default, Mapping):
            return default
        body = dict(cast("Mapping[str, Any]", default))
        body.setdefault("action", "timeout")
        return body

    # ==================================================================== register
    def register(self, result: Any, config: Any, thread_id: str) -> RegisterResult:
        """Section 4.2. Call after `graph.invoke()` returns, always -- including when the
        run finished normally, because that is when parked waits get closed out."""
        now = self.clock.now()
        interrupts = self.adapter.extract(result, config)

        # 4.2.1 -- no interrupts means the thread advanced; settle whatever it left behind.
        if not interrupts:
            self._finalize_thread(thread_id, now)
            self._release_lease(thread_id)
            return RegisterResult([], None)

        envelopes: list[WaitEnvelope] = []
        immediate: Resume | None = None

        for pending in interrupts:
            wait, created = self._create_wait(thread_id, pending, now)
            envelope = self.envelope_for(wait, "created")

            # 4.2.3 -- announce once. `notified_at` is the flag that keeps a redelivered
            # start from re-notifying (rule 1) and lets the sweeper retry a failed one.
            if created or wait.notified_at is None:
                if self.announce.announce_reporting(envelope, "created"):
                    self.store.set_fields(wait.wait_id, notified_at=now, updated_at=now)
                    wait = replace(wait, notified_at=now)
                else:
                    _log.warning("no announce adapter accepted wait %s; the sweeper will retry", wait.wait_id)
            envelopes.append(envelope)

            # 4.2.4 -- an answer that arrived before this wait existed (rule 5).
            if immediate is None:
                parked = self._take_parked(wait)
                if parked is not None:
                    outcome = self.dispatch(parked)
                    if isinstance(outcome, Resume):
                        immediate = outcome
                    else:
                        _log.info(
                            "parked answer for wait %s resolved to %s",
                            wait.wait_id,
                            getattr(outcome, "reason", "?"),
                        )

        # 4.2.5. Held deliberately when there is an immediate resume: the run has not
        # finished, and the handler is about to invoke the graph again on this thread.
        if immediate is None:
            self._release_lease(thread_id)
        return RegisterResult(envelopes, immediate)

    def _create_wait(self, thread_id: str, pending: PendingInterrupt, now: float) -> tuple[Wait, bool]:
        check_question_size(pending.question)
        timeout = pending.policy.timeout_seconds
        candidate = Wait(
            wait_id=new_ulid(self.clock),
            thread_id=thread_id,
            framework=self.adapter.name,
            interrupt_id=pending.interrupt_id,
            checkpoint_id=pending.checkpoint_id,
            idempotency_key=idempotency_key(thread_id, pending.interrupt_id, pending.checkpoint_id),
            question=pending.question,
            binding=binding_hash(pending.question, pending.interrupt_id, pending.checkpoint_id),
            policy=pending.policy,
            created_at=now,
            updated_at=now,
            expires_at=(now + timeout) if timeout else None,
        )
        # Rule 1: idempotent on (thread, interrupt, checkpoint). A crash between the
        # checkpoint and this line is the ordinary case, not the exceptional one.
        wait, created = self.store.create(candidate)
        if not created:
            _log.debug("wait %s already existed for this interrupt", wait.wait_id)
        return wait, created

    def _take_parked(self, wait: Wait) -> dict[str, Any] | None:
        parked = self.store.take_parked_answer(wait.wait_id)
        if parked is not None:
            return parked
        correlation_key = wait.policy.correlation_key()
        if correlation_key:
            return self.store.take_parked_answer(correlation_key)
        return None

    def _finalize_thread(self, thread_id: str, now: float) -> None:
        for wait in self.store.find(thread_id=thread_id):
            if wait.status not in ("answered", "expired"):
                continue
            if self.adapter.still_pending(wait):
                continue
            if self.store.transition(wait.wait_id, expect=wait.status, to="resumed", updated_at=now):
                self._announce(self.store.get(wait.wait_id) or wait, "resumed")

    # ==================================================================== cancel
    def cancel(self, wait_id: str, reason: str = "") -> bool:
        """Rule 12. Cancelling only ever affects a pending wait; later answers get
        `already_answered`. The `cancelled` announce is what deletes the schedule."""
        wait = self.store.get(wait_id)
        if wait is None:
            return False
        now = self.clock.now()
        if not self.store.transition(
            wait_id, expect="pending", to="cancelled", last_error=reason, updated_at=now
        ):
            return False
        self._announce(self.store.get(wait_id) or wait, "cancelled")
        return True

    # ==================================================================== sweeper
    def sweep(self, *, limit: int = 100) -> dict[str, int]:
        """Rule 10 / scenario D. Delegates to `agent_wait.sweep` (imported lazily to keep
        the module graph acyclic)."""
        from .sweep import sweep as _sweep

        return _sweep(self, limit=limit)

    # ==================================================================== leases
    def _take_lease(self, thread_id: str) -> bool:
        now = self.clock.now()
        return self.store.acquire_lease(thread_id, self.owner, expires_at=now + self.lease_seconds, now=now)

    def refresh_lease(self, thread_id: str) -> bool:
        """Called from the run handler's heartbeat while a long graph is running."""
        return self.store.refresh_lease(
            thread_id, self.owner, expires_at=self.clock.now() + self.lease_seconds
        )

    def _release_lease(self, thread_id: str) -> None:
        self.store.release_lease(thread_id, self.owner)

    # ==================================================================== envelopes
    def envelope_for(self, wait: Wait, transition: Transition) -> WaitEnvelope:
        detail: dict[str, Any] = {}
        if transition in ("answered", "expired"):
            detail = {
                "action": wait.action,
                "actor": wait.actor,
                "answered_at": iso(wait.answered_at),
            }
        elif transition == "cancelled" and wait.last_error:
            detail = {"reason": wait.last_error}
        return WaitEnvelope(
            type=f"wait.{transition}",
            event_id=new_ulid(self.clock),
            wait_id=wait.wait_id,
            thread_id=wait.thread_id,
            question=wait.question,
            allowed_actions=wait.policy.allowed_actions,
            expires_at=iso(wait.expires_at),
            token=self.tokens.mint(wait),
            reply_to=self.entry_point.to_dict(),
            correlation=wait.policy.correlation,
            tags=wait.policy.tags,
            transition_detail=detail,
        )

    def _announce(self, wait: Wait, transition: Transition) -> None:
        self.announce.announce(self.envelope_for(wait, transition), transition)
