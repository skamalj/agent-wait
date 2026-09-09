"""
agent-wait core (simulation-grade, but the shapes are the real design).

Four nouns: Wait, Token, Answer, Event.
Five ports: WaitStore, TokenCodec, Timer, Notifier, Resumer.
One orchestrator: Waiter.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import uuid

_CLOCK_OFFSET = 0.0
def now() -> float:
    """time.time() plus a simulation offset so scenarios can advance days."""
    return time.time() + _CLOCK_OFFSET

def advance(seconds: float):
    global _CLOCK_OFFSET
    _CLOCK_OFFSET += seconds

from dataclasses import dataclass, field, asdict
from typing import Any, Literal, Protocol

Status = Literal["pending", "answered", "expired", "cancelled", "resumed", "failed"]


# ----------------------------------------------------------------------------- model
@dataclass
class WaitPolicy:
    timeout_s: int | None = None
    on_timeout: Literal["resume_default", "fail"] = "resume_default"
    default: Any = None
    allowed_actions: list[str] = field(default_factory=lambda: ["resume"])
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class Wait:
    wait_id: str
    thread_id: str
    framework: str
    interrupt_id: str
    checkpoint_id: str
    idempotency_key: str
    question: Any
    binding: str
    policy: WaitPolicy
    status: Status = "pending"
    token_kind: str = "synthetic"
    token_ref: str | None = None
    timer_ref: str | None = None
    notified_at: float | None = None
    answer: Any = None
    action: str | None = None
    answered_by: str | None = None
    answered_at: float | None = None
    answer_id: str | None = None
    resume_attempts: int = 0
    created_at: float = field(default_factory=lambda: now())
    expires_at: float | None = None
    version: int = 0


@dataclass
class WaitEvent:
    type: str
    wait_id: str
    thread_id: str
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    token_url: str | None = None
    question: Any = None
    tags: dict[str, str] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class PendingInterrupt:
    interrupt_id: str
    checkpoint_id: str
    question: Any
    policy: WaitPolicy


def canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def sha(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


# ----------------------------------------------------------------------------- ports
class WaitStore(Protocol):
    def create(self, wait: Wait) -> tuple[Wait, bool]: ...            # (record, created?) idempotent
    def get(self, wait_id: str) -> Wait | None: ...
    def find(self, *, status: Status | None = None, thread_id: str | None = None) -> list[Wait]: ...
    def transition(self, wait_id: str, *, expect: Status, to: Status, **fields) -> bool: ...
    def set_refs(self, wait_id: str, **fields) -> None: ...


class TokenCodec(Protocol):
    kind: str
    def issue(self, wait: Wait) -> str: ...
    def verify(self, token: str) -> dict: ...                          # {wait_id, binding16, actions, exp}


class Timer(Protocol):
    def schedule(self, wait_id: str, fire_at: float) -> str: ...
    def cancel(self, timer_ref: str) -> None: ...


class Notifier(Protocol):
    def emit(self, event: WaitEvent) -> None: ...


class Resumer(Protocol):
    serialises_per_thread: bool
    def resume(self, wait: Wait, resume_input: Any) -> str: ...       # "QUEUED" | "DIRECT" | "NATIVE"


class FrameworkAdapter(Protocol):
    name: str
    def extract(self, result: Any, config: Any) -> list[PendingInterrupt]: ...
    def build_resume(self, wait: Wait, payload: Any) -> Any: ...
    def still_pending(self, wait: Wait) -> bool: ...


# ----------------------------------------------------------------------------- errors
class WaitError(Exception): ...
class TokenInvalid(WaitError): ...
class WaitNotFound(WaitError): ...
class WaitExpired(WaitError): ...
class BindingMismatch(WaitError): ...
class ActionNotAllowed(WaitError): ...
class AlreadyAnswered(WaitError): ...


# ----------------------------------------------------------------------------- synthetic tokens
class HmacTokenCodec:
    """aw1.<kid>.<wait_id>.<exp>.<binding16>.<actions>.<mac>"""
    kind = "synthetic"

    def __init__(self, keys: dict[str, bytes], current: str, ttl_s: int = 7 * 86400):
        self.keys, self.current, self.ttl_s = keys, current, ttl_s

    def _mac(self, kid: str, body: str) -> str:
        return base64.urlsafe_b64encode(hmac.new(self.keys[kid], body.encode(), hashlib.sha256).digest()[:20]).decode().rstrip("=")

    def issue(self, wait: Wait) -> str:
        exp = int(now() + self.ttl_s)          # token validity is independent of the wait's timeout; the store is the truth
        body = f"aw1.{self.current}.{wait.wait_id}.{exp}.{wait.binding[:16]}.{'+'.join(wait.policy.allowed_actions)}"
        return f"{body}.{self._mac(self.current, body)}"

    def verify(self, token: str) -> dict:
        try:
            prefix, kid, wait_id, exp, b16, actions, mac = token.split(".")
        except ValueError:
            raise TokenInvalid("malformed")
        if prefix != "aw1" or kid not in self.keys:
            raise TokenInvalid("unknown version/key")
        body = token.rsplit(".", 1)[0]
        if not hmac.compare_digest(self._mac(kid, body), mac):
            raise TokenInvalid("bad signature")
        if now() > int(exp):
            raise WaitExpired("token expired")
        return {"wait_id": wait_id, "binding16": b16, "actions": actions.split("+"), "exp": int(exp)}


# ----------------------------------------------------------------------------- the orchestrator
class Waiter:
    def __init__(self, *, adapter: FrameworkAdapter, store: WaitStore, tokens: TokenCodec,
                 timer: Timer, notifier: Notifier, resumer: Resumer, base_url: str, log=print):
        self.adapter, self.store, self.tokens = adapter, store, tokens
        self.timer, self.notifier, self.resumer, self.base_url = timer, notifier, resumer, base_url
        self.log = log

    # ---- step 1: after a run, park any interrupts
    def register(self, result: Any, config: Any, thread_id: str) -> list[Wait]:
        waits = []
        for pi in self.adapter.extract(result, config):
            key = sha(thread_id, pi.interrupt_id, pi.checkpoint_id)
            binding = sha(canonical(pi.question), pi.interrupt_id, pi.checkpoint_id)
            t0 = now()
            wait = Wait(
                wait_id=f"w_{uuid.uuid4().hex[:10]}", thread_id=thread_id, framework=self.adapter.name,
                interrupt_id=pi.interrupt_id, checkpoint_id=pi.checkpoint_id, idempotency_key=key,
                question=pi.question, binding=binding, policy=pi.policy,
                expires_at=(t0 + pi.policy.timeout_s) if pi.policy.timeout_s else None,
            )
            wait, created = self.store.create(wait)
            if not created:
                self.log(f"      [waiter] wait {wait.wait_id} already exists for this interrupt (idempotent) — no duplicate")
            self._complete_setup(wait)
            waits.append(wait)
        return waits

    def _complete_setup(self, wait: Wait):
        """Idempotent: safe to call from register() and from the sweeper."""
        if wait.status != "pending":
            return
        if wait.expires_at and not wait.timer_ref:
            ref = self.timer.schedule(wait.wait_id, wait.expires_at)
            self.store.set_refs(wait.wait_id, timer_ref=ref); wait.timer_ref = ref
        if not wait.notified_at:
            token = self.tokens.issue(wait)
            self.notifier.emit(WaitEvent("wait.created", wait.wait_id, wait.thread_id,
                                         token_url=f"{self.base_url}{token}", question=wait.question, tags=wait.policy.tags))
            self.store.set_refs(wait.wait_id, notified_at=now()); wait.notified_at = now()

    # ---- step 2: the outside world answers
    def answer(self, token: str, *, action: str, payload: Any, actor: str, answer_id: str) -> str:
        claims = self.tokens.verify(token)
        wait = self.store.get(claims["wait_id"])
        if not wait:
            raise WaitNotFound(claims["wait_id"])
        if wait.binding[:16] != claims["binding16"]:
            raise BindingMismatch("the question changed since this token was issued")
        if action not in wait.policy.allowed_actions:
            raise ActionNotAllowed(action)
        if wait.status != "pending":
            if wait.answer_id == answer_id:
                return "already_answered_same_request"          # idempotent replay → 200
            raise AlreadyAnswered(f"wait is {wait.status}")      # different request → 409
        ok = self.store.transition(wait.wait_id, expect="pending", to="answered",
                                   answer=payload, action=action, answered_by=actor,
                                   answered_at=now(), answer_id=answer_id)
        if not ok:                                               # lost the race (timer, or another click)
            w = self.store.get(wait.wait_id)
            if w.answer_id == answer_id:
                return "already_answered_same_request"
            raise AlreadyAnswered(f"wait is {w.status}")
        if wait.timer_ref:
            self.timer.cancel(wait.timer_ref)
        self.notifier.emit(WaitEvent("wait.answered", wait.wait_id, wait.thread_id, extra={"action": action, "by": actor}))
        return self._resume(self.store.get(wait.wait_id))

    # ---- step 3: timer fired
    def on_timer(self, wait_id: str) -> str:
        wait = self.store.get(wait_id)
        if not wait or not self.store.transition(wait_id, expect="pending", to="expired"):
            return "noop_already_terminal"                       # late fire after an answer → harmless
        self.notifier.emit(WaitEvent("wait.expired", wait.wait_id, wait.thread_id))
        wait = self.store.get(wait_id)
        if wait.policy.on_timeout == "resume_default":
            self.store.set_refs(wait_id, answer=wait.policy.default, action="timeout", answered_by="system:timer")
            return self._resume(self.store.get(wait_id))
        self.store.transition(wait_id, expect="expired", to="failed")
        return "failed"

    def _resume(self, wait: Wait) -> str:
        if not self.adapter.still_pending(wait):                 # thread already moved on → mark, don't re-invoke
            self.store.transition(wait.wait_id, expect=wait.status, to="resumed")
            return "already_resumed"
        resume_input = self.adapter.build_resume(wait, wait.answer)
        outcome = self.resumer.resume(wait, resume_input)
        self.store.set_refs(wait.wait_id, resume_attempts=wait.resume_attempts + 1)
        if outcome in ("DIRECT", "NATIVE"):
            self.store.transition(wait.wait_id, expect=wait.status, to="resumed")
        # QUEUED: the consumer marks "resumed" after the framework actually advanced (see lambdas.run_handler)
        return outcome.lower()

    def mark_resumed(self, thread_id: str):
        for w in self.store.find(thread_id=thread_id):
            if w.status in ("answered", "expired") and not self.adapter.still_pending(w):
                self.store.transition(w.wait_id, expect=w.status, to="resumed")
                self.notifier.emit(WaitEvent("wait.resumed", w.wait_id, thread_id))

    # ---- sweeper
    def sweep(self, at: float | None = None):
        now_ = at or now()
        for w in self.store.find(status="pending"):
            if (w.expires_at and not w.timer_ref) or not w.notified_at:
                self.log(f"      [sweeper] completing setup for {w.wait_id}"); self._complete_setup(w)
            elif w.expires_at and now_ > w.expires_at + 60:
                self.log(f"      [sweeper] timer evidently never fired for {w.wait_id}"); self.on_timer(w.wait_id)
        for w in self.store.find(status="answered"):
            if w.resume_attempts and now_ - (w.answered_at or now_) > 30:
                self.log(f"      [sweeper] retrying resume for {w.wait_id}"); self._resume(w)
