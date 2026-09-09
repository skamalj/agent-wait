"""The nouns: `Wait`, `WaitEnvelope`, the dispatch outcomes, and the clock.

Everything here is JSON-shaped on purpose. A wait is written to a store by one process
and read by another, days later, on a different machine; an envelope is emitted onto a
queue and consumed by software we will never see. Nothing may depend on a live Python
object graph.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

from .errors import QuestionTooLarge
from .policy import WaitPolicy

Status = Literal["pending", "answered", "expired", "cancelled", "resumed", "failed"]
Transition = Literal["created", "answered", "expired", "resumed", "cancelled"]
TERMINAL: frozenset[str] = frozenset({"resumed", "cancelled", "failed"})

IgnoreReason = Literal[
    "duplicate",
    "already_answered",
    "token_invalid",
    "expired",
    "not_pending",
    "action_not_allowed",
    "binding_mismatch",
    "parked",
    "unknown_payload",
    "lease_held",
]

MAX_QUESTION_BYTES = 200 * 1024


def _no_refs() -> dict[str, str]:
    """Typed empty-mapping factories. Bare `dict` leaves the value type unknown."""
    return {}


def _no_detail() -> dict[str, Any]:
    return {}


# --------------------------------------------------------------------------- clock
@runtime_checkable
class Clock(Protocol):
    """Injected everywhere so conformance tests can drive three days in a millisecond."""

    def now(self) -> float: ...


class SystemClock:
    def now(self) -> float:
        return time.time()


class FakeClock:
    """A clock the tests own. Shipped rather than kept in a conftest because the
    conformance suite runs against stores that live in other packages."""

    def __init__(self, start: float = 1_760_000_000.0) -> None:
        self._t = start

    def now(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += seconds

    def set(self, when: float) -> None:
        self._t = when


# --------------------------------------------------------------------------- ids and hashing
_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def new_ulid(clock: Clock | None = None) -> str:
    """A ULID: 48 bits of millisecond timestamp then 80 bits of randomness, Crockford
    base32. Lexicographically sortable, which is what makes it useful as a store key."""
    ms = int((clock.now() if clock else time.time()) * 1000)
    value = (ms << 80) | int.from_bytes(secrets.token_bytes(10), "big")
    out = [""] * 26
    for i in range(25, -1, -1):
        out[i] = _CROCKFORD[value & 0x1F]
        value >>= 5
    return "".join(out)


def canonical_json(obj: Any) -> str:
    """One byte-exact rendering of a value, so that a hash of it means something."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False)


def sha256_hex(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def idempotency_key(thread_id: str, interrupt_id: str, checkpoint_id: str) -> str:
    """REQUIREMENTS section 6. The same interrupt at the same checkpoint is the same
    wait, however many times the process crashes and the message is redelivered."""
    return sha256_hex(thread_id, interrupt_id, checkpoint_id)


def binding_hash(question: Any, interrupt_id: str, checkpoint_id: str) -> str:
    """Binds a token to the exact question it answers (pydantic-ai #3274). Edit the
    graph so the question changes, and previously minted tokens stop matching."""
    return sha256_hex(canonical_json(question), interrupt_id, checkpoint_id)


def check_question_size(question: Any) -> None:
    size = len(canonical_json(question).encode("utf-8"))
    if size > MAX_QUESTION_BYTES:
        raise QuestionTooLarge(
            f"question is {size} bytes; the v0.1 inline limit is {MAX_QUESTION_BYTES}. "
            "Store the payload yourself and ask with a reference to it."
        )


def iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


# --------------------------------------------------------------------------- entry point
@dataclass(frozen=True)
class EntryPoint:
    """Where the agent already listens. We do not add a second one (section 16.1);
    this is both the `reply_to` we advertise and the Scheduler's timeout target."""

    kind: Literal["sqs", "lambda", "http"]
    address: str

    def to_dict(self) -> dict[str, str]:
        key = {"sqs": "url", "lambda": "arn", "http": "url"}[self.kind]
        return {"kind": self.kind, key: self.address}


# --------------------------------------------------------------------------- the record
@dataclass(frozen=True)
class Wait:
    """One parked question. Immutable in Python; changed in the store by conditional
    write, then re-read. Use `dataclasses.replace`, never mutation in place."""

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
    notified_at: float | None = None
    announce_refs: Mapping[str, str] = field(default_factory=_no_refs)
    answer: Any = None
    action: str | None = None
    actor: str | None = None
    answered_at: float | None = None
    answer_id: str | None = None
    resume_attempts: int = 0
    last_error: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    expires_at: float | None = None
    version: int = 0
    ttl: float | None = None

    @property
    def binding16(self) -> str:
        return self.binding[:16]

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL

    def to_dict(self) -> dict[str, Any]:
        return {
            "wait_id": self.wait_id,
            "thread_id": self.thread_id,
            "framework": self.framework,
            "interrupt_id": self.interrupt_id,
            "checkpoint_id": self.checkpoint_id,
            "idempotency_key": self.idempotency_key,
            "question": self.question,
            "binding": self.binding,
            "policy": self.policy.to_dict(),
            "status": self.status,
            "notified_at": self.notified_at,
            "announce_refs": dict(self.announce_refs),
            "answer": self.answer,
            "action": self.action,
            "actor": self.actor,
            "answered_at": self.answered_at,
            "answer_id": self.answer_id,
            "resume_attempts": self.resume_attempts,
            "last_error": self.last_error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
            "version": self.version,
            "ttl": self.ttl,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Wait:
        return cls(
            wait_id=data["wait_id"],
            thread_id=data["thread_id"],
            framework=data["framework"],
            interrupt_id=data["interrupt_id"],
            checkpoint_id=data["checkpoint_id"],
            idempotency_key=data["idempotency_key"],
            question=data.get("question"),
            binding=data["binding"],
            policy=WaitPolicy.from_dict(data.get("policy")),
            status=data.get("status", "pending"),
            notified_at=data.get("notified_at"),
            announce_refs=dict(data.get("announce_refs") or {}),
            answer=data.get("answer"),
            action=data.get("action"),
            actor=data.get("actor"),
            answered_at=data.get("answered_at"),
            answer_id=data.get("answer_id"),
            resume_attempts=int(data.get("resume_attempts") or 0),
            last_error=data.get("last_error"),
            created_at=float(data.get("created_at") or 0.0),
            updated_at=float(data.get("updated_at") or 0.0),
            expires_at=data.get("expires_at"),
            version=int(data.get("version") or 0),
            ttl=data.get("ttl"),
        )


@dataclass(frozen=True)
class PendingInterrupt:
    """What a `FrameworkAdapter` hands back from a finished run."""

    interrupt_id: str
    checkpoint_id: str
    question: Any
    policy: WaitPolicy


# --------------------------------------------------------------------------- the envelope
@dataclass(frozen=True)
class WaitEnvelope:
    """The outbound contract (REQUIREMENTS section 7.1). Consumers dedupe on `event_id`."""

    type: str
    event_id: str
    wait_id: str
    thread_id: str
    question: Any
    allowed_actions: tuple[str, ...]
    expires_at: str | None
    token: str
    reply_to: Mapping[str, str]
    correlation: Mapping[str, str] | None = None
    tags: Mapping[str, str] = field(default_factory=_no_refs)
    transition_detail: Mapping[str, Any] = field(default_factory=_no_detail)

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "event_id": self.event_id,
            "wait_id": self.wait_id,
            "thread_id": self.thread_id,
            "question": self.question,
            "allowed_actions": list(self.allowed_actions),
            "expires_at": self.expires_at,
            "token": self.token,
            "reply_to": dict(self.reply_to),
            "correlation": dict(self.correlation) if self.correlation else None,
            "tags": dict(self.tags),
            "transition_detail": dict(self.transition_detail),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), default=str)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> WaitEnvelope:
        return cls(
            type=data["type"],
            event_id=data["event_id"],
            wait_id=data["wait_id"],
            thread_id=data["thread_id"],
            question=data.get("question"),
            allowed_actions=tuple(data.get("allowed_actions") or ()),
            expires_at=data.get("expires_at"),
            token=data.get("token", ""),
            reply_to=dict(data.get("reply_to") or {}),
            correlation=dict(data["correlation"]) if data.get("correlation") else None,
            tags=dict(data.get("tags") or {}),
            transition_detail=dict(data.get("transition_detail") or {}),
        )


# --------------------------------------------------------------------------- dispatch outcomes
@dataclass(frozen=True)
class Start:
    """Invoke the graph with `input`. `input is None` means the message was already
    applied to this thread: re-invoke with None so the framework replays from its own
    checkpoint rather than applying the same input twice."""

    thread_id: str
    input: Any | None
    config: dict[str, Any]


@dataclass(frozen=True)
class Resume:
    thread_id: str
    command: Any
    config: dict[str, Any]
    wait: Wait


@dataclass(frozen=True)
class Ignore:
    """Not an error. Every one of these is a payload the caller should acknowledge,
    except `lease_held`, which the caller should retry."""

    reason: IgnoreReason
    detail: str = ""

    @property
    def should_retry(self) -> bool:
        return self.reason == "lease_held"


@dataclass(frozen=True)
class RegisterResult:
    """Section 16.7. `immediate_resume` is set when a parked answer was waiting for a
    wait that did not exist yet; the run handler loops on it."""

    envelopes: list[WaitEnvelope]
    immediate_resume: Resume | None = None


@dataclass(frozen=True)
class Lease:
    thread_id: str
    owner: str
    expires_at: float


def default_owner_id() -> str:
    """Identifies this process for the purposes of the thread lease."""
    return f"{os.getpid()}-{secrets.token_hex(4)}"
