"""The data that crosses the boundary: what comes out of a run, and what goes out to
the world.

`WaitEnvelope` is the contract. Everything else in this repository is an implementation
detail; that one is not. It is specified in `docs/message-formats.md`, and a team that
has read only that file should be able to write a working consumer.
"""

from __future__ import annotations

import json
import secrets
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from .errors import QuestionTooLarge
from .policy import WaitPolicy

Transition = Literal["created", "resumed"]
"""Only two things can be said about a wait now.

`created` -- the graph is parked on this question, here is everything you need to answer
it. `resumed` -- the graph has moved past it; close the ticket, retract the Slack button.

v0.1 also had `answered`, `expired` and `cancelled`. All three were transitions of a
record this library no longer keeps: they described the *library's* opinion about an
answer it had accepted. Since the answer never reaches us, only the graph's own state
can be reported, and the graph knows exactly two things -- parked, or not.
"""

# 256 KB is the smallest of the caps on the way out (SNS, SQS and EventBridge all sit
# there). Leave headroom for the rest of the envelope.
MAX_QUESTION_BYTES = 200 * 1024

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


class Clock(Protocol):
    def now(self) -> float: ...


class SystemClock:
    def now(self) -> float:
        return time.time()


class FakeClock:
    """A clock tests can drive. Nothing in the library reads the wall clock directly."""

    def __init__(self, start: float = 1_760_000_000.0) -> None:
        self._now = start

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


def new_ulid(clock: Clock | None = None) -> str:
    """A ULID: 48 bits of millisecond timestamp then 80 bits of randomness, Crockford
    base32. Lexicographically sortable, which is what makes it useful as a key."""
    ms = int((clock.now() if clock else time.time()) * 1000)
    value = (ms << 80) | int.from_bytes(secrets.token_bytes(10), "big")
    out = [""] * 26
    for i in range(25, -1, -1):
        out[i] = _CROCKFORD[value & 0x1F]
        value >>= 5
    return "".join(out)


def canonical_json(obj: Any) -> str:
    """One byte-exact rendering of a value, so a size check or a hash of it means
    something."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False)


def check_question_size(question: Any) -> None:
    size = len(canonical_json(question).encode("utf-8"))
    if size > MAX_QUESTION_BYTES:
        raise QuestionTooLarge(
            f"question is {size} bytes; the limit is {MAX_QUESTION_BYTES}. "
            "Publish a reference (an id, an S3 key) and let the consumer fetch the rest."
        )


def iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _no_str_map() -> dict[str, str]:
    """A typed factory: a bare `dict` leaves the field's value type unknown."""
    return {}


@dataclass(frozen=True)
class EntryPoint:
    """Where the agent listens, if you want consumers told.

    Optional. agent-wait builds no return leg, so it never needs this itself; it is
    published as `reply_to` purely as a hint, so that a consumer you write does not have
    to hardcode an address that differs between environments."""

    kind: Literal["sqs", "lambda", "http"]
    address: str

    def to_dict(self) -> dict[str, str]:
        key = {"sqs": "url", "lambda": "arn", "http": "url"}[self.kind]
        return {"kind": self.kind, key: self.address}


@dataclass(frozen=True)
class PendingInterrupt:
    """One question a thread is parked on. What a `FrameworkAdapter` hands back."""

    interrupt_id: str
    question: Any
    policy: WaitPolicy
    asked_at: float | None = None
    """When the framework checkpointed this interrupt, if it can say.

    This is what `expires_at` is measured from. It has to come from the checkpoint
    rather than from the clock at publish time, because the same question is republished
    whenever a start message is redelivered -- and a deadline recomputed as `now +
    timeout` on each republish would walk forward forever, which is precisely the
    failure a timeout exists to prevent.
    """


@dataclass(frozen=True)
class WaitEnvelope:
    """The outbound contract. One per interrupt, per transition."""

    type: str
    event_id: str
    thread_id: str
    interrupt_id: str
    question: Any
    allowed_actions: tuple[str, ...]
    expires_at: str | None
    reply_with: Mapping[str, Any]
    reply_to: Mapping[str, str] | None = None
    default: Any = None
    correlation: Mapping[str, str] | None = None
    tags: Mapping[str, str] = field(default_factory=_no_str_map)

    @property
    def dedupe_key(self) -> str:
        """What a consumer -- or an adapter with a natural key -- should dedupe on.

        Not `event_id`: that is fresh per publish, so a redelivered start message
        republishing the same question would look like a second question. LangGraph
        guarantees `interrupt_id` is stable across re-entry and resume-from-checkpoint
        (verified in `test_spike_langgraph.py`), which makes this key stable for exactly
        as long as the question is.
        """
        return f"{self.type}:{self.interrupt_id}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "event_id": self.event_id,
            "thread_id": self.thread_id,
            "interrupt_id": self.interrupt_id,
            "question": self.question,
            "allowed_actions": list(self.allowed_actions),
            "expires_at": self.expires_at,
            "default": self.default,
            "reply_to": dict(self.reply_to) if self.reply_to else None,
            "reply_with": dict(self.reply_with),
            "correlation": dict(self.correlation) if self.correlation else None,
            "tags": dict(self.tags),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str, ensure_ascii=False)
