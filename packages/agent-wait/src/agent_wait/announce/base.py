"""The announce port: a `Protocol` to satisfy, and a base class that satisfies it for you.

An announce adapter puts the envelope somewhere. That is all it does. It makes no
decisions, it is never consulted, and its return value is discarded. The contract is one
line long:

    **`announce()` must not raise into the caller.**

If Slack is down, the graph still parked and the run still completes.

## Writing one

Subclass `BaseAnnounce` and implement `deliver()`:

    class RedisAnnounce(BaseAnnounce):
        name = "redis"

        def __init__(self, client, **kw):
            super().__init__(**kw)
            self.client = client

        def deliver(self, envelope, transition):
            self.client.set(envelope.dedupe_key, envelope.to_json())

That is a complete, contract-conforming adapter. The base class wraps `deliver()` so an
exception becomes a log line rather than a failed run, and gives you `only=` so a caller
can restrict the adapter to `("created",)` without you writing the filter.

You do not *have* to subclass. `AnnounceAdapter` is a `Protocol`: any object with `name`,
`supports()` and `announce()` is accepted, and `CompositeAnnounce` contains its failures
either way. The base class exists so that the common case is the correct case by default.

## Anything can be an announcer

Nothing reads state back through this library, so "announce" does not mean "publish an
event". It means *put the question where whoever answers it will find it*.
That can be a topic, a queue, a bus -- or a DynamoDB table, a Redis key, a Postgres row,
a database your UI already queries, a file on disk. `DynamoDbAnnounce` in
`agent-wait-aws` is there to make the point.

`supports()` lets an adapter opt out cheaply: a UI that only wants to draw new questions
takes `only=("created",)`. Most adapters want both -- `created` is "draw the button",
`resumed` is "retract it".
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Protocol, runtime_checkable

from ..model import Transition, WaitEnvelope


@runtime_checkable
class AnnounceAdapter(Protocol):
    """What `WaitPublisher` accepts. Structural: subclassing is not required."""

    name: str

    def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
        """Fire-and-forget. Log failures; never raise."""
        ...

    def supports(self, transition: Transition) -> bool:
        """Which transitions this adapter wants. Called before every announce."""
        ...


class BaseAnnounce(ABC):
    """Implements the contract once. Subclasses implement `deliver()` and nothing else.

    `name` is used in log lines and should be set as a class attribute.
    """

    name: str = "base"

    def __init__(self, *, only: tuple[Transition, ...] | None = None) -> None:
        self._only = only
        self._log = logging.getLogger(f"agent_wait.announce.{self.name}")

    def supports(self, transition: Transition) -> bool:
        return self._only is None or transition in self._only

    def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
        """The contract, enforced. Do not override this; override `deliver()`."""
        try:
            self.deliver(envelope, transition)
        except Exception:
            # A backend being unreachable must not fail a run that has already parked.
            self._log.exception(
                "%s failed for interrupt %s (%s)", type(self).__name__, envelope.question_id, transition
            )

    @abstractmethod
    def deliver(self, envelope: WaitEnvelope, transition: Transition) -> None:
        """Put the envelope where it goes. Raise freely; the base class contains it."""
        ...
