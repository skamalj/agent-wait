"""The `AnnounceAdapter` port -- the only thing you are expected to implement.

An announce adapter puts the envelope somewhere. That is all it does. It makes no
decisions, it is never consulted, and its return value is discarded. The contract is one
line long:

    **`announce()` must not raise into the caller.**

If Slack is down, the graph still parked and the run still completes. `CompositeAnnounce`
enforces this rather than trusting it, but an adapter that logs its own failure gives a
far better error message than a stack trace from the composite.

## Anything can be an announcer

Since v0.2 nobody reads state back through this library, so "announce" no longer means
"publish an event". It means *put the question where whoever answers it will find it*.
That can be a topic, a queue, a bus -- or a DynamoDB table, a Redis key, a Postgres row,
a database your UI already queries, a file on disk. `DynamoDbAnnounce` in
`agent-wait-aws` is there to make the point: it is thirty lines, and an approval UI can
be built on a `Query` against it with no message broker anywhere.

Two methods:

    class MyAnnounce:
        name = "mine"

        def supports(self, transition): return True
        def announce(self, envelope, transition): ...

`supports()` lets an adapter opt out cheaply -- a UI that only wants to draw new
questions returns True for `created` and False for `resumed`. Most adapters want both:
`created` is "draw the button", `resumed` is "retract it".
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..model import Transition, WaitEnvelope


@runtime_checkable
class AnnounceAdapter(Protocol):
    name: str

    def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
        """Fire-and-forget. Log failures; never raise."""
        ...

    def supports(self, transition: Transition) -> bool:
        """Which transitions this adapter wants. Called before every announce."""
        ...
