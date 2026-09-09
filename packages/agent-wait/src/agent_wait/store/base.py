"""The `WaitStore` port.

Four kinds of row live behind this one interface (REQUIREMENTS section 6): the waits
themselves, the applied-message set per thread, the thread leases, and parked answers.
They share a store because they share a transaction boundary in spirit -- and because
on DynamoDB they share a table.

The whole design rests on one primitive: **a conditional write that tells you whether
you won**. `transition()` returns a bool rather than raising, and every state change in
the runtime goes through it. Anything that cannot offer compare-and-set cannot be a
`WaitStore`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from ..model import Lease, Status, Wait

APPLIED_TTL = 7 * 86400
PARKED_TTL = 7 * 86400
DEFAULT_LEASE_SECONDS = 15 * 60


class WaitStore(Protocol):
    """Cloud-neutral by construction: no method here implies DynamoDB, SQL or anything
    else. `agent-wait-aws` and the two built-in stores implement exactly this."""

    # -- waits ---------------------------------------------------------------
    def create(self, wait: Wait) -> tuple[Wait, bool]:
        """Idempotent on `wait.idempotency_key`.

        Returns `(record, created)`. On conflict returns the *existing* record and
        `False` -- never a second wait for the same interrupt at the same checkpoint
        (rule 1).
        """
        ...

    def get(self, wait_id: str) -> Wait | None: ...

    def find(
        self,
        *,
        status: Status | None = None,
        thread_id: str | None = None,
        notified: bool | None = None,
        due_before: float | None = None,
        limit: int | None = None,
    ) -> list[Wait]:
        """Query. `notified=False` selects waits whose `created` announce never landed;
        `due_before` selects waits whose `expires_at` has passed. Both are what the
        sweeper needs (rule 10)."""
        ...

    def transition(self, wait_id: str, *, expect: Status, to: Status, **fields: Any) -> bool:
        """Compare-and-set on `status`. `True` if this caller made the change.

        `False` is not an error: it is how the loser of a race finds out (rule 2), and
        the caller re-reads to decide what to report.
        """
        ...

    def set_fields(self, wait_id: str, **fields: Any) -> None:
        """Unconditional field update for things that are not status transitions
        (`notified_at`, `announce_refs`, `resume_attempts`, `last_error`)."""
        ...

    # -- applied messages ----------------------------------------------------
    def record_applied(self, thread_id: str, message_id: str, *, ttl: float | None = None) -> bool:
        """Conditional put. `True` if this message had not been applied to this thread
        before; `False` if it is a redelivery."""
        ...

    # -- thread leases -------------------------------------------------------
    def acquire_lease(self, thread_id: str, owner: str, *, expires_at: float, now: float) -> bool:
        """Take the lease if it is free, expired, or already ours. `False` means someone
        else holds it and has not timed out."""
        ...

    def refresh_lease(self, thread_id: str, owner: str, *, expires_at: float) -> bool: ...

    def release_lease(self, thread_id: str, owner: str) -> None:
        """Release only if we hold it. Releasing someone else's lease is a no-op."""
        ...

    def get_lease(self, thread_id: str) -> Lease | None: ...

    # -- parked answers ------------------------------------------------------
    def park_answer(self, key: str, payload: Mapping[str, Any], *, ttl: float | None = None) -> None:
        """An answer that arrived before its wait existed. Stored, never dropped
        (rule 5)."""
        ...

    def take_parked_answer(self, key: str) -> dict[str, Any] | None:
        """Read-and-delete, so the answer is applied at most once."""
        ...
