"""In-memory `WaitStore`. The reference implementation of the semantics.

Every other store is judged against this one: the conformance suite runs the same
conformance rules over all of them. Deep-copies on the way in and out, so a caller holding a
`Wait` cannot reach through it and change the store -- which is exactly the isolation a
real database would give and exactly the bug an in-memory store invites.

Thread-safe under a single lock. That is enough: the concurrency this store has to model
is two Lambda invocations racing, and each of those is a separate process anyway.
"""

from __future__ import annotations

import copy
import threading
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from ..model import Lease, Status, Wait


class InMemoryWaitStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._waits: dict[str, Wait] = {}
        self._by_idem: dict[str, str] = {}
        self._applied: dict[tuple[str, str], float | None] = {}
        self._leases: dict[str, Lease] = {}
        self._parked: dict[str, dict[str, Any]] = {}

    # -- waits ---------------------------------------------------------------
    def create(self, wait: Wait) -> tuple[Wait, bool]:
        with self._lock:
            existing_id = self._by_idem.get(wait.idempotency_key)
            if existing_id is not None:
                return copy.deepcopy(self._waits[existing_id]), False
            self._waits[wait.wait_id] = copy.deepcopy(wait)
            self._by_idem[wait.idempotency_key] = wait.wait_id
            return copy.deepcopy(wait), True

    def get(self, wait_id: str) -> Wait | None:
        with self._lock:
            found = self._waits.get(wait_id)
            return copy.deepcopy(found) if found is not None else None

    def find(
        self,
        *,
        status: Status | None = None,
        thread_id: str | None = None,
        notified: bool | None = None,
        due_before: float | None = None,
        limit: int | None = None,
    ) -> list[Wait]:
        with self._lock:
            out: list[Wait] = []
            for wait in self._waits.values():
                if status is not None and wait.status != status:
                    continue
                if thread_id is not None and wait.thread_id != thread_id:
                    continue
                if notified is not None and (wait.notified_at is not None) != notified:
                    continue
                if due_before is not None and not (
                    wait.expires_at is not None and wait.expires_at <= due_before
                ):
                    continue
                out.append(copy.deepcopy(wait))
                if limit is not None and len(out) >= limit:
                    break
            out.sort(key=lambda w: w.created_at)
            return out

    def transition(self, wait_id: str, *, expect: Status, to: Status, **fields: Any) -> bool:
        with self._lock:
            wait = self._waits.get(wait_id)
            if wait is None or wait.status != expect:
                return False
            self._waits[wait_id] = replace(wait, status=to, version=wait.version + 1, **fields)
            return True

    def set_fields(self, wait_id: str, **fields: Any) -> None:
        with self._lock:
            wait = self._waits.get(wait_id)
            if wait is None:
                return
            self._waits[wait_id] = replace(wait, **fields)

    # -- applied messages ----------------------------------------------------
    def record_applied(self, thread_id: str, message_id: str, *, ttl: float | None = None) -> bool:
        with self._lock:
            key = (thread_id, message_id)
            if key in self._applied:
                return False
            self._applied[key] = ttl
            return True

    # -- leases --------------------------------------------------------------
    def acquire_lease(self, thread_id: str, owner: str, *, expires_at: float, now: float) -> bool:
        with self._lock:
            held = self._leases.get(thread_id)
            if held is not None and held.owner != owner and held.expires_at > now:
                return False
            self._leases[thread_id] = Lease(thread_id, owner, expires_at)
            return True

    def refresh_lease(self, thread_id: str, owner: str, *, expires_at: float) -> bool:
        with self._lock:
            held = self._leases.get(thread_id)
            if held is None or held.owner != owner:
                return False
            self._leases[thread_id] = Lease(thread_id, owner, expires_at)
            return True

    def release_lease(self, thread_id: str, owner: str) -> None:
        with self._lock:
            held = self._leases.get(thread_id)
            if held is not None and held.owner == owner:
                del self._leases[thread_id]

    def get_lease(self, thread_id: str) -> Lease | None:
        with self._lock:
            return self._leases.get(thread_id)

    # -- parked answers ------------------------------------------------------
    def park_answer(self, key: str, payload: Mapping[str, Any], *, ttl: float | None = None) -> None:
        with self._lock:
            self._parked.setdefault(key, dict(payload))

    def take_parked_answer(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            return self._parked.pop(key, None)
