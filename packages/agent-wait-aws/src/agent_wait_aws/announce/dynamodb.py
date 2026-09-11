"""`DynamoDbAnnounce` -- the question *is* the row.

An announce adapter does not have to be a message broker. It is somewhere the question
lands where whoever answers it will find it, and for an approvals workflow a table is a
good somewhere: a UI queries it for "everything still open" without building a
projection off an event stream.

    announce=[DynamoDbAnnounce("approvals")]

    pk = "THREAD#order-4471"   sk = "WAIT#<question_id>"   status = "open"

That is the adapter's whole job: write the row. Publishing the same question again
overwrites its own row -- `dedupe_key` is stable, so a redelivery is one row, not two.

Nothing here reads the row back, closes it, or sweeps it. Whether you treat this table
as a ledger, how you mark a question answered, and what you do when `expires_at` passes
are yours; `docs/message-formats.md` says what the row contains and what a host is
expected to check. The GSI `by_status` (`status`, `expires_at`) exists so those queries
are cheap. `expires_at` is written as the string `never` when the policy has no timeout,
so a range condition on it never returns those rows as overdue.
"""

from __future__ import annotations

import time
from typing import Any

import boto3
from agent_wait.announce import BaseAnnounce
from agent_wait.model import Transition, WaitEnvelope


class DynamoDbAnnounce(BaseAnnounce):
    name = "dynamodb"

    def __init__(
        self,
        table_name: str,
        *,
        table: Any = None,
        region_name: str | None = None,
        ttl_seconds: int | None = None,
        only: tuple[Transition, ...] | None = None,
    ) -> None:
        super().__init__(only=only)
        self.table_name = table_name
        self._table = table or boto3.resource("dynamodb", region_name=region_name).Table(table_name)
        self._ttl_seconds = ttl_seconds

    def deliver(self, envelope: WaitEnvelope, transition: Transition) -> None:
        item: dict[str, Any] = {
            "pk": f"THREAD#{envelope.thread_id}",
            "sk": f"WAIT#{envelope.question_id}",
            "status": "open",
            "thread_id": envelope.thread_id,
            "question_id": envelope.question_id,
            "question": envelope.question,
            "allowed_actions": list(envelope.allowed_actions),
            "expires_at": envelope.expires_at or "never",  # the GSI range key must exist
            "reply_with": dict(envelope.reply_with),
            "tags": dict(envelope.tags),
            "event_id": envelope.event_id,
        }
        for key in ("default", "answer_ttl", "source", "reply_to"):
            value = getattr(envelope, key)
            if value:
                item[key] = dict(value) if isinstance(value, dict) else value
        if self._ttl_seconds:
            item["ttl"] = int(time.time()) + self._ttl_seconds
        self._table.put_item(Item=item)
