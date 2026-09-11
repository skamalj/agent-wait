"""`DynamoDbAnnounce` -- the question *is* the row.

Here to make a point that only became true in v0.2: now that nothing reads state back
through this library, an "announce adapter" does not have to be a message broker. It is
just somewhere the question lands where whoever answers it will find it. A table is a
perfectly good somewhere -- and for an approval queue it is a better one, because a UI
can `Query` it for "everything still open" without anybody having to build a projection
off an event stream first.

    announce=[DynamoDbAnnounce("approvals")]

    pk = "THREAD#order-4471"   sk = "WAIT#<interrupt_id>"
    status = "open" | "closed"

`created` writes the row; `resumed` marks it closed rather than deleting it, so the row
is still there when someone asks why the button stopped working. Both are idempotent:
the same interrupt republished after a crash overwrites its own row with identical
content, which is the whole reason `dedupe_key` is stable.

A GSI on `status` gives you "every open approval", which is the query an approvals UI
actually wants. The CDK stack in this package creates it.
"""

from __future__ import annotations

import logging
from typing import Any

import boto3
from agent_wait.model import Transition, WaitEnvelope

_log = logging.getLogger("agent_wait_aws.announce.dynamodb")


class DynamoDbAnnounce:
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
        self.table_name = table_name
        self._table = table or boto3.resource("dynamodb", region_name=region_name).Table(table_name)
        self._ttl_seconds = ttl_seconds
        self._only = only

    def supports(self, transition: Transition) -> bool:
        return self._only is None or transition in self._only

    def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
        try:
            if transition == "resumed":
                self._close(envelope)
            else:
                self._open(envelope)
        except Exception:
            # The contract, same as every other adapter: log and return. A table being
            # unreachable must not fail a run that has already parked.
            _log.exception("DynamoDbAnnounce failed for interrupt %s (%s)", envelope.interrupt_id, transition)

    def _open(self, envelope: WaitEnvelope) -> None:
        item: dict[str, Any] = {
            "pk": f"THREAD#{envelope.thread_id}",
            "sk": f"WAIT#{envelope.interrupt_id}",
            "status": "open",
            "thread_id": envelope.thread_id,
            "interrupt_id": envelope.interrupt_id,
            "question": envelope.question,
            "allowed_actions": list(envelope.allowed_actions),
            "expires_at": envelope.expires_at,
            "reply_with": dict(envelope.reply_with),
            "tags": dict(envelope.tags),
            "event_id": envelope.event_id,
        }
        if envelope.default is not None:
            item["default"] = envelope.default
        if envelope.reply_to:
            item["reply_to"] = dict(envelope.reply_to)
        if self._ttl_seconds:
            item["ttl"] = int(_now()) + self._ttl_seconds
        self._table.put_item(Item=item)

    def _close(self, envelope: WaitEnvelope) -> None:
        """Mark closed, and only if the row is there.

        The condition matters: a `resumed` for an interrupt this table never saw
        (announced by a different adapter, or written before this adapter was added)
        should leave nothing behind. Creating a closed row for a question nobody was
        ever asked would put a phantom in the approvals UI's history.
        """
        self._table.update_item(
            Key={"pk": f"THREAD#{envelope.thread_id}", "sk": f"WAIT#{envelope.interrupt_id}"},
            UpdateExpression="SET #s = :closed",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":closed": "closed"},
            ConditionExpression="attribute_exists(pk)",
        )


def _now() -> float:
    import time

    return time.time()
