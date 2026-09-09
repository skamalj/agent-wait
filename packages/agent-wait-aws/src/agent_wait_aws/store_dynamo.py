"""`DynamoWaitStore` -- one table, and a conditional write behind every transition.

## Why DynamoDB at all

The core needs exactly one hard primitive: a compare-and-set that tells the caller
whether it won. DynamoDB's `ConditionExpression` gives that at any scale and across any
number of Lambda invocations, which is what makes "a human clicked at the same moment the
three-day timer fired" a non-event rather than a double refund.

## The single table (REQUIREMENTS section 16.8)

Four item kinds share one table, distinguished by key prefix:

    pk                       sk                    what
    WAIT#<wait_id>           #                     the wait record
    IDEM#<sha256>            #                     the idempotency marker
    THREAD#<thread_id>       APPLIED#<message_id>  a start message already applied
    LEASE#<thread_id>        #                     the thread lease
    PARKED#<key>             #                     an answer that arrived early

    GSI1  gsi1pk = thread_id, gsi1sk = wait_id       -- "waits on this thread"
    GSI2  gsi2pk = status,    gsi2sk = expires_at    -- "pending waits that are due"

`create()` is a `TransactWriteItems` of the wait *and* its idempotency marker, both
conditional on not existing. Either both land or neither does, so a crash can never
leave a marker without a wait or a wait nobody can find again.

It goes through `resource.meta.client`, which carries boto3's document-layer
serializer -- so items are handed over as plain Python values, exactly like every
`Table` call in this file. Pre-serializing them into `{"S": ...}` wire form makes
boto3 serialize them a second time and the transaction fails with a bare
"unhashable type: dict".

## Two decisions worth naming

**Real attributes, not a JSON blob.** Only the genuinely free-form fields (`question`,
`policy`, `answer`, `announce_refs`) are stored as JSON strings. Everything else is a
first-class attribute, so `transition()` is a true partial `UpdateItem` -- it never reads
the item first, and there is no read-modify-write window for a concurrent writer to slip
through.

**`gsi2sk` is never absent.** A wait with no timeout gets `NEVER` (year 2286) rather than
a missing attribute, so it still appears in GSI2 for the sweeper's "pending but never
announced" query, while never matching a "due before now" query.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import boto3
from agent_wait.model import Lease, Status, Wait
from botocore.exceptions import ClientError

NEVER = 9_999_999_999.0
"""The `gsi2sk` of a wait that can never time out."""

_SK = "#"

# Wait fields that are JSON documents rather than scalars.
_JSON_FIELDS = {"question", "policy", "answer", "announce_refs"}
# Wait fields that are plain scalars, stored under their own names.
_SCALAR_FIELDS = (
    "wait_id",
    "thread_id",
    "framework",
    "interrupt_id",
    "checkpoint_id",
    "idempotency_key",
    "binding",
    "status",
    "notified_at",
    "action",
    "actor",
    "answered_at",
    "answer_id",
    "resume_attempts",
    "last_error",
    "created_at",
    "updated_at",
    "expires_at",
    "version",
    "ttl",
)


def _num(value: float | int) -> Decimal:
    """DynamoDB has no float type; go through the string form to avoid binary drift."""
    return Decimal(str(value))


class DynamoWaitStore:
    def __init__(
        self,
        table_name: str,
        *,
        client: Any = None,
        resource: Any = None,
        region_name: str | None = None,
        applied_ttl_days: int = 7,
    ) -> None:
        self._resource = resource or boto3.resource("dynamodb", region_name=region_name)
        self._client = client or self._resource.meta.client
        self.table_name = table_name
        self.table = self._resource.Table(table_name)
        self.applied_ttl_days = applied_ttl_days

    # ================================================================ serialisation
    @staticmethod
    def _to_item(wait: Wait) -> dict[str, Any]:
        data = wait.to_dict()
        item: dict[str, Any] = {
            "pk": f"WAIT#{wait.wait_id}",
            "sk": _SK,
            "gsi1pk": wait.thread_id,
            "gsi1sk": wait.wait_id,
            "gsi2pk": wait.status,
            "gsi2sk": _num(wait.expires_at if wait.expires_at is not None else NEVER),
        }
        for name in _SCALAR_FIELDS:
            value = data[name]
            if value is None:
                continue
            item[name] = _num(value) if isinstance(value, int | float) else value
        for name in _JSON_FIELDS:
            item[name] = json.dumps(data[name], default=str)
        return item

    @staticmethod
    def _from_item(item: Mapping[str, Any]) -> Wait:
        data: dict[str, Any] = {}
        for name in _SCALAR_FIELDS:
            value = item.get(name)
            data[name] = float(value) if isinstance(value, Decimal) else value
        for name in _JSON_FIELDS:
            raw = item.get(name)
            data[name] = json.loads(raw) if isinstance(raw, str) else None
        return Wait.from_dict(data)

    # ================================================================ waits
    def create(self, wait: Wait) -> tuple[Wait, bool]:
        """Both puts or neither. The marker is what makes a redelivery find the original
        wait instead of minting a second one (rule 1)."""
        try:
            self._client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": self.table_name,
                            "Item": self._to_item(wait),
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    },
                    {
                        "Put": {
                            "TableName": self.table_name,
                            "Item": {
                                "pk": f"IDEM#{wait.idempotency_key}",
                                "sk": _SK,
                                "wait_id": wait.wait_id,
                            },
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    },
                ]
            )
            return wait, True
        except ClientError as err:
            if err.response["Error"]["Code"] != "TransactionCanceledException":
                raise
            existing = self._by_idempotency_key(wait.idempotency_key)
            if existing is None:  # pragma: no cover - would mean a wait_id collision
                raise
            return existing, False

    def _by_idempotency_key(self, key: str) -> Wait | None:
        marker = self.table.get_item(Key={"pk": f"IDEM#{key}", "sk": _SK}).get("Item")
        if not marker:
            return None
        return self.get(str(marker["wait_id"]))

    def get(self, wait_id: str) -> Wait | None:
        item = self.table.get_item(Key={"pk": f"WAIT#{wait_id}", "sk": _SK}).get("Item")
        return self._from_item(item) if item else None

    def find(
        self,
        *,
        status: Status | None = None,
        thread_id: str | None = None,
        notified: bool | None = None,
        due_before: float | None = None,
        limit: int | None = None,
    ) -> list[Wait]:
        if thread_id is not None:
            items = self._query_index(
                "gsi1", "gsi1pk = :v", {":v": thread_id}, limit=None if status else limit
            )
        elif status is not None:
            expression = "gsi2pk = :s"
            values: dict[str, Any] = {":s": status}
            if due_before is not None:
                expression += " AND gsi2sk <= :d"
                values[":d"] = _num(due_before)
            items = self._query_index("gsi2", expression, values, limit=None)
        else:
            # An unindexed query. The table also holds leases, markers and parked
            # answers, so the prefix filter is what keeps them out of `Wait.from_dict`.
            items = self._paginate(
                self.table.scan,
                {
                    "FilterExpression": "begins_with(pk, :prefix)",
                    "ExpressionAttributeValues": {":prefix": "WAIT#"},
                },
                limit=None,
            )

        waits = [self._from_item(item) for item in items]
        if status is not None:
            waits = [w for w in waits if w.status == status]
        if notified is not None:
            waits = [w for w in waits if (w.notified_at is not None) == notified]
        if due_before is not None:
            waits = [w for w in waits if w.expires_at is not None and w.expires_at <= due_before]
        waits.sort(key=lambda w: w.created_at)
        return waits[:limit] if limit is not None else waits

    def _query_index(
        self, index: str, expression: str, values: Mapping[str, Any], *, limit: int | None
    ) -> list[Any]:
        return self._paginate(
            self.table.query,
            {
                "IndexName": index,
                "KeyConditionExpression": expression,
                "ExpressionAttributeValues": dict(values),
            },
            limit=limit,
        )

    @staticmethod
    def _paginate(
        operation: Any, kwargs: Mapping[str, Any], *, limit: int | None, max_pages: int = 100
    ) -> list[Any]:
        """Follow `LastEvaluatedKey`.

        DynamoDB caps a response at 1 MB, so a single call is a *page*, not an answer.
        Reading only the first one would quietly hide waits from the sweeper -- and a
        wait the sweeper never sees is a thread parked forever, which is precisely the
        failure this library exists to prevent. Silent truncation is the worst possible
        way to lose one.
        """
        collected: list[Any] = []
        request = dict(kwargs)
        if limit is not None:
            request["Limit"] = limit
        for _ in range(max_pages):
            response = operation(**request)
            collected.extend(response.get("Items", []))
            cursor = response.get("LastEvaluatedKey")
            if not cursor or (limit is not None and len(collected) >= limit):
                break
            request["ExclusiveStartKey"] = cursor
        return collected

    def transition(self, wait_id: str, *, expect: Status, to: Status, **fields: Any) -> bool:
        """The primitive the whole design rests on. One `UpdateItem`, no prior read."""
        sets = [
            "#status = :to",
            "gsi2pk = :to",
            "#version = #version + :one",
        ]
        names: dict[str, str] = {"#status": "status", "#version": "version"}
        values: dict[str, Any] = {":to": to, ":expect": expect, ":one": 1}

        for index, (name, value) in enumerate(fields.items()):
            placeholder = f":f{index}"
            attribute = f"#f{index}"
            names[attribute] = name
            sets.append(f"{attribute} = {placeholder}")
            if name in _JSON_FIELDS:
                values[placeholder] = json.dumps(value, default=str)
            elif isinstance(value, bool) or value is None:
                values[placeholder] = value
            elif isinstance(value, int | float):
                values[placeholder] = _num(value)
            else:
                values[placeholder] = value

        try:
            self.table.update_item(
                Key={"pk": f"WAIT#{wait_id}", "sk": _SK},
                UpdateExpression="SET " + ", ".join(sets),
                ConditionExpression="attribute_exists(pk) AND #status = :expect",
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=values,
            )
            return True
        except ClientError as err:
            if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False  # somebody else got there first; not an error
            raise

    def set_fields(self, wait_id: str, **fields: Any) -> None:
        if not fields:
            return
        sets: list[str] = []
        names: dict[str, str] = {}
        values: dict[str, Any] = {}
        for index, (name, value) in enumerate(fields.items()):
            placeholder = f":f{index}"
            attribute = f"#f{index}"
            names[attribute] = name
            sets.append(f"{attribute} = {placeholder}")
            if name in _JSON_FIELDS:
                values[placeholder] = json.dumps(value, default=str)
            elif isinstance(value, bool) or value is None:
                values[placeholder] = value
            elif isinstance(value, int | float):
                values[placeholder] = _num(value)
            else:
                values[placeholder] = value
        try:
            self.table.update_item(
                Key={"pk": f"WAIT#{wait_id}", "sk": _SK},
                UpdateExpression="SET " + ", ".join(sets),
                ConditionExpression="attribute_exists(pk)",
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=values,
            )
        except ClientError as err:
            if err.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise

    # ================================================================ applied messages
    def record_applied(self, thread_id: str, message_id: str, *, ttl: float | None = None) -> bool:
        item: dict[str, Any] = {"pk": f"THREAD#{thread_id}", "sk": f"APPLIED#{message_id}"}
        if ttl is not None:
            item["ttl"] = _num(int(ttl))
        try:
            self.table.put_item(
                Item=item, ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)"
            )
            return True
        except ClientError as err:
            if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    # ================================================================ leases
    def acquire_lease(self, thread_id: str, owner: str, *, expires_at: float, now: float) -> bool:
        """Free, expired, or already ours. Anything else and we back off."""
        try:
            self.table.put_item(
                Item={
                    "pk": f"LEASE#{thread_id}",
                    "sk": _SK,
                    "owner": owner,
                    "lease_expires_at": _num(expires_at),
                    "ttl": _num(int(expires_at) + 3600),
                },
                ConditionExpression=(
                    "attribute_not_exists(pk) OR #owner = :owner OR lease_expires_at <= :now"
                ),
                ExpressionAttributeNames={"#owner": "owner"},
                ExpressionAttributeValues={":owner": owner, ":now": _num(now)},
            )
            return True
        except ClientError as err:
            if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def refresh_lease(self, thread_id: str, owner: str, *, expires_at: float) -> bool:
        try:
            self.table.update_item(
                Key={"pk": f"LEASE#{thread_id}", "sk": _SK},
                UpdateExpression="SET lease_expires_at = :e, #ttl = :t",
                ConditionExpression="#owner = :owner",
                ExpressionAttributeNames={"#owner": "owner", "#ttl": "ttl"},
                ExpressionAttributeValues={
                    ":e": _num(expires_at),
                    ":t": _num(int(expires_at) + 3600),
                    ":owner": owner,
                },
            )
            return True
        except ClientError as err:
            if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def release_lease(self, thread_id: str, owner: str) -> None:
        try:
            self.table.delete_item(
                Key={"pk": f"LEASE#{thread_id}", "sk": _SK},
                ConditionExpression="#owner = :owner",
                ExpressionAttributeNames={"#owner": "owner"},
                ExpressionAttributeValues={":owner": owner},
            )
        except ClientError as err:
            if err.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise  # releasing a lease we do not hold is a no-op, not a failure

    def get_lease(self, thread_id: str) -> Lease | None:
        item = self.table.get_item(Key={"pk": f"LEASE#{thread_id}", "sk": _SK}).get("Item")
        if not item:
            return None
        return Lease(thread_id, str(item["owner"]), float(item["lease_expires_at"]))

    # ================================================================ parked answers
    def park_answer(self, key: str, payload: Mapping[str, Any], *, ttl: float | None = None) -> None:
        item: dict[str, Any] = {
            "pk": f"PARKED#{key}",
            "sk": _SK,
            "payload": json.dumps(dict(payload), default=str),
        }
        if ttl is not None:
            item["ttl"] = _num(int(ttl))
        try:
            self.table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk)")
        except ClientError as err:
            if err.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise  # first answer in wins; a second one for the same wait is dropped

    def take_parked_answer(self, key: str) -> dict[str, Any] | None:
        """Read-and-delete, so a parked answer is applied at most once."""
        try:
            response = self.table.delete_item(
                Key={"pk": f"PARKED#{key}", "sk": _SK},
                ConditionExpression="attribute_exists(pk)",
                ReturnValues="ALL_OLD",
            )
        except ClientError as err:
            if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return None
            raise
        item = response.get("Attributes")
        if not item:
            return None
        loaded: dict[str, Any] = json.loads(str(item["payload"]))
        return loaded
