"""A minimal DynamoDB checkpointer for LangGraph.

**This is example infrastructure, not part of agent-wait.** It lives here on purpose:
agent-wait is deliberately checkpointer-agnostic -- it never reads or writes a
checkpoint, and only ever asks the adapter for a `checkpoint_id` -- so which saver you
use is your business, exactly as it is with plain LangGraph.

But the example has to have *some* durable saver, because the whole point of the project
is resuming on a machine that was not running when the question was asked. `InMemorySaver`
would lose the thread the moment the Lambda container went away, and the four scenarios
in REQUIREMENTS section 13 would all be testing nothing. The published options were a
stale third-party 0.1.0 and one that requires Bedrock Session Management, so this is
about a hundred lines against LangGraph's documented `BaseCheckpointSaver` interface
instead.

Layout, in its own table:

    pk  THREAD#<thread_id>#NS#<checkpoint_ns>
    sk  CKPT#<checkpoint_id>                        the checkpoint
    sk  WRITE#<checkpoint_id>#<task_id>#<nnn>       a pending write against it

LangGraph checkpoint ids are time-ordered UUIDs, so `sk` sorts chronologically and
"the latest checkpoint for this thread" is a query with `Limit=1` and no scan.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)


def _partition(config: dict[str, Any]) -> str:
    configurable = config.get("configurable", {})
    return f"THREAD#{configurable['thread_id']}#NS#{configurable.get('checkpoint_ns', '')}"


def _as_bytes(value: Any) -> bytes:
    """boto3 hands Binary attributes back wrapped."""
    return bytes(value) if not isinstance(value, bytes) else value


class DynamoDBSaver(BaseCheckpointSaver[str]):
    def __init__(
        self,
        table_name: str,
        *,
        resource: Any = None,
        region_name: str | None = None,
    ) -> None:
        super().__init__()
        self._resource = resource or boto3.resource("dynamodb", region_name=region_name)
        self.table = self._resource.Table(table_name)

    # ------------------------------------------------------------------ read
    def get_tuple(self, config: dict[str, Any]) -> CheckpointTuple | None:
        partition = _partition(config)
        checkpoint_id = config.get("configurable", {}).get("checkpoint_id")

        if checkpoint_id:
            item = self.table.get_item(Key={"pk": partition, "sk": f"CKPT#{checkpoint_id}"}).get("Item")
        else:
            # Time-ordered ids mean the newest checkpoint is simply the last sort key.
            found = self.table.query(
                KeyConditionExpression=Key("pk").eq(partition) & Key("sk").begins_with("CKPT#"),
                ScanIndexForward=False,
                Limit=1,
            ).get("Items", [])
            item = found[0] if found else None

        return self._to_tuple(config, item) if item else None

    def _to_tuple(self, config: dict[str, Any], item: dict[str, Any]) -> CheckpointTuple:
        configurable = dict(config.get("configurable", {}))
        checkpoint_id = str(item["checkpoint_id"])
        this_config = {
            "configurable": {
                **configurable,
                "thread_id": str(item["thread_id"]),
                "checkpoint_ns": str(item.get("checkpoint_ns", "")),
                "checkpoint_id": checkpoint_id,
            }
        }
        parent_config = None
        if item.get("parent_checkpoint_id"):
            parent_config = {
                "configurable": {
                    **this_config["configurable"],
                    "checkpoint_id": str(item["parent_checkpoint_id"]),
                }
            }
        return CheckpointTuple(
            config=this_config,
            checkpoint=self.serde.loads_typed((str(item["type"]), _as_bytes(item["checkpoint"]))),
            metadata=self.serde.loads_typed((str(item["metadata_type"]), _as_bytes(item["metadata"]))),
            parent_config=parent_config,
            pending_writes=self._pending_writes(str(item["pk"]), checkpoint_id),
        )

    def _pending_writes(self, partition: str, checkpoint_id: str) -> list[tuple[str, str, Any]]:
        items = self.table.query(
            KeyConditionExpression=Key("pk").eq(partition) & Key("sk").begins_with(f"WRITE#{checkpoint_id}#")
        ).get("Items", [])
        return [
            (
                str(item["task_id"]),
                str(item["channel"]),
                self.serde.loads_typed((str(item["type"]), _as_bytes(item["value"]))),
            )
            for item in items
        ]

    def list(
        self,
        config: dict[str, Any] | None,
        *,
        filter: dict[str, Any] | None = None,
        before: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        if config is None:
            return
        condition = Key("pk").eq(_partition(config))
        if before is not None:
            cutoff = before["configurable"]["checkpoint_id"]
            condition = condition & Key("sk").lt(f"CKPT#{cutoff}")
        else:
            condition = condition & Key("sk").begins_with("CKPT#")

        kwargs: dict[str, Any] = {"KeyConditionExpression": condition, "ScanIndexForward": False}
        if limit is not None:
            kwargs["Limit"] = limit
        for item in self.table.query(**kwargs).get("Items", []):
            if str(item["sk"]).startswith("CKPT#"):
                yield self._to_tuple(config, item)

    # ------------------------------------------------------------------ write
    def put(
        self,
        config: dict[str, Any],
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> dict[str, Any]:
        configurable = config.get("configurable", {})
        thread_id = configurable["thread_id"]
        namespace = configurable.get("checkpoint_ns", "")
        checkpoint_id = checkpoint["id"]

        checkpoint_type, checkpoint_bytes = self.serde.dumps_typed(checkpoint)
        metadata_type, metadata_bytes = self.serde.dumps_typed(dict(metadata))

        item: dict[str, Any] = {
            "pk": f"THREAD#{thread_id}#NS#{namespace}",
            "sk": f"CKPT#{checkpoint_id}",
            "thread_id": thread_id,
            "checkpoint_ns": namespace,
            "checkpoint_id": checkpoint_id,
            "type": checkpoint_type,
            "checkpoint": checkpoint_bytes,
            "metadata_type": metadata_type,
            "metadata": metadata_bytes,
        }
        if configurable.get("checkpoint_id"):
            item["parent_checkpoint_id"] = configurable["checkpoint_id"]
        self.table.put_item(Item=item)

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": namespace,
                "checkpoint_id": checkpoint_id,
            }
        }

    def put_writes(
        self,
        config: dict[str, Any],
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        configurable = config.get("configurable", {})
        partition = _partition(config)
        checkpoint_id = configurable["checkpoint_id"]

        with self.table.batch_writer() as batch:
            for index, (channel, value) in enumerate(writes):
                value_type, value_bytes = self.serde.dumps_typed(value)
                batch.put_item(
                    Item={
                        "pk": partition,
                        "sk": f"WRITE#{checkpoint_id}#{task_id}#{index:03d}",
                        "task_id": task_id,
                        "task_path": task_path,
                        "channel": channel,
                        "type": value_type,
                        "value": value_bytes,
                    }
                )

    def delete_thread(self, thread_id: str) -> None:
        items = self.table.query(KeyConditionExpression=Key("pk").eq(f"THREAD#{thread_id}#NS#")).get(
            "Items", []
        )
        with self.table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})


CHECKPOINT_TABLE_KEY_SCHEMA = [
    {"AttributeName": "pk", "KeyType": "HASH"},
    {"AttributeName": "sk", "KeyType": "RANGE"},
]

CHECKPOINT_TABLE_ATTRIBUTES = [
    {"AttributeName": "pk", "AttributeType": "S"},
    {"AttributeName": "sk", "AttributeType": "S"},
]
