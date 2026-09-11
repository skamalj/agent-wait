"""moto fixtures, and the approvals table the CDK stack also builds.

`create_approvals_table()` is the single source of truth for the schema in tests. The CDK
construct declares the same thing in its own language; `test_announce_dynamodb.py` asserts
the two agree, so a schema change cannot land in one and not the other.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from agent_wait import EntryPoint, WaitEnvelope
from moto import mock_aws

REGION = "ap-south-1"

# One row per question. `status` is what an approvals UI queries on.
TABLE_KEY_SCHEMA = [
    {"AttributeName": "pk", "KeyType": "HASH"},
    {"AttributeName": "sk", "KeyType": "RANGE"},
]

TABLE_ATTRIBUTES = [
    {"AttributeName": "pk", "AttributeType": "S"},
    {"AttributeName": "sk", "AttributeType": "S"},
    {"AttributeName": "status", "AttributeType": "S"},
    {"AttributeName": "expires_at", "AttributeType": "S"},
]

TABLE_INDEXES = [
    {
        "IndexName": "by_status",
        "KeySchema": [
            {"AttributeName": "status", "KeyType": "HASH"},
            {"AttributeName": "expires_at", "KeyType": "RANGE"},
        ],
        "Projection": {"ProjectionType": "ALL"},
    }
]


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make absolutely sure no test can reach a real account."""
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SECURITY_TOKEN", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(name, "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.delenv("AWS_PROFILE", raising=False)


@pytest.fixture(autouse=True)
def aws(aws_credentials: None) -> Iterator[None]:
    with mock_aws():
        yield


def create_approvals_table(name: str | None = None) -> str:
    table_name = name or f"agent-wait-approvals-{uuid.uuid4().hex[:10]}"
    boto3.client("dynamodb", region_name=REGION).create_table(
        TableName=table_name,
        KeySchema=TABLE_KEY_SCHEMA,
        AttributeDefinitions=TABLE_ATTRIBUTES,
        GlobalSecondaryIndexes=TABLE_INDEXES,
        BillingMode="PAY_PER_REQUEST",
    )
    return table_name


def make_fifo_queue(name: str = "agent-runs.fifo") -> tuple[str, str]:
    """Returns `(queue_url, queue_arn)`."""
    sqs = boto3.client("sqs", region_name=REGION)
    url = sqs.create_queue(
        QueueName=name, Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "false"}
    )["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return url, arn


def received(queue_url: str, max_messages: int = 10) -> list[dict[str, Any]]:
    sqs = boto3.client("sqs", region_name=REGION)
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=max_messages)
    return [json.loads(m["Body"]) for m in response.get("Messages", [])]


def envelope(**overrides: Any) -> WaitEnvelope:
    """A representative `wait.created`, as `WaitPublisher` would build it."""
    fields: dict[str, Any] = {
        "type": "wait.created",
        "event_id": "01JEVENT00000000000000001",
        "thread_id": "order-4471",
        "interrupt_id": "a1b2c3d4e5f60718",
        "question": {"kind": "refund_approval", "amount": 41000},
        "allowed_actions": ("approve", "reject"),
        "expires_at": "2099-01-01T00:00:00Z",
        "reply_to": EntryPoint("sqs", "https://sqs.local/q.fifo").to_dict(),
        "reply_with": {
            "thread_id": "order-4471",
            "interrupt_id": "a1b2c3d4e5f60718",
            "answer": None,
        },
        "default": {"action": "reject"},
        "tags": {"approver_group": "finance"},
    }
    fields.update(overrides)
    return WaitEnvelope(**fields)


class Broken:
    """Every call explodes. Stands in for a deleted topic or a revoked permission."""

    def __getattr__(self, name: str) -> Any:
        def boom(*args: Any, **kwargs: Any) -> Any:
            raise RuntimeError(f"{name} is unavailable")

        return boom
