"""moto fixtures and the table definition the CDK stack also builds.

`create_wait_table()` is the single source of truth for the schema in tests. The CDK
construct declares the same thing in its own language; `test_store_dynamo.py` asserts the
two agree, so a schema change cannot land in one and not the other.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from agent_wait_aws import DynamoWaitStore
from moto import mock_aws

REGION = "ap-south-1"

TABLE_KEY_SCHEMA = [
    {"AttributeName": "pk", "KeyType": "HASH"},
    {"AttributeName": "sk", "KeyType": "RANGE"},
]

TABLE_ATTRIBUTES = [
    {"AttributeName": "pk", "AttributeType": "S"},
    {"AttributeName": "sk", "AttributeType": "S"},
    {"AttributeName": "gsi1pk", "AttributeType": "S"},
    {"AttributeName": "gsi1sk", "AttributeType": "S"},
    {"AttributeName": "gsi2pk", "AttributeType": "S"},
    {"AttributeName": "gsi2sk", "AttributeType": "N"},
]

TABLE_INDEXES = [
    {
        "IndexName": "gsi1",
        "KeySchema": [
            {"AttributeName": "gsi1pk", "KeyType": "HASH"},
            {"AttributeName": "gsi1sk", "KeyType": "RANGE"},
        ],
        "Projection": {"ProjectionType": "ALL"},
    },
    {
        "IndexName": "gsi2",
        "KeySchema": [
            {"AttributeName": "gsi2pk", "KeyType": "HASH"},
            {"AttributeName": "gsi2sk", "KeyType": "RANGE"},
        ],
        "Projection": {"ProjectionType": "ALL"},
    },
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


def create_wait_table(name: str | None = None) -> str:
    table_name = name or f"agent-wait-{uuid.uuid4().hex[:10]}"
    boto3.client("dynamodb", region_name=REGION).create_table(
        TableName=table_name,
        KeySchema=TABLE_KEY_SCHEMA,
        AttributeDefinitions=TABLE_ATTRIBUTES,
        GlobalSecondaryIndexes=TABLE_INDEXES,
        BillingMode="PAY_PER_REQUEST",
    )
    return table_name


def make_dynamo_store(name: str | None = None) -> DynamoWaitStore:
    return DynamoWaitStore(create_wait_table(name), region_name=REGION)


def make_fifo_queue(name: str = "agent-runs.fifo") -> tuple[str, str]:
    """Returns `(queue_url, queue_arn)`."""
    sqs = boto3.client("sqs", region_name=REGION)
    url = sqs.create_queue(
        QueueName=name, Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "false"}
    )["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return url, arn


def received(queue_url: str, max_messages: int = 10) -> list[dict[str, Any]]:
    import json

    sqs = boto3.client("sqs", region_name=REGION)
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=max_messages)
    return [json.loads(m["Body"]) for m in response.get("Messages", [])]
