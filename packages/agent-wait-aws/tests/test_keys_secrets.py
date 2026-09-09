"""Signing keys from Secrets Manager, including the rotation that must not break links."""

from __future__ import annotations

import json

import boto3
import pytest
from agent_wait import TokenCodec, TokenInvalid, WaitPolicy
from agent_wait.model import Wait
from agent_wait_aws import SecretsManagerKeyProvider

from conftest import REGION


def put_secret(name: str, document: dict[str, object]) -> str:
    client = boto3.client("secretsmanager", region_name=REGION)
    return client.create_secret(Name=name, SecretString=json.dumps(document))["ARN"]


def a_wait() -> Wait:
    return Wait(
        wait_id="01JWAIT000000000000000001",
        thread_id="t",
        framework="langgraph",
        interrupt_id="i",
        checkpoint_id="c",
        idempotency_key="k",
        question={},
        binding="ab" * 32,
        policy=WaitPolicy(allowed_actions=("approve",)),
    )


def test_keys_are_read_from_the_secret() -> None:
    put_secret("agent-wait/keys", {"current": "k2", "keys": {"k2": "two", "k1": "one"}})
    provider = SecretsManagerKeyProvider("agent-wait/keys", region_name=REGION)

    assert provider.current_kid() == "k2"
    assert provider.keys() == {"k2": b"two", "k1": b"one"}


def test_a_token_minted_before_a_rotation_still_verifies() -> None:
    """The reason the previous key stays in the secret: tokens live for seven days and
    sit in people's inboxes. Dropping the old key invalidates every link already sent."""
    put_secret("agent-wait/keys", {"current": "k1", "keys": {"k1": "one"}})
    before = TokenCodec(SecretsManagerKeyProvider("agent-wait/keys", region_name=REGION))
    token = before.mint(a_wait())

    boto3.client("secretsmanager", region_name=REGION).put_secret_value(
        SecretId="agent-wait/keys",
        SecretString=json.dumps({"current": "k2", "keys": {"k2": "two", "k1": "one"}}),
    )
    after = TokenCodec(SecretsManagerKeyProvider("agent-wait/keys", region_name=REGION))

    assert after.verify(token).wait_id == "01JWAIT000000000000000001"
    assert after.mint(a_wait()).split(".")[1] == "k2"


def test_dropping_the_previous_key_invalidates_old_tokens() -> None:
    put_secret("agent-wait/keys", {"current": "k1", "keys": {"k1": "one"}})
    token = TokenCodec(SecretsManagerKeyProvider("agent-wait/keys", region_name=REGION)).mint(a_wait())

    boto3.client("secretsmanager", region_name=REGION).put_secret_value(
        SecretId="agent-wait/keys", SecretString=json.dumps({"current": "k2", "keys": {"k2": "two"}})
    )
    after = TokenCodec(SecretsManagerKeyProvider("agent-wait/keys", region_name=REGION))

    with pytest.raises(TokenInvalid):
        after.verify(token)


def test_keys_are_cached_between_calls() -> None:
    """One secret fetch per container, not one per message."""
    fetches = {"n": 0}
    document = json.dumps({"current": "k1", "keys": {"k1": "one"}})

    class CountingClient:
        def get_secret_value(self, **kwargs: object) -> dict[str, str]:
            fetches["n"] += 1
            return {"SecretString": document}

    provider = SecretsManagerKeyProvider("s", client=CountingClient())
    for _ in range(5):
        provider.keys()

    assert fetches["n"] == 1

    provider.invalidate()
    provider.keys()
    assert fetches["n"] == 2


@pytest.mark.parametrize(
    "document",
    [
        {"current": "k1"},
        {"keys": {"k1": "one"}, "current": "k9"},
        {"keys": {}, "current": "k1"},
    ],
)
def test_a_malformed_secret_fails_loudly(document: dict[str, object]) -> None:
    put_secret("agent-wait/keys", document)
    provider = SecretsManagerKeyProvider("agent-wait/keys", region_name=REGION)

    with pytest.raises(TokenInvalid):
        provider.keys()


def test_a_secret_that_is_not_json_fails_loudly() -> None:
    boto3.client("secretsmanager", region_name=REGION).create_secret(
        Name="agent-wait/keys", SecretString="not json"
    )
    provider = SecretsManagerKeyProvider("agent-wait/keys", region_name=REGION)

    with pytest.raises(TokenInvalid):
        provider.keys()
