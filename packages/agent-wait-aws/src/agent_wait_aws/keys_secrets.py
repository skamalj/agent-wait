"""Signing keys from Secrets Manager.

The secret is a JSON object naming the current key and holding every key that must still
verify:

    {"current": "k2", "keys": {"k2": "<random>", "k1": "<the previous one>"}}

A flat form is also accepted, where every key other than `current` is a signing key:

    {"current": "k1", "k1": "<random>"}

That is the shape CloudFormation can generate on its own (`generate_string_key` writes
one value into a template and cannot nest), so the stack can create a real random key at
deploy time instead of asking somebody to paste one in afterwards.

Keeping the old key is not optional. Tokens live for seven days and sit in inboxes; a
rotation that drops the previous key invalidates every approval link already sent, and
the failure looks exactly like an attack.

Cached for the life of the Lambda container: fetching a secret on every message would
add a round trip to the hot path and a bill to go with it.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping
from typing import Any

import boto3
from agent_wait.errors import TokenInvalid

_log = logging.getLogger("agent_wait_aws.keys")

DEFAULT_CACHE_SECONDS = 300.0


class SecretsManagerKeyProvider:
    def __init__(
        self,
        secret_id: str,
        *,
        client: Any = None,
        region_name: str | None = None,
        cache_seconds: float = DEFAULT_CACHE_SECONDS,
    ) -> None:
        self.secret_id = secret_id
        self._client = client or boto3.client("secretsmanager", region_name=region_name)
        self._cache_seconds = cache_seconds
        self._cached: tuple[str, dict[str, bytes]] | None = None
        self._fetched_at = 0.0

    def _load(self) -> tuple[str, dict[str, bytes]]:
        now = time.time()
        if self._cached is not None and (now - self._fetched_at) < self._cache_seconds:
            return self._cached

        response = self._client.get_secret_value(SecretId=self.secret_id)
        try:
            document = json.loads(response["SecretString"])
        except (KeyError, ValueError) as err:
            raise TokenInvalid(f"secret {self.secret_id} is not JSON") from err

        raw_keys = document.get("keys")
        if raw_keys is None and isinstance(document, Mapping):
            # The flat form: every entry other than `current` is a signing key.
            raw_keys = {k: v for k, v in document.items() if k != "current"}
        if not isinstance(raw_keys, Mapping) or not raw_keys:
            raise TokenInvalid(f"secret {self.secret_id} has no signing keys")
        current = document.get("current")
        if current not in raw_keys:
            raise TokenInvalid(f"secret {self.secret_id} names a 'current' key it does not hold")

        keys = {str(kid): str(value).encode("utf-8") for kid, value in raw_keys.items()}
        self._cached = (str(current), keys)
        self._fetched_at = now
        # Deliberately logs the key ids and nothing else.
        _log.debug("loaded %d signing keys; current is %s", len(keys), current)
        return self._cached

    def current_kid(self) -> str:
        return self._load()[0]

    def keys(self) -> Mapping[str, bytes]:
        return self._load()[1]

    def invalidate(self) -> None:
        """Force the next call to re-read. For use straight after a rotation."""
        self._cached = None
