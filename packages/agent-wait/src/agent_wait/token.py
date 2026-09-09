"""Signed, single-wait credentials (REQUIREMENTS section 11).

    aw1.<kid>.<wait_id>.<exp>.<binding16>.<actions>.<mac>

A token authorises answering *one* wait, with *these* actions, before *that* instant.
It never identifies a person: `actor` in the answer envelope is informational, and
authentication is the entry point's job (section 16.5).

Two properties matter more than the format:

* **The store is the truth.** A token's `exp` is its own 7-day lifetime and has nothing
  to do with the wait's timeout. A token can be perfectly valid for a wait that was
  answered an hour ago; `dispatch()` will still refuse it.
* **Verification touches no I/O.** A tampered token is rejected before the store is
  ever read (rule 8), so a stream of forged tokens costs us a HMAC each and nothing else.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from .errors import TokenExpired, TokenInvalid
from .model import Clock, SystemClock, Wait

DEFAULT_TOKEN_TTL = 7 * 86400
_MAC_CHARS = 27
_PREFIX = "aw1"


@dataclass(frozen=True)
class TokenClaims:
    wait_id: str
    exp: int
    binding16: str
    actions: tuple[str, ...]


class KeyProvider(Protocol):
    """Supplies signing keys. Verification must accept the current key *and* the
    previous one, so that a rotation does not invalidate tokens already in inboxes."""

    def current_kid(self) -> str: ...
    def keys(self) -> Mapping[str, bytes]: ...


class StaticKeyProvider:
    """Keys handed over directly. Used by tests and by anything that has already
    fetched its secrets."""

    def __init__(self, keys: Mapping[str, bytes], current: str) -> None:
        if current not in keys:
            raise ValueError(f"current kid {current!r} is not among the supplied keys")
        self._keys = dict(keys)
        self._current = current

    def current_kid(self) -> str:
        return self._current

    def keys(self) -> Mapping[str, bytes]:
        return self._keys


class EnvKeyProvider:
    """Keys from the environment, for local runs and tests.

    `AGENT_WAIT_SIGNING_KEY` / `AGENT_WAIT_SIGNING_KID` are the current pair;
    `AGENT_WAIT_PREVIOUS_KEY` / `AGENT_WAIT_PREVIOUS_KID` keep a rotated-out key
    verifiable. Keys are read as UTF-8 bytes; nothing is logged.
    """

    def __init__(self, prefix: str = "AGENT_WAIT") -> None:
        self._prefix = prefix

    def current_kid(self) -> str:
        return os.environ.get(f"{self._prefix}_SIGNING_KID", "k1")

    def keys(self) -> Mapping[str, bytes]:
        key = os.environ.get(f"{self._prefix}_SIGNING_KEY")
        if not key:
            raise TokenInvalid(f"{self._prefix}_SIGNING_KEY is not set")
        out = {self.current_kid(): key.encode("utf-8")}
        prev_kid = os.environ.get(f"{self._prefix}_PREVIOUS_KID")
        prev_key = os.environ.get(f"{self._prefix}_PREVIOUS_KEY")
        if prev_kid and prev_key:
            out.setdefault(prev_kid, prev_key.encode("utf-8"))
        return out


class TokenCodec:
    """Mints and verifies wait tokens."""

    def __init__(
        self,
        keys: KeyProvider,
        *,
        ttl_seconds: int = DEFAULT_TOKEN_TTL,
        clock: Clock | None = None,
    ) -> None:
        self._keys = keys
        self.ttl_seconds = ttl_seconds
        self._clock: Clock = clock or SystemClock()

    # ---------------------------------------------------------------- internals
    def _mac(self, kid: str, body: str) -> str:
        keys = self._keys.keys()
        if kid not in keys:
            raise TokenInvalid("unknown key id")
        digest = hmac.new(keys[kid], body.encode("utf-8"), hashlib.sha256).digest()
        return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")[:_MAC_CHARS]

    # ---------------------------------------------------------------- public
    def mint(self, wait: Wait, *, exp: int | None = None) -> str:
        kid = self._keys.current_kid()
        expiry = exp if exp is not None else int(self._clock.now()) + self.ttl_seconds
        actions = "+".join(wait.policy.allowed_actions)
        body = f"{_PREFIX}.{kid}.{wait.wait_id}.{expiry}.{wait.binding16}.{actions}"
        return f"{body}.{self._mac(kid, body)}"

    def verify(self, token: str) -> TokenClaims:
        """Raises `TokenInvalid` or `TokenExpired`. Performs no I/O."""
        # Same trust boundary as dispatch(): this string came from the outside world.
        if not isinstance(token, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TokenInvalid("token must be a string")
        parts = token.split(".")
        if len(parts) != 7:
            raise TokenInvalid("malformed token")
        prefix, kid, wait_id, exp_raw, binding16, actions_raw, mac = parts
        if prefix != _PREFIX:
            raise TokenInvalid("unknown token version")
        available = self._keys.keys()
        if kid not in available:
            raise TokenInvalid("unknown key id")
        try:
            exp = int(exp_raw)
        except ValueError as err:
            raise TokenInvalid("bad expiry") from err
        body = token.rsplit(".", 1)[0]
        if not hmac.compare_digest(self._mac(kid, body), mac):
            raise TokenInvalid("bad signature")
        if self._clock.now() > exp:
            raise TokenExpired("token expired")
        return TokenClaims(
            wait_id=wait_id,
            exp=exp,
            binding16=binding16,
            actions=tuple(a for a in actions_raw.split("+") if a),
        )
