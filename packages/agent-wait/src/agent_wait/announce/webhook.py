"""`WebhookAnnounce` -- POST the envelope to a URL.

    announce=[WebhookAnnounce("https://approvals.internal/hooks/agent-wait",
                              secret=b"shared-with-the-receiver")]

Stdlib only, so it lives in the core package with no new dependency. One POST per
transition, JSON body, three headers a receiver can route or verify on:

    Content-Type:              application/json
    X-Agent-Wait-Event:        wait.created | wait.resumed
    X-Agent-Wait-Dedupe-Key:   wait.created:<question_id>
    X-Agent-Wait-Signature:    sha256=<hex>          (only when `secret` is given)

## Signing

The signature is `HMAC-SHA256(secret, raw body)`, hex-encoded, in the same shape GitHub
and Stripe use. It answers one question for the receiver -- *did this come from the
agent?* -- and nothing else. It is not a credential for answering; the library has no
inbound path and nothing here creates one. `verify_signature()` is the
receiver's half, and is a pure function so it can be copied into a service that does not
install this package.

## Failure

A non-2xx response raises, and `BaseAnnounce` turns that into a log line: the graph that
just parked stays parked. There is no retry -- `republish()` is the retry, same as every
other adapter -- so a receiver that was down gets the question again the next time the
thread is re-invoked, with the same dedupe key.

The timeout is deliberately short. This runs inside the agent's own invocation, after the
graph has already done its work; a slow receiver must not turn a two-second run into a
thirty-second one.
"""

from __future__ import annotations

import hashlib
import hmac
import urllib.error
import urllib.request
from collections.abc import Mapping

from ..model import Transition, WaitEnvelope
from .base import BaseAnnounce

SIGNATURE_HEADER = "X-Agent-Wait-Signature"
EVENT_HEADER = "X-Agent-Wait-Event"
DEDUPE_HEADER = "X-Agent-Wait-Dedupe-Key"


def sign(secret: bytes, body: bytes) -> str:
    return "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()


def verify_signature(secret: bytes, body: bytes, header: str | None) -> bool:
    """The receiver's half. Constant-time; a missing or malformed header is False."""
    if not header:
        return False
    return hmac.compare_digest(sign(secret, body), header)


class WebhookAnnounce(BaseAnnounce):
    name = "webhook"

    def __init__(
        self,
        url: str,
        *,
        secret: bytes | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float = 5.0,
        only: tuple[Transition, ...] | None = None,
    ) -> None:
        super().__init__(only=only)
        self.url = url
        self._secret = secret
        self._headers = dict(headers or {})
        self._timeout = timeout

    def deliver(self, envelope: WaitEnvelope, transition: Transition) -> None:
        body = envelope.to_json().encode("utf-8")
        headers = {
            **self._headers,
            "Content-Type": "application/json",
            EVENT_HEADER: envelope.type,
            DEDUPE_HEADER: envelope.dedupe_key,
        }
        if self._secret is not None:
            headers[SIGNATURE_HEADER] = sign(self._secret, body)

        request = urllib.request.Request(self.url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                status = response.status
        except urllib.error.HTTPError as err:
            # A 4xx/5xx is an HTTPError in urllib; make the log line say the status.
            raise RuntimeError(f"webhook {self.url} returned {err.code}") from err
        if not 200 <= status < 300:  # pragma: no cover - urllib raises for these
            raise RuntimeError(f"webhook {self.url} returned {status}")
