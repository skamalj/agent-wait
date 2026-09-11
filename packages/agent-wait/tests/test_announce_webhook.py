"""`WebhookAnnounce` against a real local HTTP server.

Not a mocked `urlopen`: the whole point of the adapter is what arrives on the wire, so a
tiny `http.server` on a random port records exactly that.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest
from agent_wait import WaitPolicy
from agent_wait.announce.webhook import (
    DEDUPE_HEADER,
    EVENT_HEADER,
    SIGNATURE_HEADER,
    WebhookAnnounce,
    verify_signature,
)
from rig import Rig

SECRET = b"shared-with-the-receiver"


class Receiver:
    """Records every POST. `status` controls what it answers with."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.status = 200
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                receiver.requests.append({"path": self.path, "headers": dict(self.headers), "body": body})
                self.send_response(receiver.status)
                self.end_headers()

            def log_message(self, *args: object) -> None:
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}/hooks/agent-wait"

    def start(self) -> Receiver:
        self.thread.start()
        return self

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture()
def receiver() -> Iterator[Receiver]:
    r = Receiver().start()
    yield r
    r.stop()


def park(rig: Rig) -> None:
    rig.invoke_that(
        lambda: rig.adapter.park(
            "order-4471",
            "int-1",
            policy=WaitPolicy(allowed_actions=("approve", "reject")),
            question={"kind": "refund_approval", "amount": 41000},
        ),
        thread_id="order-4471",
    )


def test_the_envelope_is_posted_as_json_with_routing_headers(receiver: Receiver) -> None:
    rig = Rig()
    rig.agent.announce.adapters = [WebhookAnnounce(receiver.url, headers={"X-Team": "finance"})]

    park(rig)

    [request] = receiver.requests
    assert request["path"] == "/hooks/agent-wait"
    assert request["headers"]["Content-Type"] == "application/json"
    assert request["headers"][EVENT_HEADER] == "wait.created"
    assert request["headers"][DEDUPE_HEADER] == "wait.created:int-1"
    assert request["headers"]["X-Team"] == "finance", "caller headers survive"
    assert SIGNATURE_HEADER not in request["headers"], "no secret, no signature"

    body = json.loads(request["body"])
    assert body["interrupt_id"] == "int-1"
    assert body["question"] == {"kind": "refund_approval", "amount": 41000}
    assert body["reply_with"]["interrupt_id"] == "int-1"


def test_the_body_is_signed_and_the_receiver_can_verify_it(receiver: Receiver) -> None:
    """GitHub/Stripe shape: HMAC-SHA256 of the raw bytes, hex, `sha256=` prefix."""
    rig = Rig()
    rig.agent.announce.adapters = [WebhookAnnounce(receiver.url, secret=SECRET)]

    park(rig)

    [request] = receiver.requests
    header = request["headers"][SIGNATURE_HEADER]
    assert header.startswith("sha256=")
    assert verify_signature(SECRET, request["body"], header)
    assert not verify_signature(b"wrong-secret", request["body"], header)
    assert not verify_signature(SECRET, request["body"] + b" ", header), "any byte change fails"
    assert not verify_signature(SECRET, request["body"], None)


def test_a_5xx_is_a_log_line_not_a_failed_run(receiver: Receiver, caplog: pytest.LogCaptureFixture) -> None:
    receiver.status = 503
    rig = Rig()
    rig.agent.announce.adapters = [WebhookAnnounce(receiver.url)]

    with caplog.at_level(logging.ERROR, logger="agent_wait.announce.webhook"):
        park(rig)  # must not raise

    assert "returned 503" in caplog.text
    assert len(receiver.requests) == 1, "no retry; republish() is the retry"


def test_an_unreachable_receiver_is_a_log_line_not_a_failed_run(caplog: pytest.LogCaptureFixture) -> None:
    rig = Rig()
    rig.agent.announce.adapters = [WebhookAnnounce("http://127.0.0.1:9/nothing-listens-here", timeout=0.5)]

    with caplog.at_level(logging.ERROR, logger="agent_wait.announce.webhook"):
        park(rig)

    assert "WebhookAnnounce failed" in caplog.text


def test_resumed_is_posted_too(receiver: Receiver) -> None:
    rig = Rig()
    rig.agent.announce.adapters = [WebhookAnnounce(receiver.url)]
    rig.adapter.park("t", "int-1")

    rig.invoke_that(lambda: rig.adapter.unpark("t", "int-1"))

    [request] = receiver.requests
    assert request["headers"][EVENT_HEADER] == "wait.resumed"
    assert request["headers"][DEDUPE_HEADER] == "wait.resumed:int-1"
