"""The core announcers: `BaseAnnounce`, `LogAnnounce`, and `WebhookAnnounce` against a
real local HTTP server.

Two rules hold for every announcer, and both are asserted here rather than assumed: a
raise inside `deliver()` is a log line, never a failed publish; and the question never
reaches a log at INFO.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest
from helpers import pending

from agent_wait import (
    AnnounceAdapter,
    BaseAnnounce,
    LogAnnounce,
    Transition,
    WaitEnvelope,
    WaitPolicy,
    publish,
)
from agent_wait.announce.webhook import (
    DEDUPE_HEADER,
    EVENT_HEADER,
    SIGNATURE_HEADER,
    WebhookAnnounce,
    verify_signature,
)

REFUND = pending(
    "int-1",
    question={"kind": "refund_approval", "amount": 41000},
    policy=WaitPolicy(allowed_actions=("approve", "reject"), tags={"group": "finance"}),
)


# ============================================================ BaseAnnounce
class Exploding(BaseAnnounce):
    """A third-party adapter written by someone who did not read the contract."""

    name = "exploding"

    def deliver(self, envelope: WaitEnvelope, transition: Transition) -> None:
        raise ConnectionError("redis is down")


class Recording(BaseAnnounce):
    name = "recording"

    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.seen: list[str] = []

    def deliver(self, envelope: WaitEnvelope, transition: Transition) -> None:
        self.seen.append(envelope.dedupe_key)


def test_deliver_is_all_a_subclass_has_to_write() -> None:
    adapter = Recording()

    assert isinstance(adapter, AnnounceAdapter), "the base satisfies the protocol"
    publish([REFUND], "t", [adapter])
    assert adapter.seen == ["wait.created:int-1"]


def test_a_raising_deliver_becomes_a_log_line_not_an_exception(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.ERROR, logger="agent_wait.announce.exploding"):
        sent = publish([REFUND], "t", [Exploding()])  # must not raise

    assert len(sent) == 1
    assert "Exploding failed for interrupt int-1" in caplog.text
    assert "redis is down" in caplog.text


def test_only_filters_without_the_subclass_doing_anything() -> None:
    assert Recording(only=("created",)).supports("created")
    assert not Recording(only=()).supports("created")


# ============================================================ LogAnnounce
def test_the_question_is_not_logged_at_info(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="agent_wait.announce"):
        publish([REFUND], "order-4471", [LogAnnounce()])

    assert "refund_approval" not in caplog.text
    assert "41000" not in caplog.text
    assert "wait.created" in caplog.text and "order-4471" in caplog.text and "finance" in caplog.text


def test_the_question_is_available_at_debug(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.DEBUG, logger="agent_wait.announce"):
        publish([REFUND], "order-4471", [LogAnnounce()])

    assert "refund_approval" in caplog.text


# ============================================================ WebhookAnnounce
SECRET = b"shared-with-the-receiver"


class Receiver:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.status = 200
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                receiver.requests.append({"path": self.path, "headers": dict(self.headers), "body": body})
                self.send_response(receiver.status)
                self.end_headers()

            def log_message(self, *args: object) -> None:
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}/hooks/agent-wait"

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture()
def receiver() -> Iterator[Receiver]:
    r = Receiver()
    yield r
    r.stop()


def test_the_envelope_is_posted_as_json_with_routing_headers(receiver: Receiver) -> None:
    publish([REFUND], "order-4471", [WebhookAnnounce(receiver.url, headers={"X-Team": "finance"})])

    [request] = receiver.requests
    assert request["path"] == "/hooks/agent-wait"
    assert request["headers"]["Content-Type"] == "application/json"
    assert request["headers"][EVENT_HEADER] == "wait.created"
    assert request["headers"][DEDUPE_HEADER] == "wait.created:int-1"
    assert request["headers"]["X-Team"] == "finance"
    assert SIGNATURE_HEADER not in request["headers"], "no secret, no signature"
    body = json.loads(request["body"])
    assert body["question"] == {"kind": "refund_approval", "amount": 41000}
    assert body["reply_with"]["question_id"] == "int-1"


def test_the_body_is_signed_and_the_receiver_can_verify_it(receiver: Receiver) -> None:
    publish([REFUND], "t", [WebhookAnnounce(receiver.url, secret=SECRET)])

    [request] = receiver.requests
    header = request["headers"][SIGNATURE_HEADER]
    assert header.startswith("sha256=")
    assert verify_signature(SECRET, request["body"], header)
    assert not verify_signature(b"wrong-secret", request["body"], header)
    assert not verify_signature(SECRET, request["body"] + b" ", header)
    assert not verify_signature(SECRET, request["body"], None)


def test_a_5xx_is_a_log_line_not_a_failed_publish(
    receiver: Receiver, caplog: pytest.LogCaptureFixture
) -> None:
    receiver.status = 503

    with caplog.at_level(logging.ERROR, logger="agent_wait.announce.webhook"):
        sent = publish([REFUND], "t", [WebhookAnnounce(receiver.url)])

    assert len(sent) == 1
    assert "returned 503" in caplog.text
    assert len(receiver.requests) == 1, "no retry"


def test_an_unreachable_receiver_is_a_log_line(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.ERROR, logger="agent_wait.announce.webhook"):
        publish([REFUND], "t", [WebhookAnnounce("http://127.0.0.1:9/nothing-listens-here", timeout=0.5)])

    assert "WebhookAnnounce failed" in caplog.text
