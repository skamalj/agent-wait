"""`make_run_handler(graph, runtime)` -- the whole Lambda.

    handler = make_run_handler(graph, runtime)

That is the deployment. The queue the agent already had is now also where approvals
arrive, because `dispatch()` can tell a start message from an answer, and the handler
does not need to care which it got.

What the wrapper is actually responsible for (REQUIREMENTS section 9.3):

* **One record at a time, keyed by thread.** With `MessageGroupId = thread_id`, SQS FIFO
  gives one in-flight message per conversation, which is the real serialisation. The
  thread lease is the belt to that pair of braces, and matters under direct invoke.
* **Keeping the message invisible while a slow graph runs.** A model call can outlast the
  visibility timeout, and a redelivery mid-run is how you get two of everything. A
  background heartbeat extends it until the graph returns.
* **Partial batch failures.** Only `lease_held` and genuine exceptions go back to SQS.
  Every other `Ignore` -- a double click, a forged token, a late timer -- is *success*:
  the system behaved correctly, and retrying would only produce the same answer more
  slowly. Nacking those is how a healthy queue turns into a DLQ full of duplicates.
* **The immediate-resume loop.** A parked answer can make `register()` hand back a resume
  to apply straight away, so the handler loops rather than waiting for another message.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable, Mapping
from typing import Any

import boto3
from agent_wait import Ignore, RegisterResult, Resume, Start, WaitRuntime

_log = logging.getLogger("agent_wait_aws.run_handler")

DEFAULT_VISIBILITY_SECONDS = 300
DEFAULT_HEARTBEAT_SECONDS = 60


class _VisibilityHeartbeat:
    """Keeps one SQS message invisible while the graph runs.

    Stops on its own if the extension call fails -- if we have lost the message there is
    nothing useful left to do, and a thread retrying forever in a dying Lambda helps
    nobody.
    """

    def __init__(
        self,
        client: Any,
        queue_url: str,
        receipt_handle: str,
        *,
        interval: float = DEFAULT_HEARTBEAT_SECONDS,
        visibility: int = DEFAULT_VISIBILITY_SECONDS,
        runtime: WaitRuntime | None = None,
        thread_id: str | None = None,
    ) -> None:
        self._client = client
        self._queue_url = queue_url
        self._receipt_handle = receipt_handle
        self._interval = interval
        self._visibility = visibility
        self._runtime = runtime
        self._thread_id = thread_id
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> _VisibilityHeartbeat:
        if self._client and self._queue_url and self._receipt_handle:
            self._thread = threading.Thread(target=self._run, daemon=True, name="agent-wait-heartbeat")
            self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self._client.change_message_visibility(
                    QueueUrl=self._queue_url,
                    ReceiptHandle=self._receipt_handle,
                    VisibilityTimeout=self._visibility,
                )
                # The lease has its own clock and must not expire under a long run either.
                if self._runtime is not None and self._thread_id is not None:
                    self._runtime.refresh_lease(self._thread_id)
            except Exception:
                _log.warning("could not extend visibility; stopping the heartbeat", exc_info=True)
                return


def queue_url_from_arn(arn: str) -> str:
    parts = arn.split(":")
    return f"https://sqs.{parts[3]}.amazonaws.com/{parts[4]}/{parts[5]}"


def make_run_handler(
    graph: Any,
    runtime: WaitRuntime,
    *,
    sqs_client: Any = None,
    visibility_seconds: int = DEFAULT_VISIBILITY_SECONDS,
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
    heartbeat: bool = True,
) -> Callable[[Mapping[str, Any], Any], dict[str, Any]]:
    """Returns an AWS Lambda handler `(event, context) -> {"batchItemFailures": [...]}`."""

    def _client() -> Any:
        return sqs_client if sqs_client is not None else boto3.client("sqs")

    def handle_record(record: Mapping[str, Any]) -> bool:
        """Returns True if this record should go back on the queue."""
        body = _parse_body(record)
        if body is None:
            return False  # unparseable: a retry produces the same nothing

        outcome = runtime.dispatch(body)
        if isinstance(outcome, Ignore):
            _log.info("ignoring message: %s (%s)", outcome.reason, outcome.detail or "no further detail")
            return outcome.should_retry

        queue_url = _queue_url_for(record)
        with _VisibilityHeartbeat(
            _client() if heartbeat else None,
            queue_url,
            str(record.get("receiptHandle", "")),
            interval=heartbeat_seconds,
            visibility=visibility_seconds,
            runtime=runtime,
            thread_id=outcome.thread_id,
        ):
            result = _invoke(outcome)
            registered = runtime.register(result, outcome.config, outcome.thread_id)
            _drain(registered)
        return False

    def _invoke(outcome: Start | Resume) -> Any:
        if isinstance(outcome, Start):
            return graph.invoke(outcome.input, outcome.config)
        return graph.invoke(outcome.command, outcome.config)

    def _drain(registered: RegisterResult) -> None:
        while registered.immediate_resume is not None:
            resume = registered.immediate_resume
            _log.info("applying a parked answer for wait %s", resume.wait.wait_id)
            result = graph.invoke(resume.command, resume.config)
            registered = runtime.register(result, resume.config, resume.thread_id)

    def _queue_url_for(record: Mapping[str, Any]) -> str:
        source = record.get("eventSourceARN")
        return queue_url_from_arn(str(source)) if source else ""

    def handler(event: Mapping[str, Any], context: Any = None) -> dict[str, Any]:
        failures: list[dict[str, str]] = []
        for record in event.get("Records", []):
            message_id = str(record.get("messageId", ""))
            try:
                if handle_record(record):
                    failures.append({"itemIdentifier": message_id})
            except Exception:
                # Something genuinely broke. Let SQS redeliver; every path the redelivery
                # can take is idempotent, which is the point of the whole library.
                _log.exception("run handler failed on message %s", message_id)
                failures.append({"itemIdentifier": message_id})
        return {"batchItemFailures": failures}

    return handler


def _parse_body(record: Mapping[str, Any]) -> dict[str, Any] | None:
    raw = record.get("body")
    if raw is None:
        return None
    try:
        body = json.loads(raw) if isinstance(raw, str) else dict(raw)
    except (ValueError, TypeError):
        _log.error("message %s is not JSON; acknowledging it", record.get("messageId"))
        return None
    if not isinstance(body, dict):
        _log.error("message %s is not a JSON object; acknowledging it", record.get("messageId"))
        return None
    # Section 7.3: `message_id` is optional in a start message, and the SQS messageId
    # stands in. That is what makes a redelivered start idempotent for free.
    if "token" not in body and not body.get("message_id") and record.get("messageId"):
        body["message_id"] = str(record["messageId"])
    return body


def make_sweep_handler(runtime: WaitRuntime, *, limit: int = 100) -> Callable[..., dict[str, int]]:
    """The one-minute repair pass (scenario D), as its own tiny Lambda.

    It needs no graph and no checkpointer -- it only ever pushes waits back onto the
    announce path and lets `dispatch()` at the real entry point do the deciding.
    """

    def handler(event: Mapping[str, Any] | None = None, context: Any = None) -> dict[str, int]:
        counts = runtime.sweep(limit=limit)
        if counts["announced"] or counts["overdue"]:
            _log.info("sweeper: %s", json.dumps(counts))
        return counts

    return handler
