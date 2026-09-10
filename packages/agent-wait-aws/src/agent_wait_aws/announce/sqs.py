"""`SqsAnnounce` -- put the envelope on a queue.

Every adapter in this package shares one shape: it does its work inside a `try`, and on
failure it logs and returns. Never raises. A queue being unreachable must not fail a
refund that has already been decided.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

import boto3
from agent_wait.model import Transition, WaitEnvelope

_log = logging.getLogger("agent_wait_aws.announce.sqs")


class SqsAnnounce:
    name = "sqs"

    def __init__(
        self,
        queue_url: str,
        *,
        group_id: Callable[[WaitEnvelope], str] | None = None,
        client: object | None = None,
        region_name: str | None = None,
        only: tuple[Transition, ...] | None = None,
    ) -> None:
        self.queue_url = queue_url
        # Grouping by thread means one in-flight message per conversation, so a
        # consumer never sees two questions from the same graph out of order.
        self.group_id = group_id or (lambda envelope: envelope.thread_id)
        self._client = client or boto3.client("sqs", region_name=region_name)
        self._only = only
        self.is_fifo = queue_url.endswith(".fifo")

    def supports(self, transition: Transition) -> bool:
        return self._only is None or transition in self._only

    def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
        try:
            kwargs: dict[str, object] = {
                "QueueUrl": self.queue_url,
                "MessageBody": envelope.to_json(),
            }
            if self.is_fifo:
                kwargs["MessageGroupId"] = self.group_id(envelope)
                # `dedupe_key`, not `event_id`. The event id is fresh on every publish,
                # so a republished question (a redelivered start message, a retry after
                # a crash) would look new. The dedupe key is stable for as long as the
                # question is, which is what SQS's five-minute window can then act on.
                kwargs["MessageDeduplicationId"] = envelope.dedupe_key
            self._client.send_message(**kwargs)  # type: ignore[attr-defined]
        except Exception:
            _log.exception("SqsAnnounce failed for interrupt %s (%s)", envelope.interrupt_id, transition)
