"""`SqsAnnounce` -- put the envelope on a queue.

On a FIFO queue, `MessageGroupId` is the thread id (one in-flight question per
conversation, so a consumer never sees two from the same graph out of order) and
`MessageDeduplicationId` is the envelope's `dedupe_key` -- not its `event_id`, which is
fresh on every publish and would let a republished question through as new.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import boto3
from agent_wait.announce import BaseAnnounce
from agent_wait.model import Transition, WaitEnvelope


class SqsAnnounce(BaseAnnounce):
    name = "sqs"

    def __init__(
        self,
        queue_url: str,
        *,
        group_id: Callable[[WaitEnvelope], str] | None = None,
        client: Any = None,
        region_name: str | None = None,
        only: tuple[Transition, ...] | None = None,
    ) -> None:
        super().__init__(only=only)
        self.queue_url = queue_url
        self.group_id = group_id or (lambda envelope: envelope.thread_id)
        self._client = client or boto3.client("sqs", region_name=region_name)
        self.is_fifo = queue_url.endswith(".fifo")

    def deliver(self, envelope: WaitEnvelope, transition: Transition) -> None:
        kwargs: dict[str, Any] = {"QueueUrl": self.queue_url, "MessageBody": envelope.to_json()}
        if self.is_fifo:
            kwargs["MessageGroupId"] = self.group_id(envelope)
            kwargs["MessageDeduplicationId"] = envelope.dedupe_key
        self._client.send_message(**kwargs)
