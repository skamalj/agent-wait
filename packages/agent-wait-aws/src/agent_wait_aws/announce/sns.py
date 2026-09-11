"""`SnsAnnounce` -- publish the envelope, with the policy's tags as message attributes.

The tags are the useful part. `WaitPolicy(tags={"approver_group": "finance"})` becomes an
SNS message attribute, so a subscription filter policy can route finance approvals to one
place and everything else somewhere else -- without the agent knowing any of those places
exist.
"""

from __future__ import annotations

from typing import Any

import boto3
from agent_wait.announce import BaseAnnounce
from agent_wait.model import Transition, WaitEnvelope


class SnsAnnounce(BaseAnnounce):
    name = "sns"

    def __init__(
        self,
        topic_arn: str,
        *,
        client: Any = None,
        region_name: str | None = None,
        only: tuple[Transition, ...] | None = None,
    ) -> None:
        super().__init__(only=only)
        self.topic_arn = topic_arn
        self._client = client or boto3.client("sns", region_name=region_name)

    def deliver(self, envelope: WaitEnvelope, transition: Transition) -> None:
        attributes: dict[str, dict[str, str]] = {
            "transition": {"DataType": "String", "StringValue": transition},
            "thread_id": {"DataType": "String", "StringValue": envelope.thread_id},
        }
        for key, value in envelope.tags.items():
            # SNS rejects an empty StringValue, and a filter policy cannot use one.
            if value:
                attributes[key] = {"DataType": "String", "StringValue": str(value)}
        self._client.publish(
            TopicArn=self.topic_arn,
            Subject=envelope.type,
            Message=envelope.to_json(),
            MessageAttributes=attributes,
        )
