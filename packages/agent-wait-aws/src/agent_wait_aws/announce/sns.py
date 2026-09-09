"""`SnsAnnounce` -- publish the envelope, with the policy's tags as message attributes.

The tags are the useful part. `WaitPolicy(tags={"approver_group": "finance"})` becomes an
SNS message attribute, so a subscription filter policy can route finance approvals to one
place and everything else somewhere else -- without the agent knowing any of those places
exist.
"""

from __future__ import annotations

import logging

import boto3
from agent_wait.model import Transition, WaitEnvelope

_log = logging.getLogger("agent_wait_aws.announce.sns")


class SnsAnnounce:
    name = "sns"

    def __init__(
        self,
        topic_arn: str,
        *,
        client: object | None = None,
        region_name: str | None = None,
        only: tuple[Transition, ...] | None = None,
    ) -> None:
        self.topic_arn = topic_arn
        self._client = client or boto3.client("sns", region_name=region_name)
        self._only = only

    def supports(self, transition: Transition) -> bool:
        return self._only is None or transition in self._only

    def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
        try:
            attributes: dict[str, dict[str, str]] = {
                "transition": {"DataType": "String", "StringValue": transition},
                "thread_id": {"DataType": "String", "StringValue": envelope.thread_id},
            }
            for key, value in envelope.tags.items():
                # SNS rejects an empty StringValue, and a filter policy cannot use one.
                if value:
                    attributes[key] = {"DataType": "String", "StringValue": str(value)}
            self._client.publish(  # type: ignore[attr-defined]
                TopicArn=self.topic_arn,
                Subject=f"wait.{transition}",
                Message=envelope.to_json(),
                MessageAttributes=attributes,
            )
        except Exception:
            _log.exception("SnsAnnounce failed for wait %s (%s)", envelope.wait_id, transition)
