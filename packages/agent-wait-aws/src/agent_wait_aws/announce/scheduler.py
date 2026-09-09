"""`SchedulerAnnounce` -- the timeout, as an announce adapter.

This is the piece of the design that removes a whole component. There is no timer Lambda
and no `on_timer` handler, because a timeout is not a special kind of event: it is an
*answer*, with `action: "timeout"`, delivered to the agent's own entry point by a
messenger that happens to be three days slow. `dispatch()` handles it with exactly the
same code path as a human clicking Approve, which is why "the timer fired at the same
moment somebody clicked" needs no special handling at all -- it is rule 2, unchanged.

So this adapter is a delivery mechanism to the future:

* on `created` with an `expires_at`, create a one-shot EventBridge schedule named after
  the wait, whose target sends the timeout answer to the entry-point queue;
* on `answered`, `expired`, `resumed` or `cancelled`, delete it.

Details that matter:

**The schedule is named `wait_id`.** Creating it is therefore idempotent -- a redelivered
`created` announce gets `ConflictException` and we shrug. Deleting it needs no stored
handle, which is what keeps `announce_refs` empty in v0.1.

**`ActionAfterCompletion=DELETE`** so a fired schedule cleans itself up, and
**`FlexibleTimeWindow=OFF`** because "within 15 minutes of the deadline" is not a
deadline.

**An expiry already in the past is delivered immediately** rather than scheduled.
EventBridge will not accept a schedule in the past, and this is a real case: the sweeper
re-announcing a wait whose schedule was lost while the process was down. Sending the
timeout straight to the queue keeps the invariant that timeouts always arrive as ordinary
answers at the ordinary entry point.
"""

from __future__ import annotations

import json
import logging
import time

import boto3
from agent_wait.model import Transition, WaitEnvelope

_log = logging.getLogger("agent_wait_aws.announce.scheduler")

_CANCELLING: tuple[Transition, ...] = ("answered", "expired", "resumed", "cancelled")


class SchedulerAnnounce:
    name = "scheduler"

    def __init__(
        self,
        group_name: str,
        *,
        queue_arn: str,
        role_arn: str,
        queue_url: str | None = None,
        client: object | None = None,
        sqs_client: object | None = None,
        region_name: str | None = None,
    ) -> None:
        self.group_name = group_name
        self.queue_arn = queue_arn
        self.role_arn = role_arn
        self.queue_url = queue_url
        self._client = client or boto3.client("scheduler", region_name=region_name)
        self._sqs = sqs_client
        self._region_name = region_name

    def supports(self, transition: Transition) -> bool:
        return transition == "created" or transition in _CANCELLING

    # ------------------------------------------------------------------ entry
    def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
        try:
            if transition == "created":
                self._arm(envelope)
            elif transition in _CANCELLING:
                self._disarm(envelope)
        except Exception:
            _log.exception("SchedulerAnnounce failed for wait %s (%s)", envelope.wait_id, transition)

    # ------------------------------------------------------------------ arm
    def _arm(self, envelope: WaitEnvelope) -> None:
        if not envelope.expires_at:
            return  # a wait with no timeout needs no schedule

        due = _parse_iso(envelope.expires_at)
        if due <= time.time():
            # Already overdue: the sweeper is repairing a schedule that was lost. Deliver
            # the timeout now rather than trying to schedule the past.
            _log.info("wait %s is already overdue; delivering the timeout now", envelope.wait_id)
            self._send_now(envelope)
            return

        try:
            self._client.create_schedule(  # type: ignore[attr-defined]
                Name=envelope.wait_id,
                GroupName=self.group_name,
                ScheduleExpression=f"at({envelope.expires_at.rstrip('Z')})",
                ScheduleExpressionTimezone="UTC",
                ActionAfterCompletion="DELETE",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={
                    "Arn": self.queue_arn,
                    "RoleArn": self.role_arn,
                    "Input": json.dumps(timeout_message(envelope)),
                    "SqsParameters": {"MessageGroupId": envelope.thread_id},
                },
            )
        except Exception as err:
            if type(err).__name__ == "ConflictException":
                # The schedule already exists: a redelivered `created` announce. Fine.
                _log.debug("schedule for wait %s already exists", envelope.wait_id)
                return
            raise

    # ------------------------------------------------------------------ disarm
    def _disarm(self, envelope: WaitEnvelope) -> None:
        try:
            self._client.delete_schedule(  # type: ignore[attr-defined]
                Name=envelope.wait_id, GroupName=self.group_name
            )
        except Exception as err:
            if type(err).__name__ == "ResourceNotFoundException":
                # Already fired and self-deleted, or never armed. Both are fine.
                return
            raise

    # ------------------------------------------------------------------ immediate
    def _send_now(self, envelope: WaitEnvelope) -> None:
        client = self._sqs or boto3.client("sqs", region_name=self._region_name)
        url = self.queue_url or _url_from_arn(self.queue_arn)
        kwargs: dict[str, object] = {
            "QueueUrl": url,
            "MessageBody": json.dumps(timeout_message(envelope)),
        }
        if url.endswith(".fifo"):
            kwargs["MessageGroupId"] = envelope.thread_id
            kwargs["MessageDeduplicationId"] = f"timeout-{envelope.wait_id}"
        client.send_message(**kwargs)  # type: ignore[attr-defined]


def timeout_message(envelope: WaitEnvelope) -> dict[str, str]:
    """The answer envelope the schedule delivers (REQUIREMENTS section 8).

    `answer_id` is derived from the wait id, so a schedule that somehow fires twice is a
    `duplicate` rather than a conflict.
    """
    return {
        "token": envelope.token,
        "action": "timeout",
        "answer_id": f"timeout:{envelope.wait_id}",
    }


def _parse_iso(value: str) -> float:
    return time.mktime(time.strptime(value, "%Y-%m-%dT%H:%M:%SZ")) - time.timezone


def _url_from_arn(arn: str) -> str:
    # arn:aws:sqs:<region>:<account>:<name>
    parts = arn.split(":")
    return f"https://sqs.{parts[3]}.amazonaws.com/{parts[4]}/{parts[5]}"
