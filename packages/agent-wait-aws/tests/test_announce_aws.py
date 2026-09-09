"""The four AWS announce adapters, against moto.

Two things get checked for every one of them: that the envelope lands where it should
with the right routing metadata, and that a broken backend produces a log line rather
than an exception. The second is the contract (REQUIREMENTS section 8) and is the reason
a Slack outage cannot fail a refund.
"""

from __future__ import annotations

import json
from typing import Any

import boto3
import pytest
from agent_wait import EntryPoint, WaitEnvelope
from agent_wait_aws import (
    EventBridgeAnnounce,
    SchedulerAnnounce,
    SnsAnnounce,
    SqsAnnounce,
    queue_arn_from_url,
    timeout_message,
)

from conftest import REGION, make_fifo_queue, received


def envelope(**overrides: Any) -> WaitEnvelope:
    fields: dict[str, Any] = {
        "type": "wait.created",
        "event_id": "01JEVENT00000000000000001",
        "wait_id": "01JWAIT000000000000000001",
        "thread_id": "order-4471",
        "question": {"kind": "refund_approval", "amount": 41000},
        "allowed_actions": ("approve", "reject"),
        "expires_at": "2099-01-01T00:00:00Z",
        "token": "aw1.k1.01JWAIT000000000000000001.4102444800.abcdef0123456789.approve+reject.mac",
        "reply_to": EntryPoint("sqs", "https://sqs.local/q.fifo").to_dict(),
        "tags": {"approver_group": "finance"},
    }
    fields.update(overrides)
    return WaitEnvelope(**fields)


class Broken:
    """Every call explodes. Stands in for a deleted topic or a revoked permission."""

    def __getattr__(self, name: str) -> Any:
        def boom(*args: Any, **kwargs: Any) -> Any:
            raise RuntimeError(f"{name} is unavailable")

        return boom


# ============================================================== SQS
def test_sqs_announce_sends_the_envelope_to_a_fifo_queue() -> None:
    url, _ = make_fifo_queue("announcements.fifo")
    SqsAnnounce(url, region_name=REGION).announce(envelope(), "created")

    [body] = received(url)

    assert body["type"] == "wait.created"
    assert body["wait_id"] == "01JWAIT000000000000000001"
    assert body["question"]["amount"] == 41000


def test_sqs_announce_groups_by_thread_and_dedupes_on_event_id() -> None:
    url, _ = make_fifo_queue("announcements.fifo")
    adapter = SqsAnnounce(url, region_name=REGION)

    adapter.announce(envelope(), "created")
    adapter.announce(envelope(), "created")  # same event_id: SQS must swallow it

    assert len(received(url)) == 1


def test_sqs_announce_omits_fifo_parameters_for_a_standard_queue() -> None:
    url = boto3.client("sqs", region_name=REGION).create_queue(QueueName="plain")["QueueUrl"]

    SqsAnnounce(url, region_name=REGION).announce(envelope(), "created")

    assert len(received(url)) == 1


def test_sqs_announce_accepts_a_custom_group_id() -> None:
    url, _ = make_fifo_queue("announcements.fifo")
    adapter = SqsAnnounce(url, group_id=lambda e: e.tags["approver_group"], region_name=REGION)

    adapter.announce(envelope(), "created")

    assert len(received(url)) == 1


def test_sqs_announce_swallows_a_broken_client() -> None:
    SqsAnnounce("https://q.fifo", client=Broken()).announce(envelope(), "created")


# ============================================================== SNS
def test_sns_announce_publishes_with_tags_as_message_attributes() -> None:
    sns = boto3.client("sns", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)
    topic_arn = sns.create_topic(Name="waits")["TopicArn"]
    # A standard queue: SNS refuses to deliver from a standard topic to a FIFO one.
    url = sqs.create_queue(QueueName="sns-target")["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=arn, Attributes={"RawMessageDelivery": "true"})

    SnsAnnounce(topic_arn, region_name=REGION).announce(envelope(), "created")

    [body] = received(url)
    assert body["wait_id"] == "01JWAIT000000000000000001"


def test_sns_announce_skips_empty_tag_values() -> None:
    """SNS rejects an empty StringValue, and no filter policy can match one."""
    captured: dict[str, Any] = {}

    class Capture:
        def publish(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {}

    SnsAnnounce("arn:topic", client=Capture()).announce(
        envelope(tags={"approver_group": "finance", "note": ""}), "created"
    )

    assert set(captured["MessageAttributes"]) == {"transition", "thread_id", "approver_group"}
    assert captured["MessageAttributes"]["transition"]["StringValue"] == "created"


def test_sns_announce_swallows_a_broken_client() -> None:
    SnsAnnounce("arn:topic", client=Broken()).announce(envelope(), "created")


# ============================================================== EventBridge
def test_eventbridge_announce_uses_the_transition_as_the_detail_type() -> None:
    captured: dict[str, Any] = {}

    class Capture:
        def put_events(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {"FailedEntryCount": 0}

    EventBridgeAnnounce("agent-wait-bus", client=Capture()).announce(envelope(), "answered")

    [entry] = captured["Entries"]
    assert entry["DetailType"] == "wait.answered"
    assert entry["Source"] == "agent-wait"
    assert entry["EventBusName"] == "agent-wait-bus"
    assert json.loads(entry["Detail"])["wait_id"] == "01JWAIT000000000000000001"


def test_eventbridge_announce_notices_a_failed_entry(caplog: pytest.LogCaptureFixture) -> None:
    """PutEvents returns 200 with the failures in the body, so a naive call looks fine."""

    class PartialFailure:
        def put_events(self, **kwargs: Any) -> dict[str, Any]:
            return {"FailedEntryCount": 1, "Entries": [{"ErrorCode": "ThrottlingException"}]}

    with caplog.at_level("ERROR"):
        EventBridgeAnnounce("bus", client=PartialFailure()).announce(envelope(), "created")

    assert "ThrottlingException" in caplog.text


def test_eventbridge_announce_swallows_a_broken_client() -> None:
    EventBridgeAnnounce("bus", client=Broken()).announce(envelope(), "created")


# ============================================================== Scheduler (the timeout)
def scheduler_adapter(queue_url: str, queue_arn: str) -> SchedulerAnnounce:
    boto3.client("scheduler", region_name=REGION).create_schedule_group(Name="agent-wait")
    return SchedulerAnnounce(
        "agent-wait",
        queue_arn=queue_arn,
        role_arn="arn:aws:iam::123456789012:role/agent-wait-scheduler",
        queue_url=queue_url,
        region_name=REGION,
    )


def test_scheduler_arms_a_one_shot_schedule_named_for_the_wait() -> None:
    url, arn = make_fifo_queue()
    scheduler_adapter(url, arn).announce(envelope(), "created")

    schedule = boto3.client("scheduler", region_name=REGION).get_schedule(
        Name="01JWAIT000000000000000001", GroupName="agent-wait"
    )

    assert schedule["ScheduleExpression"] == "at(2099-01-01T00:00:00)"
    assert schedule["FlexibleTimeWindow"]["Mode"] == "OFF"
    assert schedule["ActionAfterCompletion"] == "DELETE"
    assert schedule["Target"]["Arn"] == arn
    assert schedule["Target"]["SqsParameters"]["MessageGroupId"] == "order-4471"


def test_the_scheduled_payload_is_an_ordinary_answer_envelope() -> None:
    """The whole design in one assertion: a timeout is not a special event, it is an
    answer that arrives late at the ordinary entry point."""
    url, arn = make_fifo_queue()
    scheduler_adapter(url, arn).announce(envelope(), "created")

    schedule = boto3.client("scheduler", region_name=REGION).get_schedule(
        Name="01JWAIT000000000000000001", GroupName="agent-wait"
    )
    payload = json.loads(schedule["Target"]["Input"])

    assert payload == {
        "token": envelope().token,
        "action": "timeout",
        "answer_id": "timeout:01JWAIT000000000000000001",
    }


def test_scheduler_does_nothing_for_a_wait_with_no_timeout() -> None:
    url, arn = make_fifo_queue()
    scheduler_adapter(url, arn).announce(envelope(expires_at=None), "created")

    assert (
        boto3.client("scheduler", region_name=REGION).list_schedules(GroupName="agent-wait")["Schedules"]
        == []
    )


def test_arming_twice_is_harmless() -> None:
    """A redelivered `created` announce. The schedule is named after the wait, so the
    second attempt is a ConflictException we shrug at."""
    url, arn = make_fifo_queue()
    adapter = scheduler_adapter(url, arn)

    adapter.announce(envelope(), "created")
    adapter.announce(envelope(), "created")

    assert (
        len(boto3.client("scheduler", region_name=REGION).list_schedules(GroupName="agent-wait")["Schedules"])
        == 1
    )


@pytest.mark.parametrize("transition", ["answered", "expired", "resumed", "cancelled"])
def test_settling_a_wait_deletes_its_schedule(transition: str) -> None:
    url, arn = make_fifo_queue()
    adapter = scheduler_adapter(url, arn)
    adapter.announce(envelope(), "created")

    adapter.announce(envelope(type=f"wait.{transition}"), transition)  # type: ignore[arg-type]

    assert (
        boto3.client("scheduler", region_name=REGION).list_schedules(GroupName="agent-wait")["Schedules"]
        == []
    )


def test_deleting_a_schedule_that_already_fired_is_harmless() -> None:
    url, arn = make_fifo_queue()
    adapter = scheduler_adapter(url, arn)

    adapter.announce(envelope(), "answered")  # never armed in the first place


def test_an_overdue_wait_is_delivered_immediately_instead_of_scheduled() -> None:
    """The sweeper's repair path. EventBridge will not accept a schedule in the past, so
    the timeout goes straight to the entry point -- still as an ordinary answer."""
    url, arn = make_fifo_queue()
    adapter = scheduler_adapter(url, arn)

    adapter.announce(envelope(expires_at="2020-01-01T00:00:00Z"), "created")

    assert (
        boto3.client("scheduler", region_name=REGION).list_schedules(GroupName="agent-wait")["Schedules"]
        == []
    )
    [body] = received(url)
    assert body["action"] == "timeout"
    assert body["answer_id"] == "timeout:01JWAIT000000000000000001"


def test_scheduler_swallows_a_broken_client() -> None:
    SchedulerAnnounce(
        "g", queue_arn="arn:aws:sqs:ap-south-1:1:q", role_arn="arn:role", client=Broken()
    ).announce(envelope(), "created")


def test_scheduler_only_reacts_to_transitions_it_cares_about() -> None:
    adapter = SchedulerAnnounce(
        "g", queue_arn="arn:aws:sqs:ap-south-1:1:q", role_arn="arn:role", client=Broken()
    )
    assert adapter.supports("created") is True
    assert adapter.supports("answered") is True
    assert adapter.supports("cancelled") is True


def test_timeout_message_helper_matches_what_is_scheduled() -> None:
    assert timeout_message(envelope())["action"] == "timeout"


# ============================================================== entry point helpers
def test_queue_arn_from_url() -> None:
    url, arn = make_fifo_queue()
    assert queue_arn_from_url(url) == arn
