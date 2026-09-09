"""`make_run_handler` against fake SQS events: redelivery, partial batch failure, junk.

The wrapper's judgement call is which failures go back on the queue. Only `lease_held`
and real exceptions do. A double click, a forged token or a late timer are *successes* --
the system behaved correctly -- and nacking them would turn a healthy queue into a DLQ
full of duplicates.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from agent_wait import EntryPoint, FakeClock, InMemoryAnnounce, StaticKeyProvider, TokenCodec, WaitRuntime
from agent_wait_aws import (
    SchedulerAnnounce,
    SqsAnnounce,
    make_run_handler,
    make_sweep_handler,
    queue_arn_from_url,
)
from langgraph.checkpoint.memory import InMemorySaver
from langgraph_wait import LangGraphAdapter
from refund_agent.graph import PAYMENTS_CALLED, build_graph, reset_side_effects

from conftest import REGION, make_dynamo_store, make_fifo_queue, received

KEYS = StaticKeyProvider({"k1": b"aws-test-key"}, "k1")

START_BODY = {
    "thread_id": "order-4471",
    "input": {"order_id": "order-4471", "amount": 41000},
}


@pytest.fixture(autouse=True)
def clean() -> Iterator[None]:
    reset_side_effects()
    yield
    reset_side_effects()


class Agent:
    """A deployed agent: graph, runtime, queue, handler."""

    def __init__(self, *, announce: list[Any] | None = None, queue_url: str | None = None) -> None:
        self.graph = build_graph(InMemorySaver())
        self.store = make_dynamo_store()
        # Anchored to real time: SchedulerAnnounce compares the envelope's expiry
        # against time.time(), and a clock stuck in the past makes every wait look
        # overdue and skip straight to the immediate-delivery path.
        self.clock = FakeClock(time.time())
        self.announce = InMemoryAnnounce()
        self.queue_url = queue_url or make_fifo_queue()[0]
        self.runtime = WaitRuntime(
            adapter=LangGraphAdapter(self.graph),
            store=self.store,
            tokens=TokenCodec(KEYS, clock=self.clock),
            announce=announce if announce is not None else [self.announce],
            entry_point=EntryPoint("sqs", self.queue_url),
            clock=self.clock,
            owner="worker-a",
        )
        self.handler = make_run_handler(self.graph, self.runtime, heartbeat=False)

    def event(self, *bodies: dict[str, Any], message_ids: list[str] | None = None) -> dict[str, Any]:
        ids = message_ids or [f"sqs-{i}" for i in range(len(bodies))]
        return {
            "Records": [
                {
                    "messageId": mid,
                    "receiptHandle": f"rh-{mid}",
                    "body": json.dumps(body),
                    "eventSourceARN": queue_arn_from_url(self.queue_url),
                }
                for body, mid in zip(bodies, ids, strict=True)
            ]
        }

    def token(self, index: int = 0) -> str:
        return self.announce.of("created")[index].token

    def answer(self, action: str = "approve", answer_id: str = "click-1") -> dict[str, Any]:
        return {
            "token": self.token(),
            "action": action,
            "payload": {"note": "ok"},
            "actor": "priya@corp",
            "answer_id": answer_id,
        }


# ============================================================== the basics
def test_a_start_message_parks_the_thread() -> None:
    agent = Agent()

    result = agent.handler(agent.event(START_BODY), None)

    assert result == {"batchItemFailures": []}
    assert len(agent.store.find(thread_id="order-4471")) == 1
    assert len(agent.announce.of("created")) == 1
    assert PAYMENTS_CALLED == []


def test_the_sqs_message_id_stands_in_for_a_missing_message_id() -> None:
    """Section 7.3. It is what makes a redelivered start idempotent for free."""
    agent = Agent()

    agent.handler(agent.event(START_BODY, message_ids=["sqs-abc"]), None)
    agent.handler(agent.event(START_BODY, message_ids=["sqs-abc"]), None)

    assert len(agent.store.find(thread_id="order-4471")) == 1
    assert len(agent.announce.of("created")) == 1


def test_an_answer_resumes_the_graph_and_refunds_once() -> None:
    agent = Agent()
    agent.handler(agent.event(START_BODY), None)

    result = agent.handler(agent.event(agent.answer()), None)

    assert result == {"batchItemFailures": []}
    assert PAYMENTS_CALLED == ["order-4471"]


def test_a_redelivered_answer_is_acknowledged_not_retried() -> None:
    agent = Agent()
    agent.handler(agent.event(START_BODY), None)
    agent.handler(agent.event(agent.answer()), None)

    result = agent.handler(agent.event(agent.answer()), None)

    assert result == {"batchItemFailures": []}, "a duplicate is success, not a retry"
    assert PAYMENTS_CALLED == ["order-4471"]


def test_a_forged_token_is_acknowledged() -> None:
    agent = Agent()
    agent.handler(agent.event(START_BODY), None)
    forged = {"token": agent.token()[:-1] + "Z", "action": "approve", "answer_id": "evil"}

    result = agent.handler(agent.event(forged), None)

    assert result == {"batchItemFailures": []}
    assert PAYMENTS_CALLED == []


# ============================================================== batches
def test_a_batch_of_two_threads_is_processed_independently() -> None:
    agent = Agent()

    result = agent.handler(
        agent.event(
            {"thread_id": "order-1", "input": {"order_id": "order-1", "amount": 41000}},
            {"thread_id": "order-2", "input": {"order_id": "order-2", "amount": 900}},
        ),
        None,
    )

    assert result == {"batchItemFailures": []}
    assert len(agent.store.find(thread_id="order-1")) == 1
    assert PAYMENTS_CALLED == ["order-2"], "the small one auto-approved and paid out"


def test_lease_held_becomes_a_partial_batch_failure() -> None:
    agent = Agent()
    other = WaitRuntime(
        adapter=LangGraphAdapter(agent.graph),
        store=agent.store,
        tokens=TokenCodec(KEYS, clock=agent.clock),
        announce=[],
        entry_point=EntryPoint("sqs", agent.queue_url),
        clock=agent.clock,
        owner="worker-b",
    )
    other.dispatch({"thread_id": "order-4471", "input": {}, "message_id": "held"})

    result = agent.handler(agent.event(START_BODY, message_ids=["sqs-blocked"]), None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "sqs-blocked"}]}


def test_an_exception_becomes_a_partial_batch_failure() -> None:
    agent = Agent()

    class Exploding:
        def invoke(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("the model provider is down")

        def get_state(self, *args: Any, **kwargs: Any) -> Any:
            return agent.graph.get_state(*args, **kwargs)

    handler = make_run_handler(Exploding(), agent.runtime, heartbeat=False)

    result = handler(agent.event(START_BODY, message_ids=["sqs-boom"]), None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "sqs-boom"}]}


def test_one_bad_record_does_not_take_down_the_batch() -> None:
    agent = Agent()
    event = agent.event(START_BODY, message_ids=["sqs-good"])
    event["Records"].append({"messageId": "sqs-junk", "receiptHandle": "rh", "body": "not json at all"})

    result = agent.handler(event, None)

    assert result == {"batchItemFailures": []}, "junk is acknowledged; a retry changes nothing"
    assert len(agent.store.find(thread_id="order-4471")) == 1


@pytest.mark.parametrize("body", ["[]", '"a string"', "null"])
def test_a_non_object_body_is_acknowledged(body: str) -> None:
    agent = Agent()
    event = {"Records": [{"messageId": "m", "receiptHandle": "rh", "body": body}]}

    assert agent.handler(event, None) == {"batchItemFailures": []}


def test_an_empty_event_is_fine() -> None:
    agent = Agent()
    assert agent.handler({}, None) == {"batchItemFailures": []}


# ============================================================== the parked-answer loop
def test_the_handler_loops_on_an_immediate_resume() -> None:
    """An answer that beat its wait into existence. `register()` hands back a resume and
    the handler applies it without waiting for another message."""
    agent = Agent()
    agent.handler(agent.event(START_BODY, message_ids=["sqs-start"]), None)
    wait = agent.store.find(thread_id="order-4471")[0]
    agent.store.park_answer(wait.wait_id, agent.answer(answer_id="click-early"))

    # The redelivery carries the same message id, so dispatch() re-invokes with None and
    # the thread lands on the same interrupt at the same checkpoint -- the same wait.
    result = agent.handler(agent.event(START_BODY, message_ids=["sqs-start"]), None)

    assert result == {"batchItemFailures": []}
    assert PAYMENTS_CALLED == ["order-4471"]
    assert len(agent.store.find(thread_id="order-4471")) == 1
    assert agent.store.get(wait.wait_id).status == "resumed"


# ============================================================== end to end, on moto
def test_the_full_aws_wiring_parks_schedules_and_resumes() -> None:
    """Everything but Lambda itself: DynamoDB, an SQS FIFO entry point, and a real
    EventBridge schedule armed and then deleted."""
    queue_url, queue_arn = make_fifo_queue()
    announcements_url, _ = make_fifo_queue("announcements.fifo")
    boto3.client("scheduler", region_name=REGION).create_schedule_group(Name="agent-wait")
    scheduler = SchedulerAnnounce(
        "agent-wait",
        queue_arn=queue_arn,
        role_arn="arn:aws:iam::123456789012:role/agent-wait-scheduler",
        queue_url=queue_url,
        region_name=REGION,
    )
    watcher = InMemoryAnnounce()
    agent = Agent(
        announce=[watcher, SqsAnnounce(announcements_url, region_name=REGION), scheduler],
        queue_url=queue_url,
    )

    agent.handler(agent.event(START_BODY), None)

    # the world was told, and the timeout was armed
    [announced] = received(announcements_url)
    assert announced["type"] == "wait.created"
    assert announced["reply_to"] == {"kind": "sqs", "url": queue_url}
    schedules = boto3.client("scheduler", region_name=REGION).list_schedules(GroupName="agent-wait")
    assert [s["Name"] for s in schedules["Schedules"]] == [announced["wait_id"]]

    # now answer at the agent's own entry point
    approval = {
        "token": announced["token"],
        "action": "approve",
        "payload": {"note": "ok"},
        "answer_id": "click-9f1",
    }
    agent.handler(agent.event(approval), None)

    assert PAYMENTS_CALLED == ["order-4471"]
    assert (
        boto3.client("scheduler", region_name=REGION).list_schedules(GroupName="agent-wait")["Schedules"]
        == []
    ), "answering deletes the schedule"


# ============================================================== the heartbeat
class SlowGraph:
    """A graph that takes long enough for the heartbeat to tick -- which is the case the
    heartbeat exists for: a model call outlasting the visibility timeout."""

    def __init__(self, inner: Any, delay: float = 0.25) -> None:
        self._inner = inner
        self._delay = delay

    def invoke(self, *args: Any, **kwargs: Any) -> Any:
        time.sleep(self._delay)
        return self._inner.invoke(*args, **kwargs)

    def get_state(self, *args: Any, **kwargs: Any) -> Any:
        return self._inner.get_state(*args, **kwargs)


def test_the_heartbeat_extends_visibility_while_the_graph_runs() -> None:
    calls: list[dict[str, Any]] = []

    class RecordingSqs:
        def change_message_visibility(self, **kwargs: Any) -> None:
            calls.append(kwargs)

    agent = Agent()
    handler = make_run_handler(
        SlowGraph(agent.graph),
        agent.runtime,
        sqs_client=RecordingSqs(),
        heartbeat=True,
        heartbeat_seconds=0.05,
        visibility_seconds=120,
    )

    handler(agent.event(START_BODY), None)

    assert calls, "the heartbeat must fire while a graph is running"
    assert calls[0]["VisibilityTimeout"] == 120
    assert calls[0]["ReceiptHandle"].startswith("rh-")


def test_a_broken_heartbeat_does_not_fail_the_run() -> None:
    class BrokenSqs:
        def change_message_visibility(self, **kwargs: Any) -> None:
            raise RuntimeError("message is gone")

    agent = Agent()
    handler = make_run_handler(
        SlowGraph(agent.graph, 0.1),
        agent.runtime,
        sqs_client=BrokenSqs(),
        heartbeat=True,
        heartbeat_seconds=0.02,
    )

    assert handler(agent.event(START_BODY), None) == {"batchItemFailures": []}


# ============================================================== the sweeper lambda
def test_the_sweep_handler_repairs_an_unannounced_wait() -> None:
    from agent_wait import FailingAnnounce

    agent = Agent()
    agent.runtime.announce.adapters = [FailingAnnounce()]
    agent.handler(agent.event(START_BODY), None)
    wait = agent.store.find(thread_id="order-4471")[0]
    assert wait.notified_at is None

    agent.runtime.announce.adapters = [agent.announce]
    counts = make_sweep_handler(agent.runtime)({}, None)

    assert counts["announced"] == 1
    assert agent.store.get(wait.wait_id).notified_at is not None
