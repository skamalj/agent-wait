"""The four scenarios of REQUIREMENTS section 13, against a real deployed stack.

    uv run python examples/refund_agent/demo_scenarios.py --stack agent-wait-poc-ks

Nothing here is mocked. Messages go onto the real SQS FIFO queue, a real Lambda runs a
real LangGraph graph against a real DynamoDB checkpointer, EventBridge Scheduler really
does hold the timeout, and the refund counter is a real DynamoDB item that this script
reads from outside the Lambda -- because an in-memory list proves nothing about a process
you are not inside.

Two things are *simulated* rather than induced, and it is worth being straight about
which:

* **The crashes.** You cannot kill a managed Lambda half way through from out here. What
  this script does instead is produce the same input the crash produces -- a redelivered
  message, an answer applied twice -- and assert the same invariant. The crashes
  themselves are induced properly in the local suite, where `CrashStore` kills the
  process at every individual store write.
* **Scenario D's failed announce.** Rather than breaking SNS, the script strips
  `notified_at` from a live wait and deletes its schedule, which is the state a crash
  between the create and the announce leaves behind. The sweeper then has to notice.

Everything else -- idempotency, the double click, the stale reject, the forged token, the
late timer, the real timeout firing -- happens for real.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3

REPORTS = Path(__file__).resolve().parents[2] / "reports"


def now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Journal:
    """Every observation, with a timestamp, for the report."""

    lines: list[dict[str, Any]] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    def note(self, scenario: str, message: str, **extra: Any) -> None:
        entry = {"at": now(), "scenario": scenario, "note": message, **extra}
        self.lines.append(entry)
        print(f"  [{entry['at']}] {message}")

    def check(self, scenario: str, description: str, ok: bool, detail: Any = "") -> bool:
        status = "PASS" if ok else "FAIL"
        entry = {
            "at": now(),
            "scenario": scenario,
            "check": description,
            "status": status,
            "detail": str(detail),
        }
        self.lines.append(entry)
        print(f"  [{entry['at']}] {status}  {description}" + (f"  ({detail})" if detail else ""))
        if not ok:
            self.failures.append(f"{scenario}: {description} ({detail})")
        return ok


class Deployment:
    """Talks to the deployed stack the way any operator or consumer would."""

    def __init__(self, stack_name: str, region: str) -> None:
        self.region = region
        self.stack_name = stack_name
        cfn = boto3.client("cloudformation", region_name=region)
        outputs = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]
        self.out = {o["OutputKey"]: o["OutputValue"] for o in outputs}

        self.sqs = boto3.client("sqs", region_name=region)
        self.ddb = boto3.resource("dynamodb", region_name=region)
        self.scheduler = boto3.client("scheduler", region_name=region)
        self.table = self.ddb.Table(self.out["TableName"])

    # ---------------------------------------------------------------- sending
    def start(self, thread_id: str, amount: int, message_id: str) -> None:
        body = {
            "thread_id": thread_id,
            "input": {"order_id": thread_id, "amount": amount},
            "message_id": message_id,
        }
        self.sqs.send_message(
            QueueUrl=self.out["QueueUrl"],
            MessageBody=json.dumps(body),
            MessageGroupId=thread_id,
            MessageDeduplicationId=uuid.uuid4().hex,
        )

    def answer(self, thread_id: str, body: dict[str, Any]) -> None:
        self.sqs.send_message(
            QueueUrl=self.out["QueueUrl"],
            MessageBody=json.dumps(body),
            MessageGroupId=thread_id,
            MessageDeduplicationId=uuid.uuid4().hex,
        )

    # ---------------------------------------------------------------- observing
    def waits_for(self, thread_id: str) -> list[dict[str, Any]]:
        from boto3.dynamodb.conditions import Key

        return self.table.query(IndexName="gsi1", KeyConditionExpression=Key("gsi1pk").eq(thread_id)).get(
            "Items", []
        )

    def await_wait(
        self, thread_id: str, predicate: Any, *, timeout: float = 90.0, label: str = ""
    ) -> dict[str, Any] | None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            for item in self.waits_for(thread_id):
                if predicate(item):
                    return item
            time.sleep(2)
        print(f"    (timed out after {timeout:.0f}s waiting for {label or 'a wait'})")
        return None

    def refund_count(self, order_id: str) -> int:
        item = self.table.get_item(Key={"pk": f"REFUND#{order_id}", "sk": "#"}).get("Item")
        return int(item["calls"]) if item else 0

    def await_refund_count(self, order_id: str, expected: int, *, timeout: float = 60.0) -> int:
        deadline = time.time() + timeout
        seen = self.refund_count(order_id)
        while time.time() < deadline and seen < expected:
            time.sleep(2)
            seen = self.refund_count(order_id)
        return seen

    def announcements(self, *, timeout: float = 60.0, until: int = 1) -> list[dict[str, Any]]:
        """Read the SNS-fed queue: exactly what an approvals UI would consume."""
        collected: list[dict[str, Any]] = []
        deadline = time.time() + timeout
        while time.time() < deadline and len(collected) < until:
            response = self.sqs.receive_message(
                QueueUrl=self.out["AnnouncementsQueueUrl"],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,
            )
            for message in response.get("Messages", []):
                collected.append(json.loads(message["Body"]))
                self.sqs.delete_message(
                    QueueUrl=self.out["AnnouncementsQueueUrl"], ReceiptHandle=message["ReceiptHandle"]
                )
        return collected

    def schedules(self) -> list[str]:
        return [
            s["Name"]
            for s in self.scheduler.list_schedules(GroupName=self.out["ScheduleGroup"]).get("Schedules", [])
        ]

    def await_schedule_gone(self, name: str, *, timeout: float = 60.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if name not in self.schedules():
                return True
            time.sleep(3)
        return False

    def drain_announcements(self) -> None:
        while True:
            response = self.sqs.receive_message(
                QueueUrl=self.out["AnnouncementsQueueUrl"], MaxNumberOfMessages=10
            )
            messages = response.get("Messages", [])
            if not messages:
                return
            for message in messages:
                self.sqs.delete_message(
                    QueueUrl=self.out["AnnouncementsQueueUrl"], ReceiptHandle=message["ReceiptHandle"]
                )


# ============================================================== scenario A
def scenario_a(dep: Deployment, journal: Journal) -> None:
    """Crash-before-register, then every wrong answer a real system receives."""
    name = "A"
    thread = f"order-a-{uuid.uuid4().hex[:6]}"
    journal.note(name, f"thread {thread}: start message, amount 41000")

    message_id = f"evt-{thread}"
    dep.start(thread, 41000, message_id)
    wait = dep.await_wait(thread, lambda i: i.get("notified_at"), label="the wait to be announced")
    if wait is None:
        journal.check(name, "the wait was created and announced", False, "nothing appeared")
        return
    journal.check(name, "one wait created and announced", True, wait["wait_id"])

    envelopes = dep.announcements(until=1)
    created = [e for e in envelopes if e["type"] == "wait.created"]
    journal.check(name, "the world was told, once", len(created) == 1, [e["type"] for e in envelopes])
    if not created:
        return
    envelope = created[0]
    journal.check(
        name,
        "the envelope advertises the agent's own queue",
        envelope["reply_to"]["url"] == dep.out["QueueUrl"],
    )
    journal.check(name, "a timeout schedule was armed", envelope["wait_id"] in dep.schedules())

    # -- the redelivery the crash produces
    journal.note(name, "redelivering the identical start message (what a crash looks like)")
    dep.start(thread, 41000, message_id)
    time.sleep(12)
    journal.check(name, "still exactly one wait", len(dep.waits_for(thread)) == 1, len(dep.waits_for(thread)))
    journal.check(name, "no second announcement", dep.announcements(timeout=8, until=1) == [])

    # -- the double click
    approval = {
        "token": envelope["token"],
        "action": "approve",
        "payload": {"note": "within budget"},
        "actor": "priya@corp",
        "answer_id": "click-9f1",
    }
    journal.note(name, "approving")
    dep.answer(thread, approval)
    journal.check(name, "the refund happened exactly once", dep.await_refund_count(thread, 1) == 1)

    journal.note(name, "the same click again (double click / redelivery)")
    dep.answer(thread, approval)
    time.sleep(12)
    journal.check(name, "still exactly one refund", dep.refund_count(thread) == 1, dep.refund_count(thread))

    journal.note(name, "a different decision, arriving late")
    dep.answer(thread, {**approval, "action": "reject", "answer_id": "click-different"})

    journal.note(name, "a tampered token")
    forged = envelope["token"][:-1] + ("A" if envelope["token"][-1] != "A" else "B")
    dep.answer(thread, {**approval, "token": forged, "answer_id": "evil"})

    journal.note(name, "a timeout arriving after the answer")
    dep.answer(
        thread,
        {"token": envelope["token"], "action": "timeout", "answer_id": f"timeout:{envelope['wait_id']}"},
    )
    time.sleep(15)

    journal.check(name, "STILL exactly one refund", dep.refund_count(thread) == 1, dep.refund_count(thread))
    settled = dep.waits_for(thread)[0]
    journal.check(name, "the wait ended resumed", settled["status"] == "resumed", settled["status"])
    journal.check(
        name, "the action recorded is the first one", settled["action"] == "approve", settled["action"]
    )
    journal.check(name, "the schedule was deleted on answer", dep.await_schedule_gone(envelope["wait_id"]))
    journal.check(name, "nothing reached the dead-letter queue", _dlq_empty(dep))


# ============================================================== scenario B
def scenario_b(dep: Deployment, journal: Journal, *, timeout_seconds: int) -> None:
    """Nobody answers. The schedule fires and the declared default is applied."""
    name = "B"
    thread = f"order-b-{uuid.uuid4().hex[:6]}"
    journal.note(name, f"thread {thread}: start, then wait out the {timeout_seconds}s timeout")

    dep.start(thread, 41000, f"evt-{thread}")
    wait = dep.await_wait(thread, lambda i: i.get("notified_at"), label="the wait")
    if wait is None:
        journal.check(name, "the wait was created", False)
        return
    envelope = next((e for e in dep.announcements(until=1) if e["type"] == "wait.created"), None)
    if envelope is None:
        journal.check(name, "the wait was announced", False)
        return
    journal.check(name, "a schedule is holding the timeout", envelope["wait_id"] in dep.schedules())
    journal.note(name, f"expires_at is {envelope['expires_at']}; waiting for the scheduler")

    settled = dep.await_wait(
        thread,
        lambda i: i["status"] in ("expired", "resumed"),
        timeout=timeout_seconds + 180,
        label="the timeout to fire",
    )
    if settled is None:
        journal.check(name, "the timeout fired on its own", False, "the wait is still pending")
        return

    journal.check(name, "the timeout fired with no human involved", True, settled["status"])
    journal.check(name, "the action recorded is 'timeout'", settled["action"] == "timeout", settled["action"])
    journal.check(name, "no refund was issued", dep.refund_count(thread) == 0, dep.refund_count(thread))

    journal.note(name, "approving after the timeout")
    dep.answer(
        thread,
        {
            "token": envelope["token"],
            "action": "approve",
            "payload": {},
            "answer_id": "click-too-late",
        },
    )
    time.sleep(15)
    journal.check(name, "the late approval changed nothing", dep.refund_count(thread) == 0)
    final = dep.waits_for(thread)[0]
    journal.check(
        name, "the wait ended resumed on the default", final["status"] == "resumed", final["status"]
    )


# ============================================================== scenario C
def scenario_c(dep: Deployment, journal: Journal) -> None:
    """The refund happened, then the handler died before acking. SQS redelivers."""
    name = "C"
    thread = f"order-c-{uuid.uuid4().hex[:6]}"
    journal.note(name, f"thread {thread}: park, approve, then redeliver the same approval")

    dep.start(thread, 41000, f"evt-{thread}")
    if dep.await_wait(thread, lambda i: i.get("notified_at"), label="the wait") is None:
        journal.check(name, "the wait was created", False)
        return
    envelope = next((e for e in dep.announcements(until=1) if e["type"] == "wait.created"), None)
    if envelope is None:
        journal.check(name, "the wait was announced", False)
        return

    approval = {
        "token": envelope["token"],
        "action": "approve",
        "payload": {"note": "ok"},
        "answer_id": "click-c",
    }
    dep.answer(thread, approval)
    journal.check(name, "the refund was issued", dep.await_refund_count(thread, 1) == 1)

    journal.note(name, "redelivering the identical approval, three times")
    for _ in range(3):
        dep.answer(thread, approval)
    time.sleep(20)

    journal.check(
        name, "the refund count is still 1", dep.refund_count(thread) == 1, dep.refund_count(thread)
    )
    journal.check(name, "nothing reached the dead-letter queue", _dlq_empty(dep))


# ============================================================== scenario D
def scenario_d(dep: Deployment, journal: Journal) -> None:
    """A wait nobody was told about, and no timer will fire for. The sweeper repairs it."""
    name = "D"
    thread = f"order-d-{uuid.uuid4().hex[:6]}"
    journal.note(name, f"thread {thread}: park, then simulate a crash between create and announce")

    dep.start(thread, 41000, f"evt-{thread}")
    wait = dep.await_wait(thread, lambda i: i.get("notified_at"), label="the wait")
    if wait is None:
        journal.check(name, "the wait was created", False)
        return
    dep.drain_announcements()
    wait_id = wait["wait_id"]

    # The state a crash between the DynamoDB write and the announce leaves behind.
    dep.table.update_item(
        Key={"pk": f"WAIT#{wait_id}", "sk": "#"},
        UpdateExpression="REMOVE notified_at",
    )
    with contextlib.suppress(dep.scheduler.exceptions.ResourceNotFoundException):
        dep.scheduler.delete_schedule(Name=wait_id, GroupName=dep.out["ScheduleGroup"])
    journal.check(
        name, "the wait is now invisible: no notified_at, no schedule", wait_id not in dep.schedules()
    )

    journal.note(name, "waiting for the one-minute sweeper")
    repaired = dep.await_wait(
        thread, lambda i: i.get("notified_at"), timeout=180, label="the sweeper to repair it"
    )
    journal.check(name, "the sweeper re-announced it", repaired is not None)
    if repaired is None:
        return
    # Read the re-announcement once and hold on to it. The token inside is the whole
    # point of the repair, and a second receive_message would find the queue drained.
    reannounced = [e for e in dep.announcements(until=1) if e["type"] == "wait.created"]
    journal.check(name, "the world was told after all", len(reannounced) >= 1)
    journal.check(name, "and the timeout was armed", _await(lambda: wait_id in dep.schedules(), 90), wait_id)
    if not reannounced:
        return

    # The repair only counts if the wait is answerable again.
    dep.answer(
        thread,
        {
            "token": reannounced[0]["token"],
            "action": "approve",
            "payload": {},
            "answer_id": "click-d",
        },
    )
    journal.check(name, "the repaired wait is answerable", dep.await_refund_count(thread, 1) == 1)
    journal.check(name, "and it refunded exactly once", dep.refund_count(thread) == 1)


# ============================================================== helpers
def _await(predicate: Any, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(3)
    return False


def _dlq_empty(dep: Deployment) -> bool:
    attributes = dep.sqs.get_queue_attributes(
        QueueUrl=dep.out["DlqUrl"], AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    return attributes["ApproximateNumberOfMessages"] == "0"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack", default="agent-wait-poc")
    parser.add_argument("--region", default="ap-south-1")
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--only", nargs="*", choices=["A", "B", "C", "D"])
    arguments = parser.parse_args()

    dep = Deployment(arguments.stack, arguments.region)
    journal = Journal()
    started = now()
    print(f"agent-wait end-to-end against {arguments.stack} in {arguments.region}, {started}\n")
    dep.drain_announcements()

    chosen = arguments.only or ["A", "B", "C", "D"]
    for label in chosen:
        print(f"\n--- scenario {label} " + "-" * 50)
        dep.drain_announcements()
        if label == "A":
            scenario_a(dep, journal)
        elif label == "B":
            scenario_b(dep, journal, timeout_seconds=arguments.timeout_seconds)
        elif label == "C":
            scenario_c(dep, journal)
        elif label == "D":
            scenario_d(dep, journal)

    REPORTS.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    log = REPORTS / f"e2e-{stamp}.json"
    log.write_text(
        json.dumps(
            {
                "stack": arguments.stack,
                "region": arguments.region,
                "started": started,
                "finished": now(),
                "entries": journal.lines,
                "failures": journal.failures,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    checks = [entry for entry in journal.lines if "check" in entry]
    passed = sum(1 for entry in checks if entry["status"] == "PASS")
    print(f"\n{passed}/{len(checks)} checks passed. Evidence: {log.relative_to(REPORTS.parent)}")
    if journal.failures:
        print("\nFAILURES:")
        for failure in journal.failures:
            print(f"  - {failure}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
