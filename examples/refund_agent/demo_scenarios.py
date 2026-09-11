"""Three scenarios against a real deployed stack.

    uv run python examples/refund_agent/demo_scenarios.py --stack agent-wait-poc-ks

Nothing here is mocked. Messages go onto the real SQS FIFO queue, a real Lambda runs a
real LangGraph graph against a real DynamoDB checkpointer, the questions land in a real
approvals table and on a real SNS topic, and the refund counter is a real DynamoDB item
this script reads from *outside* the Lambda -- because an in-memory list proves nothing
about a process you are not inside.

This script plays the consumer: it reads the envelope off the announcements queue, fills
in `reply_with`, posts it back, and in scenario B decides for itself that the deadline
has passed and sends the default. That is the whole return leg, and it is forty lines.

The crashes in the original design cannot be induced on a managed Lambda from out here;
what is exercised instead is every redelivery a crash produces -- the same answer twice,
a different answer late -- and the invariant that one refund is one refund.
"""

from __future__ import annotations

import argparse
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
    entries: list[dict[str, Any]] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    def note(self, scenario: str, message: str) -> None:
        self.entries.append({"at": now(), "scenario": scenario, "note": message})
        print(f"  [{scenario}] {message}")

    def check(self, scenario: str, description: str, ok: bool, detail: Any = "") -> bool:
        self.entries.append(
            {"at": now(), "scenario": scenario, "check": description, "ok": bool(ok), "detail": detail}
        )
        print(f"  [{scenario}] {'PASS' if ok else 'FAIL'}  {description}  {detail if detail else ''}")
        if not ok:
            self.failures.append(f"{scenario}: {description} ({detail})")
        return bool(ok)


class Deployment:
    """Talks to the deployed stack the way a consumer would."""

    def __init__(self, stack_name: str, region: str) -> None:
        cfn = boto3.client("cloudformation", region_name=region)
        outputs = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]
        self.out = {o["OutputKey"]: o["OutputValue"] for o in outputs}
        self.sqs = boto3.client("sqs", region_name=region)
        self.approvals = boto3.resource("dynamodb", region_name=region).Table(self.out["ApprovalsTableName"])

    def send(self, thread_id: str, body: dict[str, Any]) -> None:
        self.sqs.send_message(
            QueueUrl=self.out["QueueUrl"],
            MessageBody=json.dumps(body),
            MessageGroupId=thread_id,
            MessageDeduplicationId=uuid.uuid4().hex,
        )

    def start(self, thread_id: str, amount: int) -> None:
        self.send(thread_id, {"thread_id": thread_id, "input": {"order_id": thread_id, "amount": amount}})

    def reply(self, envelope: dict[str, Any], answer: Any) -> None:
        """Exactly what a UI does: take `reply_with`, fill in `answer`, post it back."""
        body = {**envelope["reply_with"], "answer": answer}
        self.send(str(body["thread_id"]), body)

    def rows(self, thread_id: str) -> list[dict[str, Any]]:
        from boto3.dynamodb.conditions import Key

        return self.approvals.query(KeyConditionExpression=Key("pk").eq(f"THREAD#{thread_id}")).get(
            "Items", []
        )

    def await_rows(self, thread_id: str, count: int, *, timeout: float = 90.0) -> list[dict[str, Any]]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            seen = self.rows(thread_id)
            if len(seen) >= count:
                return seen
            time.sleep(2)
        return self.rows(thread_id)

    def refund_count(self, order_id: str) -> int:
        item = self.approvals.get_item(Key={"pk": f"REFUND#{order_id}", "sk": "#"}).get("Item")
        return int(item["calls"]) if item else 0

    def await_refund_count(self, order_id: str, expected: int, *, timeout: float = 60.0) -> int:
        deadline = time.time() + timeout
        seen = self.refund_count(order_id)
        while time.time() < deadline and seen < expected:
            time.sleep(2)
            seen = self.refund_count(order_id)
        return seen

    def announcements(self, *, timeout: float = 60.0, until: int = 1) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        deadline = time.time() + timeout
        while time.time() < deadline and len(collected) < until:
            response = self.sqs.receive_message(
                QueueUrl=self.out["AnnouncementsQueueUrl"], MaxNumberOfMessages=10, WaitTimeSeconds=5
            )
            for message in response.get("Messages", []):
                collected.append(json.loads(message["Body"]))
                self.sqs.delete_message(
                    QueueUrl=self.out["AnnouncementsQueueUrl"], ReceiptHandle=message["ReceiptHandle"]
                )
        return collected

    def drain_announcements(self) -> None:
        while self.announcements(timeout=1, until=10):
            pass


# ============================================================== scenario A
def scenario_a(dep: Deployment, journal: Journal) -> None:
    """The happy path, then every redelivery a real queue produces."""
    name = "A"
    thread = f"order-a-{uuid.uuid4().hex[:6]}"
    journal.note(name, f"starting a 41000 refund on {thread}")

    dep.start(thread, 41000)
    envelopes = dep.announcements(until=1, timeout=90)
    if not journal.check(name, "the question was announced", len(envelopes) == 1, len(envelopes)):
        return
    envelope = envelopes[0]
    journal.check(
        name,
        "the stub names the question",
        envelope["reply_with"].get("question_id") == envelope["question_id"],
    )
    journal.check(name, "no refund before an answer", dep.refund_count(thread) == 0)
    journal.check(name, "the question is a row in the approvals table", len(dep.await_rows(thread, 1)) == 1)

    dep.reply(envelope, {"action": "approve", "note": "within budget"})
    journal.check(name, "exactly one refund", dep.await_refund_count(thread, 1) == 1)

    journal.note(name, "the same answer again, then a different one, then one for a made-up question")
    dep.reply(envelope, {"action": "approve", "note": "within budget"})
    dep.reply(envelope, {"action": "reject", "reason": "changed my mind"})
    dep.send(thread, {"thread_id": thread, "question_id": "not-a-real-id", "answer": {"action": "approve"}})
    time.sleep(20)
    journal.check(name, "still exactly one refund", dep.refund_count(thread) == 1, dep.refund_count(thread))


# ============================================================== scenario B
def scenario_b(dep: Deployment, journal: Journal) -> None:
    """Nobody answers. The consumer decides the deadline has passed and sends the default.

    Nothing in the stack enforces `expires_at`; this script is the consumer, and this is
    what the consumer does. The graph receives the author's default verbatim.
    """
    name = "B"
    thread = f"order-b-{uuid.uuid4().hex[:6]}"
    dep.start(thread, 41000)
    envelopes = dep.announcements(until=1, timeout=90)
    if not journal.check(name, "the question was announced", len(envelopes) == 1):
        return
    envelope = envelopes[0]
    journal.check(
        name, "the deadline is an absolute instant", bool(envelope["expires_at"]), envelope["expires_at"]
    )
    journal.check(
        name, "the default is published with it", envelope["default"] is not None, envelope["default"]
    )

    journal.note(name, "consumer: deadline passed (as far as this script is concerned); sending the default")
    dep.reply(envelope, envelope["default"])
    time.sleep(20)
    journal.check(name, "no refund", dep.refund_count(thread) == 0, dep.refund_count(thread))

    dep.reply(envelope, {"action": "approve"})
    time.sleep(20)
    journal.check(name, "a late approval changes nothing", dep.refund_count(thread) == 0)


# ============================================================== scenario C
def scenario_c(dep: Deployment, journal: Journal) -> None:
    """The refund happened; the answer is redelivered five times. One refund."""
    name = "C"
    thread = f"order-c-{uuid.uuid4().hex[:6]}"
    dep.start(thread, 41000)
    envelopes = dep.announcements(until=1, timeout=90)
    if not journal.check(name, "the question was announced", len(envelopes) == 1):
        return
    dep.reply(envelopes[0], {"action": "approve"})
    journal.check(name, "the refund happened", dep.await_refund_count(thread, 1) == 1)

    for _ in range(5):
        dep.reply(envelopes[0], {"action": "approve"})
    time.sleep(25)
    journal.check(name, "still exactly one refund", dep.refund_count(thread) == 1, dep.refund_count(thread))


# ============================================================== driver
def _dlq_empty(dep: Deployment) -> bool:
    attributes = dep.sqs.get_queue_attributes(
        QueueUrl=dep.out["DlqUrl"], AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    return int(attributes["ApproximateNumberOfMessages"]) == 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack", default="agent-wait-poc")
    parser.add_argument("--region", default="ap-south-1")
    parser.add_argument("--only", default="", help="run a subset, e.g. --only ac")
    args = parser.parse_args()

    dep = Deployment(args.stack, args.region)
    journal = Journal()
    scenarios = {"a": scenario_a, "b": scenario_b, "c": scenario_c}
    for key in args.only.lower() or "abc":
        if key in scenarios:
            print(f"\n=== scenario {key.upper()} ===")
            dep.drain_announcements()
            scenarios[key](dep, journal)

    journal.check("Z", "the dead-letter queue is empty", _dlq_empty(dep))

    REPORTS.mkdir(exist_ok=True)
    checks = [e for e in journal.entries if "check" in e]
    path = REPORTS / f"e2e-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    path.write_text(
        json.dumps(
            {
                "stack": args.stack,
                "region": args.region,
                "passed": sum(1 for c in checks if c["ok"]),
                "total": len(checks),
                "entries": journal.entries,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\n{sum(1 for c in checks if c['ok'])}/{len(checks)} checks passed -- {path.name}")
    for failure in journal.failures:
        print(f"  FAILED: {failure}")
    return 1 if journal.failures else 0


if __name__ == "__main__":
    sys.exit(main())
