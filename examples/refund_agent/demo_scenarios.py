"""The four scenarios against a real deployed stack.

    uv run python examples/refund_agent/demo_scenarios.py --stack agent-wait-poc-ks

Nothing here is mocked. Messages go onto the real SQS FIFO queue, a real Lambda runs a
real LangGraph graph against a real DynamoDB checkpointer, the questions land in a real
DynamoDB approvals table and on a real SNS topic, and the refund counter is a real
DynamoDB item this script reads from *outside* the Lambda -- because an in-memory list
proves nothing about a process you are not inside.

## This script is the consumer

The library does not receive answers, so this script is not just an observer: it plays
the part of the consumer. It reads the envelope off the announcements queue, fills in
`reply_with`, posts it back, and -- in scenario B -- enforces the timeout itself by
sweeping the approvals table. That is the point. **If this script can do it in forty
lines, the case for the library doing it is weak.**

Two things are *simulated* rather than induced, and it is worth being straight about
which:

* **The crashes.** You cannot kill a managed Lambda half way through from out here. This
  script produces the same *input* a crash produces -- a redelivered start, an answer
  posted twice -- and asserts the same invariant.
* **Scenario D's lost announce.** Rather than breaking SNS, the script deletes the row
  from the approvals table, which is the state a crash between the run and the announce
  leaves behind. The redelivery then has to repair it.

Everything else -- the double answer, the stale answer, an answer for an unknown
interrupt, the real deadline being enforced -- happens for real.
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
    """Every check, with its outcome, so the report is a transcript rather than a claim."""

    entries: list[dict[str, Any]] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    def note(self, scenario: str, message: str, **extra: Any) -> None:
        self.entries.append({"at": now(), "scenario": scenario, "note": message, **extra})
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
        self.region = region
        self.stack_name = stack_name
        cfn = boto3.client("cloudformation", region_name=region)
        outputs = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]
        self.out = {o["OutputKey"]: o["OutputValue"] for o in outputs}

        self.sqs = boto3.client("sqs", region_name=region)
        self.ddb = boto3.resource("dynamodb", region_name=region)
        self.approvals = self.ddb.Table(self.out["ApprovalsTableName"])

    # ---------------------------------------------------------------- sending
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

    # ---------------------------------------------------------------- observing
    def open_rows(self, thread_id: str) -> list[dict[str, Any]]:
        from boto3.dynamodb.conditions import Key

        items = self.approvals.query(KeyConditionExpression=Key("pk").eq(f"THREAD#{thread_id}")).get(
            "Items", []
        )
        return [i for i in items if i.get("status") == "open"]

    def await_rows(
        self, thread_id: str, count: int, *, status: str = "open", timeout: float = 90.0
    ) -> list[dict[str, Any]]:
        from boto3.dynamodb.conditions import Key

        deadline = time.time() + timeout
        seen: list[dict[str, Any]] = []
        while time.time() < deadline:
            items = self.approvals.query(KeyConditionExpression=Key("pk").eq(f"THREAD#{thread_id}")).get(
                "Items", []
            )
            seen = [i for i in items if i.get("status") == status]
            if len(seen) >= count:
                return seen
            time.sleep(2)
        return seen

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
        """Read the SNS-fed queue: exactly what an approvals consumer would."""
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
    """The happy path, then every wrong answer a real system receives."""
    name = "A"
    thread = f"order-a-{uuid.uuid4().hex[:6]}"
    journal.note(name, f"starting a 41000 refund on {thread}")

    dep.start(thread, 41000)
    envelopes = dep.announcements(until=1, timeout=90)
    journal.check(name, "the question was announced", len(envelopes) == 1, len(envelopes))
    if not envelopes:
        return
    envelope = envelopes[0]

    journal.check(name, "the envelope carries a reply stub", "reply_with" in envelope)
    journal.check(
        name,
        "the stub names the interrupt to resume",
        envelope["reply_with"].get("interrupt_id") == envelope["interrupt_id"],
    )
    journal.check(name, "no refund before an answer", dep.refund_count(thread) == 0)

    rows = dep.await_rows(thread, 1)
    journal.check(name, "the question is queryable as an open row", len(rows) == 1, len(rows))

    # -- a redelivered start must not ask a second time
    dep.start(thread, 41000)
    time.sleep(15)
    rows = dep.await_rows(thread, 1)
    journal.check(name, "a redelivered start does not open a second question", len(rows) == 1, len(rows))

    # -- approve
    dep.reply(envelope, {"action": "approve", "note": "within budget"})
    journal.check(name, "exactly one refund", dep.await_refund_count(thread, 1) == 1)

    closed = dep.await_rows(thread, 1, status="closed")
    journal.check(name, "the question was closed", len(closed) == 1, len(closed))

    # -- the same answer again, a different answer, and an answer for nothing
    dep.reply(envelope, {"action": "approve", "note": "within budget"})
    dep.reply(envelope, {"action": "reject", "reason": "changed my mind"})
    dep.send(thread, {"thread_id": thread, "interrupt_id": "not-a-real-id", "answer": {"action": "approve"}})
    time.sleep(20)

    journal.check(
        name,
        "still exactly one refund after three stale answers",
        dep.refund_count(thread) == 1,
        dep.refund_count(thread),
    )


# ============================================================== scenario B
def scenario_b(dep: Deployment, journal: Journal, *, timeout_seconds: int) -> None:
    """The timeout, enforced by the consumer -- because nothing else will.

    The envelope carries an absolute `expires_at` and the default the graph author
    declared; the sweep below is the entire enforcement mechanism, and it lives here
    rather than in the library.
    """
    name = "B"
    thread = f"order-b-{uuid.uuid4().hex[:6]}"
    journal.note(name, f"starting {thread}; nobody will answer it")

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

    journal.note(name, f"waiting up to {timeout_seconds}s for the deadline to pass")
    deadline = envelope["expires_at"]
    applied = False
    stop = time.time() + timeout_seconds
    while time.time() < stop:
        # The sweep: every open row whose deadline has passed gets its default sent.
        # An approvals table makes this a query; on the SNS-only path it would be your
        # own store of open questions.
        for row in dep.open_rows(thread):
            if str(row.get("expires_at") or "") <= now():
                dep.reply(
                    {"reply_with": row["reply_with"], "interrupt_id": row["interrupt_id"]}, row["default"]
                )
                applied = True
        if applied:
            break
        time.sleep(5)

    journal.check(name, f"the deadline {deadline} passed and the sweep fired", applied)

    closed = dep.await_rows(thread, 1, status="closed", timeout=90)
    journal.check(name, "the question was closed by the default", len(closed) == 1, len(closed))
    journal.check(name, "no refund", dep.refund_count(thread) == 0, dep.refund_count(thread))

    # A late human answer, after the default already landed.
    dep.reply(envelope, {"action": "approve"})
    time.sleep(20)
    journal.check(name, "a late approval changes nothing", dep.refund_count(thread) == 0)


# ============================================================== scenario C
def scenario_c(dep: Deployment, journal: Journal) -> None:
    """The refund happened; the answer is delivered again.

    The handler's `is_still_open()` check refuses it by reading the graph's own state.
    This scenario exists to prove that holds under a real queue with real redeliveries.
    """
    name = "C"
    thread = f"order-c-{uuid.uuid4().hex[:6]}"
    journal.note(name, f"starting {thread}")

    dep.start(thread, 41000)
    envelopes = dep.announcements(until=1, timeout=90)
    if not journal.check(name, "the question was announced", len(envelopes) == 1):
        return
    envelope = envelopes[0]

    dep.reply(envelope, {"action": "approve"})
    journal.check(name, "the refund happened", dep.await_refund_count(thread, 1) == 1)

    journal.note(name, "redelivering the identical approval, five times")
    for _ in range(5):
        dep.reply(envelope, {"action": "approve"})
    time.sleep(25)

    count = dep.refund_count(thread)
    journal.check(name, "still exactly one refund", count == 1, count)


# ============================================================== scenario D
def scenario_d(dep: Deployment, journal: Journal) -> None:
    """The announce was lost. The redelivery repairs it.

    With no store and no sweeper, this is the only recovery path there is, so it had
    better work: the row is deleted to simulate a crash before the announce, and a
    redelivered start message must put it back -- with the *same* interrupt id, or a
    consumer would see a second question.
    """
    name = "D"
    thread = f"order-d-{uuid.uuid4().hex[:6]}"
    journal.note(name, f"starting {thread}")

    dep.start(thread, 41000)
    rows = dep.await_rows(thread, 1)
    if not journal.check(name, "the question was announced", len(rows) == 1):
        return
    original = rows[0]

    journal.note(name, "deleting the row: the state a crash before the announce leaves")
    dep.approvals.delete_item(Key={"pk": original["pk"], "sk": original["sk"]})
    journal.check(name, "nobody can see the question now", dep.open_rows(thread) == [])

    journal.note(name, "redelivering the start message")
    dep.start(thread, 41000)

    repaired = dep.await_rows(thread, 1, timeout=90)
    journal.check(name, "the question is visible again", len(repaired) == 1, len(repaired))
    if repaired:
        journal.check(
            name,
            "and it is the same question, not a second one",
            repaired[0]["interrupt_id"] == original["interrupt_id"],
            repaired[0]["interrupt_id"],
        )

        dep.drain_announcements()
        dep.reply({"reply_with": repaired[0]["reply_with"]}, {"action": "approve"})
        journal.check(name, "and it is answerable", dep.await_refund_count(thread, 1) == 1)


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
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--only", default="", help="run a subset, e.g. --only ac")
    args = parser.parse_args()

    dep = Deployment(args.stack, args.region)
    journal = Journal()

    scenarios = {
        "a": lambda: scenario_a(dep, journal),
        "b": lambda: scenario_b(dep, journal, timeout_seconds=args.timeout_seconds),
        "c": lambda: scenario_c(dep, journal),
        "d": lambda: scenario_d(dep, journal),
    }
    selected = args.only.lower() or "abcd"

    for key in selected:
        if key not in scenarios:
            continue
        print(f"\n=== scenario {key.upper()} ===")
        dep.drain_announcements()
        scenarios[key]()

    journal.check("Z", "the dead-letter queue is empty", _dlq_empty(dep))

    REPORTS.mkdir(exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = REPORTS / f"e2e-{stamp}.json"
    checks = [e for e in journal.entries if "check" in e]
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
