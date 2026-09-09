"""
Stand-ins for the AWS services agent-wait-aws targets. Each mimics the ONE property
the design relies on: DynamoDB conditional writes, Scheduler one-shot timers keyed by
name, EventBridge fan-out, SQS FIFO per-group exclusivity + visibility, Lambda invoke.
Swap for boto3 in the real package; the port shapes are identical.
"""
from __future__ import annotations

import copy
import time
import uuid
from collections import defaultdict, deque
from dataclasses import replace
from typing import Any, Callable

from .core import Wait, WaitEvent


class DynamoWaitStore:                       # DynamoDB: PK wait_id, GSI thread_id, GSI status
    def __init__(self):
        self.items: dict[str, Wait] = {}
        self.by_key: dict[str, str] = {}

    def create(self, wait: Wait):            # TransactWrite: put item + put idempotency marker, both conditional
        if wait.idempotency_key in self.by_key:
            return copy.deepcopy(self.items[self.by_key[wait.idempotency_key]]), False
        self.items[wait.wait_id] = copy.deepcopy(wait); self.by_key[wait.idempotency_key] = wait.wait_id
        return copy.deepcopy(wait), True

    def get(self, wait_id):
        w = self.items.get(wait_id); return copy.deepcopy(w) if w else None

    def find(self, *, status=None, thread_id=None):
        return [copy.deepcopy(w) for w in self.items.values()
                if (status is None or w.status == status) and (thread_id is None or w.thread_id == thread_id)]

    def transition(self, wait_id, *, expect, to, **fields):   # UpdateItem with ConditionExpression status = :expect
        w = self.items.get(wait_id)
        if not w or w.status != expect:
            return False
        self.items[wait_id] = replace(w, status=to, version=w.version + 1, **fields)
        return True

    def set_refs(self, wait_id, **fields):
        self.items[wait_id] = replace(self.items[wait_id], **fields)


class EventBridgeScheduler:                  # one-time schedule, Name = wait_id (idempotent), DELETE after completion
    def __init__(self, target: Callable[[dict], Any]):
        self.schedules: dict[str, float] = {}; self.target = target

    def schedule(self, wait_id, fire_at):
        self.schedules[wait_id] = fire_at; return f"sched/{wait_id}"

    def cancel(self, timer_ref):
        self.schedules.pop(timer_ref.split("/", 1)[1], None)

    def tick(self, now):                     # the clock advancing: fire what is due (Lambda target)
        for wait_id, at in list(self.schedules.items()):
            if now >= at:
                del self.schedules[wait_id]
                self.target({"source": "aws.scheduler", "wait_id": wait_id})


class EventBridgeBus:                        # rules fan out to SNS / Slack Lambda / API destinations
    def __init__(self, subscribers: list[Callable[[WaitEvent], Any]] | None = None):
        self.events: list[WaitEvent] = []; self.subscribers = subscribers or []

    def emit(self, event):
        self.events.append(event)
        for s in self.subscribers: s(event)


class SqsFifo:                               # MessageGroupId = thread_id → one in-flight message per thread
    def __init__(self):
        self.groups: dict[str, deque] = defaultdict(deque); self.inflight: dict[str, tuple[str, dict, float]] = {}
        self.dlq: list[dict] = []; self.receive_counts: dict[str, int] = defaultdict(int)

    def send(self, group: str, body: dict, dedup_id: str | None = None):
        body = {**body, "message_id": dedup_id or uuid.uuid4().hex}
        self.groups[group].append(body)

    def receive(self, visibility_s=30):
        for group, q in self.groups.items():
            if q and group not in self.inflight:
                body = q.popleft(); self.inflight[group] = (body["message_id"], body, time.time() + visibility_s)
                self.receive_counts[body["message_id"]] += 1
                return group, body
        return None

    def delete(self, group):                 # ack
        self.inflight.pop(group, None)

    def crash_consumer(self, group):         # visibility timeout elapses → message returns to head of its group
        mid, body, _ = self.inflight.pop(group)
        if self.receive_counts[mid] >= 3:
            self.dlq.append(body)
        else:
            self.groups[group].appendleft(body)


class SqsResumer:
    serialises_per_thread = True

    def __init__(self, queue: SqsFifo):
        self.queue = queue

    def resume(self, wait, resume_input):
        self.queue.send(wait.thread_id, {"kind": "resume", "thread_id": wait.thread_id,
                                          "wait_id": wait.wait_id, **resume_input},
                        dedup_id=f"resume-{wait.wait_id}-{wait.version}")
        return "QUEUED"


class LambdaRuntime:                         # invokes a handler; lets the simulation kill it mid-way
    def __init__(self, name, handler):
        self.name, self.handler = name, handler

    def invoke(self, event, *, crash_after: str | None = None):
        return self.handler(event, {"function_name": self.name, "crash_after": crash_after})
