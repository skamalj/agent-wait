"""
End-to-end simulation: LangGraph agent on Lambda, SQS FIFO in front, DynamoDB wait store,
EventBridge Scheduler timers, EventBridge notifications, API Gateway answer endpoint.

    python simulate.py
"""
import json
import re
import sys
import textwrap

from agent_wait import core
from app import lambdas as L
from app.graph import PAYMENTS_CALLED

DAY = 86400


def title(s):    print(f"\n{'─'*100}\n{s}\n{'─'*100}")
def step(s):     print(f"\n  ▸ {s}")
def note(s):     print(textwrap.indent(s, "      "))
def api(token, action, actor, answer_id):
    resp = L.answer_handler({"pathParameters": {"token": token},
                             "body": json.dumps({"action": action, "answer_id": answer_id, "payload": {"note": "via sim"}}),
                             "requestContext": {"authorizer": {"email": actor}}}, {})
    print(f"      [answer λ] POST /w/{token[:22]}… action={action} by={actor} → {resp['statusCode']} {resp['body']}")
    return resp


# EventBridge rule → "email" the approver
def mail_rule(ev: core.WaitEvent):
    if ev.type == "wait.created":
        print(f"      [EventBridge→SNS] to finance: 'Refund of ₹{ev.question['amount']:,} on {ev.question['order_id']} needs approval'")
        print(f"                          approve/reject link: {ev.token_url}")
    else:
        print(f"      [EventBridge] {ev.type} {ev.wait_id} {ev.extra or ''}")
L.bus.subscribers.append(mail_rule)


def token_from_events(thread_id):
    ev = next(e for e in reversed(L.bus.events) if e.type == "wait.created" and e.thread_id == thread_id)
    return ev.token_url.split("/w/")[1]


# ════════════════════════════════════════════════════════════════════════════════════════
title("SCENARIO A · ₹48,000 refund · crash before the wait is registered · double-click approve · resume on another Lambda")

step("1. Order service drops a start message on agent-runs.fifo (MessageGroupId = thread_id)")
L.run_queue.send("order-4471", {"kind": "start", "thread_id": "order-4471",
                                "input": {"order_id": "order-4471", "amount": 48_000}}, dedup_id="evt-return-4471")

step("2. Lambda consumes it, graph runs load_order → review → interrupt(); LangGraph checkpoints.\n"
     "     Then the Lambda DIES before Waiter.register() runs (the worst crash window).")
L.drain_queue(crash_first_after="invoke")

step("3. SQS redelivers the SAME start message. Worker dedupe sees it was applied → invoke(None) resumes from\n"
     "     the checkpoint; the graph re-raises the identical interrupt (same id) → register() is idempotent.")
L.drain_queue()
w = L.store.find(thread_id="order-4471")[0]
note(f"DynamoDB wait record: id={w.wait_id} status={w.status} interrupt_id={w.interrupt_id[:12]}… "
     f"timer_ref={w.timer_ref} allowed={w.policy.allowed_actions}")
note(f"EventBridge Scheduler has {len(L.scheduler.schedules)} one-time schedule(s): {list(L.scheduler.schedules)}")
note(f"Lambdas alive right now: 0.   Payments API called so far: {PAYMENTS_CALLED}")

step("4. Two days pass. Nothing runs, nothing is billed.")
core.advance(2 * DAY); L.scheduler.tick(core.now())

step("5. Finance approver opens the email link and clicks Approve — twice (double-click). Same answer_id both times.")
tok = token_from_events("order-4471")
api(tok, "approve", "priya@finance", answer_id="click-9f1")
api(tok, "approve", "priya@finance", answer_id="click-9f1")

step("6. A colleague, looking at a stale tab, clicks Reject with a different answer_id.")
api(tok, "reject", "dev@finance", answer_id="click-zz9")

step("7. Someone tampers with the token (changes one character).")
bad = tok[:-3] + ("AAA" if not tok.endswith("AAA") else "BBB")
api(bad, "approve", "mallory@example", answer_id="click-evil")

step("8. The resume message is on agent-runs.fifo. A different Lambda instance picks it up and the graph continues:\n"
     "     review returns the decision → issue_refund (the side effect) → notify_customer.")
note(f"queue for order-4471: {[m['kind'] for m in L.run_queue.groups['order-4471']]}")
L.drain_queue()
w = L.store.get(w.wait_id)
note(f"wait {w.wait_id}: status={w.status} action={w.action} by={w.answered_by} resume_attempts={w.resume_attempts}")
note(f"Payments API called: {PAYMENTS_CALLED}  ← exactly once, despite the crash in step 2 and the double-click in step 5")

step("9. The scheduled timer would have fired on day 3. It was cancelled on answer; even if it fires late it is a no-op.")
core.advance(2 * DAY); L.scheduler.tick(core.now())
print("      [timer λ] (nothing fired — schedule was deleted on answer)")
print(f"      late fire simulated directly → {L.waiter.on_timer(w.wait_id)}")


# ════════════════════════════════════════════════════════════════════════════════════════
title("SCENARIO B · ₹90,000 refund · nobody answers · timer expires → default reject · late human click")

step("1. Start")
L.run_queue.send("order-5120", {"kind": "start", "thread_id": "order-5120",
                                "input": {"order_id": "order-5120", "amount": 90_000}}, dedup_id="evt-return-5120")
L.drain_queue()
tok_b = token_from_events("order-5120")

step("2. Three days pass with no answer. EventBridge Scheduler fires the on_timer Lambda.")
core.advance(3 * DAY + 60); L.scheduler.tick(core.now())
wb = L.store.find(thread_id="order-5120")[0]
note(f"wait {wb.wait_id}: status={wb.status} action={wb.action} by={wb.answered_by} answer={wb.answer}")

step("3. The resume with the policy default is queued; the run Lambda applies it → notify_customer (rejected).")
L.drain_queue()
wb = L.store.get(wb.wait_id)
note(f"wait {wb.wait_id}: status={wb.status}.   Payments API called: {PAYMENTS_CALLED} (unchanged)")

step("4. On day 5 the approver finally clicks Approve on the old email.")
api(tok_b, "approve", "priya@finance", answer_id="click-late")
note("The signed token is still within its 7-day validity, but the wait is terminal → 409. The graph is not touched.")


# ════════════════════════════════════════════════════════════════════════════════════════
title("SCENARIO C · ₹30,000 refund · Lambda crashes AFTER applying the resume but BEFORE acking the SQS message")

step("1. Start, park, approve")
L.run_queue.send("order-6001", {"kind": "start", "thread_id": "order-6001",
                                "input": {"order_id": "order-6001", "amount": 30_000}}, dedup_id="evt-return-6001")
L.drain_queue()
api(token_from_events("order-6001"), "approve", "priya@finance", answer_id="click-c1")

step("2. Consumer takes the resume message, graph completes (refund issued), then the Lambda dies before delete().")
L.drain_queue(crash_first_after="invoke")

step("3. (above) Visibility timeout → SQS redelivered the resume message; the consumer checked still_pending() first.")
L.drain_queue()
wc = L.store.find(thread_id="order-6001")[0]
note(f"wait {wc.wait_id}: status={wc.status}.   Payments API called: {PAYMENTS_CALLED}")
note("order-6001 refunded exactly once: the redelivered resume was recognised as already applied.")


# ════════════════════════════════════════════════════════════════════════════════════════
title("SCENARIO D · the sweeper closes the other crash window: created in DynamoDB, crashed before Scheduler/notify")
step("1. Simulate a record that exists with no timer_ref and no notification")
from agent_wait.core import Wait, WaitPolicy, sha, now
orphan = Wait(wait_id="w_orphan", thread_id="order-7777", framework="langgraph", interrupt_id="i-x", checkpoint_id="c-x",
              idempotency_key=sha("order-7777", "i-x", "c-x"), question={"kind": "refund_approval", "order_id": "order-7777", "amount": 51_000},
              binding=sha("q"), policy=WaitPolicy(timeout_s=DAY, allowed_actions=["approve", "reject"]), expires_at=now() + DAY)
L.store.create(orphan)
note(f"before sweep: timer_ref={L.store.get('w_orphan').timer_ref} notified_at={L.store.get('w_orphan').notified_at}")
step("2. The 1-minute sweeper runs")
L.waiter.sweep()
o = L.store.get("w_orphan")
note(f"after sweep:  timer_ref={o.timer_ref} notified_at={'set' if o.notified_at else None}")

title("SUMMARY")
print(f"  refunds issued: {PAYMENTS_CALLED}")
for w in L.store.find():
    print(f"  {w.wait_id:14} {w.thread_id:11} {w.status:9} action={str(w.action):8} by={w.answered_by}")
print(f"  DLQ: {L.run_queue.dlq}")
