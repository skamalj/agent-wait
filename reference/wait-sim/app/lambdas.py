"""
Three Lambda functions. In production each is its own deployment sharing DynamoDB + SQS.
Here they share module-level singletons that stand in for those services.

  run_handler      SQS FIFO trigger (MessageGroupId = thread_id). Starts or resumes a graph run,
                   then hands the result to Waiter.register().  [agent code + langgraph-wait]
  answer_handler   API Gateway  POST /w/{token}                   [agent-wait-aws ingress]
  on_timer_handler EventBridge Scheduler target                   [agent-wait-aws]
"""
from __future__ import annotations

import json

from langgraph.types import Command

from agent_wait.aws_sim import DynamoWaitStore, EventBridgeBus, EventBridgeScheduler, SqsFifo, SqsResumer
from agent_wait.core import (ActionNotAllowed, AlreadyAnswered, BindingMismatch, HmacTokenCodec,
                             TokenInvalid, WaitExpired, WaitNotFound, Waiter)
from agent_wait.langgraph_adapter import LangGraphAdapter
from app.graph import build_graph

LOG: list[str] = []
def log(msg): LOG.append(msg); print(msg)


class CrashedMidway(RuntimeError): ...


# ---- "infrastructure" (shared across the three functions)
graph = build_graph()                                   # checkpointer = DynamoDB in prod
run_queue = SqsFifo()                                   # agent-runs.fifo
applied_messages: dict[str, set[str]] = {}              # tier-2 worker dedupe: thread_id -> message ids applied
store = DynamoWaitStore()
scheduler = EventBridgeScheduler(target=lambda ev: on_timer_handler(ev, {}))
bus = EventBridgeBus()
tokens = HmacTokenCodec(keys={"k1": b"dev-secret"}, current="k1")

waiter = Waiter(adapter=LangGraphAdapter(graph), store=store, tokens=tokens, timer=scheduler,
                notifier=bus, resumer=SqsResumer(run_queue), base_url="https://approvals.example.com/w/", log=log)


# ---- Lambda 1: the agent runner (SQS-triggered)
def run_handler(event, context):
    for rec in event["Records"]:
        body = rec["body"]; thread_id = body["thread_id"]; config = {"configurable": {"thread_id": thread_id}}
        seen = applied_messages.setdefault(thread_id, set())

        if body["kind"] == "start":
            if body["message_id"] in seen:
                log(f"      [run λ] message {body['message_id'][:8]} already applied to {thread_id} → invoke(None) (resume from checkpoint)")
                result = graph.invoke(None, config)
            else:
                log(f"      [run λ] start {thread_id} with input {body['input']}")
                result = graph.invoke(body["input"], config)
                seen.add(body["message_id"])
        else:  # resume
            wait = store.get(body["wait_id"])
            if not waiter.adapter.still_pending(wait):
                log(f"      [run λ] resume for {wait.wait_id}: thread already past this interrupt → no-op (idempotent)")
                waiter.mark_resumed(thread_id); continue
            log(f"      [run λ] resume {thread_id} with {body['resume_map']}")
            result = graph.invoke(Command(resume=body["resume_map"]), config)

        if context.get("crash_after") == "invoke":
            raise CrashedMidway("Lambda died after graph.invoke() returned — before register()/ack")

        waits = waiter.register(result, config, thread_id)
        if waits:
            for w in waits:
                log(f"      [run λ] parked: wait {w.wait_id} status={w.status} expires_in={round((w.expires_at - w.created_at)/86400)}d")
        else:
            waiter.mark_resumed(thread_id)
            log(f"      [run λ] run complete: {graph.get_state(config).values.get('status')}")


# ---- Lambda 2: the answer API (API Gateway)
def answer_handler(event, context):
    token = event["pathParameters"]["token"]
    body = json.loads(event["body"]); actor = event["requestContext"]["authorizer"]["email"]
    try:
        outcome = waiter.answer(token, action=body["action"], payload=body.get("payload", {}) | {"action": body["action"]},
                                actor=actor, answer_id=body["answer_id"])
        return {"statusCode": 200, "body": {"outcome": outcome}}
    except (TokenInvalid, WaitNotFound) as e:  return {"statusCode": 404, "body": {"error": str(e)}}
    except WaitExpired as e:                    return {"statusCode": 410, "body": {"error": str(e)}}
    except (AlreadyAnswered, BindingMismatch) as e: return {"statusCode": 409, "body": {"error": str(e)}}
    except ActionNotAllowed as e:               return {"statusCode": 403, "body": {"error": f"action not allowed: {e}"}}


# ---- Lambda 3: the timeout (EventBridge Scheduler target)
def on_timer_handler(event, context):
    outcome = waiter.on_timer(event["wait_id"])
    log(f"      [timer λ] wait {event['wait_id']} → {outcome}")
    return outcome


# ---- helpers the simulation uses to "be" SQS + the Lambda service
def drain_queue(*, crash_first_after: str | None = None):
    crashed = False
    while (msg := run_queue.receive()):
        group, body = msg
        try:
            run_handler({"Records": [{"body": body}]}, {"crash_after": None if crashed or not crash_first_after else crash_first_after})
            run_queue.delete(group)
        except CrashedMidway as e:
            crashed = True
            log(f"      [SQS] consumer crashed ({e}); visibility timeout elapses → message redelivered")
            run_queue.crash_consumer(group)
