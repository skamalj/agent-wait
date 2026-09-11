"""The host. Wire the publisher once, then route each message to `invoke()`.

Two things happen here and nothing else:

    agent = WaitPublisher(...)      # where questions go, and where answers should come back
    agent.invoke(value, thread_id)  # run the graph, publish whatever it parked on

`route()` below is the part that used to be a library feature. It is a dozen lines, it is
yours, and it is deliberately in the example rather than in the package: deciding whether
an inbound message is a fresh request or an answer is a decision about *your* queue, and
the moment a library makes it, the library needs to know about tokens, idempotency and
who is allowed to answer. v0.2 does not.

Copy it. The two guards in it are not decoration -- see the comments on each.

## What you own now

Everything after `expires_at`. Specifically:

* **Enforcing the timeout.** The envelope carries an absolute `expires_at` and the
  `default` the graph author declared. Something of yours -- a scheduled sweep over
  the DynamoDB announce table, an EventBridge schedule, a cron -- has to notice the
  deadline and send the default. Nothing here will.
* **Authenticating the answer.** There is no token. Whoever can put a message on this
  queue can answer any question on it, so the queue policy is the security boundary.
* **Deciding a race.** Two approvals for one interrupt: the first resumes the graph, and
  the second arrives at a thread that is no longer parked. `agent.pending(thread_id)`
  is how you check before invoking; see `is_still_open()`.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from typing import Any

from agent_wait import EntryPoint, LogAnnounce, WaitPublisher
from agent_wait_aws import DynamoDbAnnounce, SnsAnnounce
from langgraph_wait import LangGraphAdapter, is_answer, resume_command

from refund_agent.dynamo_checkpointer import DynamoDBSaver
from refund_agent.graph import build_graph

# The Lambda runtime leaves the root logger at WARNING, so a library logging at INFO is
# silent in production -- which is exactly when you want to know what was published.
for _name in ("agent_wait", "agent_wait_aws"):
    logging.getLogger(_name).setLevel(os.environ.get("AGENT_WAIT_LOG_LEVEL", "INFO").upper())

_log = logging.getLogger("refund_agent")

graph = build_graph(DynamoDBSaver(os.environ["AGENT_WAIT_CHECKPOINT_TABLE"]))

agent = WaitPublisher(
    LangGraphAdapter(graph),
    announce=[
        LogAnnounce(),
        # Fan out to whoever subscribes; the policy's tags become filterable attributes.
        SnsAnnounce(os.environ["AGENT_WAIT_TOPIC_ARN"]),
        # And land the question in a table, so an approvals UI can query open waits
        # directly instead of building a projection off the topic.
        DynamoDbAnnounce(os.environ["AGENT_WAIT_APPROVALS_TABLE"]),
    ],
    # Optional. The library builds no return leg and never reads this; it is published
    # so a consumer does not have to hardcode which queue this environment's agent is on.
    reply_to=EntryPoint("sqs", os.environ["AGENT_WAIT_QUEUE_URL"]),
)


def route(message: Mapping[str, Any]) -> Any:
    """Start or resume. The whole rule is whether `interrupt_id` is present."""
    thread_id = message["thread_id"]
    if is_answer(message):
        if not is_still_open(thread_id, str(message["interrupt_id"])):
            _log.info("ignoring an answer for a question that is already closed")
            return None
        return agent.invoke(resume_command(message), thread_id)
    if agent.pending(thread_id):
        # A redelivered start for a thread that is already parked. Re-invoking with the
        # original input would ask the question a second time under a new interrupt id;
        # republishing repairs an announce that may have been lost without touching the
        # graph. This is the rule v0.1's store used to apply for you.
        _log.info("republishing an already-parked thread rather than starting a new turn")
        agent.republish(thread_id)
        return None
    return agent.invoke(message.get("input"), thread_id)


def is_still_open(thread_id: str, interrupt_id: str) -> bool:
    """Did somebody already answer this?

    Not a guarantee -- two answers a millisecond apart both pass this check, and the
    second then resumes a thread that has moved on. Making it a guarantee needs a
    conditional write somewhere, which is the thing v0.2 handed back to you. On SQS FIFO
    with `MessageGroupId = thread_id` the queue already serialises per thread, and this
    check closes the rest of the gap.
    """
    return any(p.interrupt_id == interrupt_id for p in agent.pending(thread_id))


def handler(event: Mapping[str, Any], context: Any = None) -> dict[str, Any]:
    """SQS entry point. One record at a time, keyed by thread."""
    failures: list[dict[str, str]] = []
    for record in event.get("Records", []):
        try:
            route(json.loads(record["body"]))
        except Exception:
            # Let SQS redeliver. Every path a redelivery can take is safe: an answer for
            # a closed question is dropped, and a start for a parked thread republishes
            # the same interrupt id rather than asking again.
            _log.exception("failed on message %s", record.get("messageId"))
            failures.append({"itemIdentifier": str(record.get("messageId", ""))})
    return {"batchItemFailures": failures}
