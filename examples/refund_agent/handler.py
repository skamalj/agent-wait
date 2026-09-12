"""The Lambda host. One graph, one queue, one library call after each run.

    result = graph.invoke(value, config)
    publish_interrupts(result, thread_id, ANNOUNCE)

The `if` that tells an answer from a start is the documented rule, written out. Nothing
is validated on the way in: a duplicate answer lands on a thread that has already moved
on and LangGraph runs nothing; a late answer is the consumer's problem to not send. See
`docs/message-formats.md` for what a consumer is expected to send and why.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from typing import Any

from langgraph.types import Command

from agent_wait import LogAnnounce
from agent_wait.aws import DynamoDbAnnounce, SnsAnnounce
from agent_wait.langgraph import publish_interrupts
from refund_agent.dynamo_checkpointer import DynamoDBSaver
from refund_agent.graph import build_graph

# The Lambda runtime leaves the root logger at WARNING, so a library logging at INFO is
# silent in production -- which is exactly when you want to know what was published.
logging.getLogger("agent_wait").setLevel(os.environ.get("AGENT_WAIT_LOG_LEVEL", "INFO").upper())

_log = logging.getLogger("refund_agent")

graph = build_graph(DynamoDBSaver(os.environ["AGENT_WAIT_CHECKPOINT_TABLE"]))

# Where questions go: a topic for whoever subscribes, and a table an approvals UI can
# query directly. The library writes to both and reads from neither.
ANNOUNCE = [
    LogAnnounce(),
    SnsAnnounce(os.environ["AGENT_WAIT_TOPIC_ARN"]),
    DynamoDbAnnounce(os.environ["AGENT_WAIT_APPROVALS_TABLE"]),
]


def route(message: Mapping[str, Any]) -> Any:
    thread_id = str(message["thread_id"])
    config = {"configurable": {"thread_id": thread_id}}

    if "question_id" in message:  # an answer, in the documented shape
        value: Any = Command(resume={str(message["question_id"]): message.get("answer")})
    else:  # a start
        value = message.get("input")

    result = graph.invoke(value, config)
    publish_interrupts(result, thread_id, ANNOUNCE)
    return result


def handler(event: Mapping[str, Any], context: Any = None) -> dict[str, Any]:
    """SQS entry point. One record at a time, keyed by thread."""
    failures: list[dict[str, str]] = []
    for record in event.get("Records", []):
        try:
            route(json.loads(record["body"]))
        except Exception:
            _log.exception("failed on message %s", record.get("messageId"))
            failures.append({"itemIdentifier": str(record.get("messageId", ""))})
    return {"batchItemFailures": failures}
