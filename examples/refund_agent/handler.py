"""The Lambda entry point.

This is the entire deployment story. Two of these lines are agent-wait:

    runtime = WaitRuntime(...)
    handler = make_run_handler(graph, runtime)

Everything else is naming the pieces -- which store, which announce adapters, where the
agent listens. The graph in `graph.py` still knows nothing about any of it.

Note what is absent. There is no answer endpoint, no timer function and no second
handler for approvals. `dispatch()`, inside `make_run_handler`, reads whatever arrives on
the one queue and works out whether it is a start, an answer, a timeout, or something to
ignore.
"""

from __future__ import annotations

import logging
import os

from agent_wait import EntryPoint, LogAnnounce, TokenCodec, WaitRuntime
from agent_wait_aws import (
    DynamoWaitStore,
    EventBridgeAnnounce,
    SchedulerAnnounce,
    SecretsManagerKeyProvider,
    SnsAnnounce,
    make_run_handler,
    make_sweep_handler,
)
from langgraph_wait import LangGraphAdapter

from refund_agent.dynamo_checkpointer import DynamoDBSaver
from refund_agent.graph import build_graph

# The Lambda runtime leaves the root logger at WARNING, so a library logging at INFO is
# silent in production -- which is exactly when you want to know why a message was
# ignored. Raising the level on our own loggers is enough; the records still reach the
# handler the runtime installed.
for _name in ("agent_wait", "agent_wait_aws"):
    logging.getLogger(_name).setLevel(os.environ.get("AGENT_WAIT_LOG_LEVEL", "INFO").upper())

queue_url = os.environ["AGENT_WAIT_QUEUE_URL"]

graph = build_graph(DynamoDBSaver(os.environ["AGENT_WAIT_CHECKPOINT_TABLE"]))

runtime = WaitRuntime(
    adapter=LangGraphAdapter(graph),
    store=DynamoWaitStore(os.environ["AGENT_WAIT_TABLE"]),
    tokens=TokenCodec(SecretsManagerKeyProvider(os.environ["AGENT_WAIT_SECRET_ID"])),
    announce=[
        LogAnnounce(),
        SnsAnnounce(os.environ["AGENT_WAIT_TOPIC_ARN"]),
        EventBridgeAnnounce(os.environ["AGENT_WAIT_BUS_NAME"]),
        # The timeout. Delivers an ordinary answer to the queue above, three days late.
        SchedulerAnnounce(
            os.environ["AGENT_WAIT_SCHEDULE_GROUP"],
            queue_arn=os.environ["AGENT_WAIT_QUEUE_ARN"],
            role_arn=os.environ["AGENT_WAIT_SCHEDULER_ROLE_ARN"],
            queue_url=queue_url,
        ),
    ],
    entry_point=EntryPoint("sqs", queue_url),
)

handler = make_run_handler(graph, runtime)
sweep_handler = make_sweep_handler(runtime)
