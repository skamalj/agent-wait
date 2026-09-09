"""Describing where the agent already listens.

`EntryPoint` itself lives in the core, because it is part of the message contract -- it
is what fills `reply_to` in every envelope we emit. What lives here is the AWS-flavoured
plumbing around it: turning a queue URL into the ARN that EventBridge Scheduler needs,
and reading the whole configuration out of the environment so a Lambda handler is five
lines long.
"""

from __future__ import annotations

import os

from agent_wait.model import EntryPoint


def sqs_entry_point(queue_url: str) -> EntryPoint:
    return EntryPoint("sqs", queue_url)


def lambda_entry_point(function_arn: str) -> EntryPoint:
    return EntryPoint("lambda", function_arn)


def queue_arn_from_url(queue_url: str) -> str:
    """`https://sqs.<region>.amazonaws.com/<account>/<name>` -> `arn:aws:sqs:...`.

    Scheduler targets are addressed by ARN while everything else in the SQS API uses the
    URL, and carrying both through configuration is one more thing to get out of step.
    """
    stripped = queue_url.split("://", 1)[-1]
    parts = stripped.split("/")
    host, account, name = parts[0], parts[1], parts[2]
    region = host.split(".")[1]
    return f"arn:aws:sqs:{region}:{account}:{name}"


def entry_point_from_env(variable: str = "AGENT_WAIT_QUEUE_URL") -> EntryPoint:
    queue_url = os.environ.get(variable)
    if not queue_url:
        raise RuntimeError(f"{variable} is not set; the agent has no entry point to advertise")
    return sqs_entry_point(queue_url)
