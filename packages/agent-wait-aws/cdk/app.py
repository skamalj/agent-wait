#!/usr/bin/env python3
"""The CDK app.

Everything is parameterised through context so one definition serves the PoC, a sandbox
and anything else:

    cdk deploy -c stackName=agent-wait-poc-ks -c bundlePath=../../../build/lambda \
               -c waitTimeout=PT2M
"""

from __future__ import annotations

import os

import aws_cdk as cdk
from wait_stack import AgentWaitPocStack

app = cdk.App()

stack_name = app.node.try_get_context("stackName") or "agent-wait-poc"
region = app.node.try_get_context("region") or os.environ.get("CDK_DEFAULT_REGION", "ap-south-1")
account = os.environ.get("CDK_DEFAULT_ACCOUNT")

AgentWaitPocStack(
    app,
    stack_name,
    stack_name=stack_name,
    env=cdk.Environment(account=account, region=region),
    description="agent-wait proof of concept: durable waits for a LangGraph refund agent",
)

app.synth()
