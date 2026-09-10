"""The CDK stack: everything the agent needs, and nothing it does not.

Read the resource list and the shape of the design falls out of it.

    SQS FIFO agent-runs.fifo ──► run Lambda ──► SNS topic       (tell the world)
             ▲                        │
             │                        └──► DynamoDB approvals   (the question *is* a row)
             │
             └── answers come back to the same queue

One queue, which is the agent's entry point *and* where answers arrive. One Lambda. Two
tables -- the checkpointer's, and the approvals table that one of the announce adapters
writes. No wait store, no signing-key secret, no scheduler, no sweeper: v0.2 keeps no
state of its own, so there is nothing to store, sign or repair.

What went away, and where it went, is in `docs/migrating-from-0.1.md`. The short version:
the timeout is now something a consumer enforces off the approvals table, and the queue
policy is now the only thing standing between a message and the graph.

`WaitStack` is a construct rather than a stack so it can be embedded in somebody else's
app; `AgentWaitPocStack` is the deployable wrapper used for the end-to-end run.
"""

from __future__ import annotations

from typing import Any

import aws_cdk as cdk
from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as sources
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subscriptions
from aws_cdk import aws_sqs as sqs
from constructs import Construct

RUNTIME = lambda_.Runtime.PYTHON_3_12


class WaitStack(Construct):
    """Every resource the publisher needs, parameterised."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        bundle_path: str,
        handler: str = "refund_agent.handler.handler",
        timeout: str = "P3D",
        visibility_seconds: int = 300,
        lambda_timeout_seconds: int = 120,
        approvals_ttl_days: int = 30,
        removal_policy: RemovalPolicy = RemovalPolicy.DESTROY,
    ) -> None:
        super().__init__(scope, construct_id)

        # ---------------------------------------------------------------- storage
        # The checkpointer's table. This is the only durable state in the system now:
        # LangGraph's own, holding the parked interrupt. Lose it and the question is gone,
        # which is exactly as true of any LangGraph deployment.
        self.checkpoints = dynamodb.Table(
            self,
            "Checkpoints",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=removal_policy,
        )

        # Where `DynamoDbAnnounce` puts the questions. Not a store the library reads --
        # nothing in agent-wait ever queries this. It exists for whatever renders the
        # approvals UI and whatever enforces the timeouts.
        self.approvals = dynamodb.Table(
            self,
            "Approvals",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            removal_policy=removal_policy,
        )
        # "Every open question, oldest deadline first" -- the query an approvals UI runs,
        # and the one a timeout sweep runs with a range condition on `expires_at`.
        self.approvals.add_global_secondary_index(
            index_name="by_status",
            partition_key=dynamodb.Attribute(name="status", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="expires_at", type=dynamodb.AttributeType.STRING),
        )

        # ---------------------------------------------------------------- the entry point
        self.dlq = sqs.Queue(
            self,
            "Dlq",
            fifo=True,
            content_based_deduplication=False,
            retention_period=Duration.days(14),
            removal_policy=removal_policy,
        )
        # FIFO with `MessageGroupId = thread_id`: one in-flight message per conversation.
        # That serialisation is what stops two answers for one question racing, and it
        # matters more in v0.2 than it did in v0.1, because there is no longer a
        # conditional write behind it.
        self.queue = sqs.Queue(
            self,
            "Runs",
            fifo=True,
            content_based_deduplication=False,
            visibility_timeout=Duration.seconds(visibility_seconds),
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=5, queue=self.dlq),
            removal_policy=removal_policy,
        )

        # ---------------------------------------------------------------- telling the world
        self.topic = sns.Topic(self, "WaitEvents")
        # A queue on the topic so the end-to-end run can read a real envelope, exactly as
        # a consumer would.
        self.announcements = sqs.Queue(
            self, "Announcements", retention_period=Duration.days(1), removal_policy=removal_policy
        )
        self.topic.add_subscription(
            sns_subscriptions.SqsSubscription(self.announcements, raw_message_delivery=True)
        )

        # ---------------------------------------------------------------- the agent
        self.run_function = lambda_.Function(
            self,
            "Run",
            runtime=RUNTIME,
            code=lambda_.Code.from_asset(bundle_path),
            handler=handler,
            timeout=Duration.seconds(lambda_timeout_seconds),
            memory_size=1024,
            environment={
                "AGENT_WAIT_CHECKPOINT_TABLE": self.checkpoints.table_name,
                "AGENT_WAIT_APPROVALS_TABLE": self.approvals.table_name,
                "AGENT_WAIT_QUEUE_URL": self.queue.queue_url,
                "AGENT_WAIT_TOPIC_ARN": self.topic.topic_arn,
                "AGENT_WAIT_SIDE_EFFECT_TABLE": self.approvals.table_name,
                "AGENT_WAIT_TIMEOUT": timeout,
                "AGENT_WAIT_APPROVALS_TTL_DAYS": str(approvals_ttl_days),
            },
        )

        self.checkpoints.grant_read_write_data(self.run_function)
        self.approvals.grant_read_write_data(self.run_function)
        self.topic.grant_publish(self.run_function)

        # Batch size 1: one thread, one message, one graph run.
        self.run_function.add_event_source(
            sources.SqsEventSource(
                self.queue, batch_size=1, report_batch_item_failures=True, max_concurrency=10
            )
        )


class AgentWaitPocStack(Stack):
    """The deployable proof-of-concept stack."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bundle_path = self.node.try_get_context("bundlePath") or "build/lambda"
        timeout = self.node.try_get_context("waitTimeout") or "P3D"

        self.waits = WaitStack(self, "AgentWait", bundle_path=bundle_path, timeout=timeout)

        cdk.Tags.of(self).add("project", "agent-wait")

        cdk.CfnOutput(self, "QueueUrl", value=self.waits.queue.queue_url)
        cdk.CfnOutput(self, "QueueArn", value=self.waits.queue.queue_arn)
        cdk.CfnOutput(self, "DlqUrl", value=self.waits.dlq.queue_url)
        cdk.CfnOutput(self, "ApprovalsTableName", value=self.waits.approvals.table_name)
        cdk.CfnOutput(self, "CheckpointTableName", value=self.waits.checkpoints.table_name)
        cdk.CfnOutput(self, "TopicArn", value=self.waits.topic.topic_arn)
        cdk.CfnOutput(self, "AnnouncementsQueueUrl", value=self.waits.announcements.queue_url)
        cdk.CfnOutput(self, "RunFunctionName", value=self.waits.run_function.function_name)
        cdk.CfnOutput(self, "RunLogGroup", value=self.waits.run_function.log_group.log_group_name)
