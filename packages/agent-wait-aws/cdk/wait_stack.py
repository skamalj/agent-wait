"""The CDK stack: everything the agent needs, and nothing it does not.

Read the resource list and the shape of the design falls out of it. There is one queue,
and it is the agent's entry point *and* where approvals arrive *and* where the scheduler
delivers timeouts. There is no answer API, no timer function, no receiver service. The
only two Lambdas are the agent itself and a sweeper that does nothing but re-announce.

    SQS FIFO agent-runs.fifo ──► run Lambda ──► DynamoDB waits
             ▲    ▲                   │
             │    │                   ├──► SNS topic / EventBridge bus  (tell the world)
             │    └───────────────────┴──► EventBridge Scheduler        (tell the future)
             │                                     │
             └─────────────────────────────────────┘
                        the timeout comes back as an ordinary answer

`WaitStack` is a construct rather than a stack so it can be embedded in somebody else's
app; `AgentWaitPocStack` is the deployable wrapper used for the end-to-end run.
"""

from __future__ import annotations

from typing import Any

import aws_cdk as cdk
from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as sources
from aws_cdk import aws_scheduler as scheduler
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subscriptions
from aws_cdk import aws_sqs as sqs
from constructs import Construct

RUNTIME = lambda_.Runtime.PYTHON_3_12


class WaitStack(Construct):
    """Every durable-wait resource, parameterised."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        bundle_path: str,
        handler: str = "refund_agent.handler.handler",
        sweep_handler: str = "refund_agent.handler.sweep_handler",
        timeout: str = "P3D",
        visibility_seconds: int = 300,
        lambda_timeout_seconds: int = 120,
        removal_policy: RemovalPolicy = RemovalPolicy.DESTROY,
    ) -> None:
        super().__init__(scope, construct_id)

        # ---------------------------------------------------------------- storage
        # One table, five item kinds (see store_dynamo.py). `ttl` expires the
        # short-lived rows -- applied messages, leases, parked answers -- on its own.
        self.table = dynamodb.Table(
            self,
            "Waits",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            removal_policy=removal_policy,
        )
        self.table.add_global_secondary_index(
            index_name="gsi1",
            partition_key=dynamodb.Attribute(name="gsi1pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="gsi1sk", type=dynamodb.AttributeType.STRING),
        )
        self.table.add_global_secondary_index(
            index_name="gsi2",
            partition_key=dynamodb.Attribute(name="gsi2pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="gsi2sk", type=dynamodb.AttributeType.NUMBER),
        )

        # The graph's own checkpoints. A separate table because the checkpointer is the
        # application's business, not agent-wait's.
        self.checkpoints = dynamodb.Table(
            self,
            "Checkpoints",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=removal_policy,
        )

        # ---------------------------------------------------------------- the entry point
        self.dlq = sqs.Queue(
            self,
            "RunsDlq",
            fifo=True,
            retention_period=Duration.days(14),
            removal_policy=removal_policy,
        )
        # FIFO with MessageGroupId = thread_id: one in-flight message per conversation.
        # Content-based dedup is off because two different answers to the same wait are
        # different messages and must both be delivered -- dispatch() decides, not SQS.
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
        self.bus = events.EventBus(self, "Bus")

        # A standing subscriber, so there is somewhere to *see* what the world was told.
        # This is an ordinary consumer -- exactly what an approvals UI would be -- and it
        # is how the end-to-end run gets a real token out of a real envelope rather than
        # minting one for itself.
        self.announcements = sqs.Queue(
            self, "Announcements", retention_period=Duration.days(1), removal_policy=removal_policy
        )
        self.topic.add_subscription(
            sns_subscriptions.SqsSubscription(self.announcements, raw_message_delivery=True)
        )

        # ---------------------------------------------------------------- telling the future
        self.schedule_group = scheduler.CfnScheduleGroup(self, "Schedules")
        self.scheduler_role = iam.Role(
            self,
            "SchedulerRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
            description="Lets EventBridge Scheduler deliver a timeout answer to the agent's queue",
        )
        # The only thing a timeout may do is put one message on one queue.
        self.queue.grant_send_messages(self.scheduler_role)

        # ---------------------------------------------------------------- signing keys
        # Generated by CloudFormation, so no key ever exists in source or in a log. The
        # flat {"current": "k1", "k1": "..."} shape is what a template can produce;
        # SecretsManagerKeyProvider reads it directly.
        self.signing_keys = secretsmanager.Secret(
            self,
            "SigningKeys",
            description="HMAC keys for agent-wait tokens",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"current": "k1"}',
                generate_string_key="k1",
                password_length=48,
                exclude_punctuation=True,
            ),
            removal_policy=removal_policy,
        )

        # ---------------------------------------------------------------- the agent
        code = lambda_.Code.from_asset(bundle_path)
        environment = {
            "AGENT_WAIT_TABLE": self.table.table_name,
            "AGENT_WAIT_CHECKPOINT_TABLE": self.checkpoints.table_name,
            "AGENT_WAIT_QUEUE_URL": self.queue.queue_url,
            "AGENT_WAIT_QUEUE_ARN": self.queue.queue_arn,
            "AGENT_WAIT_TOPIC_ARN": self.topic.topic_arn,
            "AGENT_WAIT_BUS_NAME": self.bus.event_bus_name,
            "AGENT_WAIT_SCHEDULE_GROUP": self.schedule_group.ref,
            "AGENT_WAIT_SCHEDULER_ROLE_ARN": self.scheduler_role.role_arn,
            "AGENT_WAIT_SECRET_ID": self.signing_keys.secret_arn,
            "AGENT_WAIT_SIDE_EFFECT_TABLE": self.table.table_name,
            "AGENT_WAIT_TIMEOUT": timeout,
            "POWERTOOLS_LOG_LEVEL": "INFO",
        }

        self.run_function = lambda_.Function(
            self,
            "Run",
            runtime=RUNTIME,
            code=code,
            handler=handler,
            timeout=Duration.seconds(lambda_timeout_seconds),
            memory_size=1024,
            environment=environment,
        )
        self.sweep_function = lambda_.Function(
            self,
            "Sweep",
            runtime=RUNTIME,
            code=code,
            handler=sweep_handler,
            timeout=Duration.seconds(60),
            memory_size=512,
            environment=environment,
        )

        for function in (self.run_function, self.sweep_function):
            self.table.grant_read_write_data(function)
            self.checkpoints.grant_read_write_data(function)
            self.topic.grant_publish(function)
            self.bus.grant_put_events_to(function)
            self.signing_keys.grant_read(function)
            self.queue.grant_send_messages(function)
            function.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["scheduler:CreateSchedule", "scheduler:DeleteSchedule", "scheduler:GetSchedule"],
                    resources=[
                        cdk.Arn.format(
                            cdk.ArnComponents(
                                service="scheduler",
                                resource="schedule",
                                resource_name=f"{self.schedule_group.ref}/*",
                            ),
                            cdk.Stack.of(self),
                        )
                    ],
                )
            )
            # Handing the scheduler role to a schedule is itself a privileged act.
            function.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["iam:PassRole"],
                    resources=[self.scheduler_role.role_arn],
                    conditions={"StringEquals": {"iam:PassedToService": "scheduler.amazonaws.com"}},
                )
            )

        # Batch size 1: one thread, one message, one graph run. Partial batch responses
        # are still reported so a `lease_held` goes back on the queue on its own.
        self.run_function.add_event_source(
            sources.SqsEventSource(
                self.queue, batch_size=1, report_batch_item_failures=True, max_concurrency=10
            )
        )

        # The repair pass (scenario D).
        events.Rule(
            self,
            "SweepEveryMinute",
            schedule=events.Schedule.rate(Duration.minutes(1)),
            targets=[targets.LambdaFunction(self.sweep_function)],
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
        cdk.CfnOutput(self, "TableName", value=self.waits.table.table_name)
        cdk.CfnOutput(self, "CheckpointTableName", value=self.waits.checkpoints.table_name)
        cdk.CfnOutput(self, "TopicArn", value=self.waits.topic.topic_arn)
        cdk.CfnOutput(self, "AnnouncementsQueueUrl", value=self.waits.announcements.queue_url)
        cdk.CfnOutput(self, "BusName", value=self.waits.bus.event_bus_name)
        cdk.CfnOutput(self, "ScheduleGroup", value=self.waits.schedule_group.ref)
        cdk.CfnOutput(self, "SecretId", value=self.waits.signing_keys.secret_arn)
        cdk.CfnOutput(self, "RunFunctionName", value=self.waits.run_function.function_name)
        cdk.CfnOutput(self, "SweepFunctionName", value=self.waits.sweep_function.function_name)
        cdk.CfnOutput(self, "RunLogGroup", value=self.waits.run_function.log_group.log_group_name)
