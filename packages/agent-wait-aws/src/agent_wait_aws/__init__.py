"""agent-wait-aws: the AWS backends.

    from agent_wait_aws import (
        DynamoWaitStore, SqsAnnounce, SchedulerAnnounce, make_run_handler,
    )

Nothing here is a second entry point. The store is a store, the announce adapters tell
the world, and `make_run_handler` wraps the queue the agent already had. The timeout is
`SchedulerAnnounce`, which delivers an ordinary answer to that same queue, three days
late.
"""

from .announce import (
    EventBridgeAnnounce,
    SchedulerAnnounce,
    SnsAnnounce,
    SqsAnnounce,
    timeout_message,
)
from .entry_point import (
    entry_point_from_env,
    lambda_entry_point,
    queue_arn_from_url,
    sqs_entry_point,
)
from .keys_secrets import SecretsManagerKeyProvider
from .run_handler import make_run_handler, make_sweep_handler, queue_url_from_arn
from .store_dynamo import DynamoWaitStore

__version__ = "0.1.0"

__all__ = [
    "DynamoWaitStore",
    "EventBridgeAnnounce",
    "SchedulerAnnounce",
    "SecretsManagerKeyProvider",
    "SnsAnnounce",
    "SqsAnnounce",
    "entry_point_from_env",
    "lambda_entry_point",
    "make_run_handler",
    "make_sweep_handler",
    "queue_arn_from_url",
    "queue_url_from_arn",
    "sqs_entry_point",
    "timeout_message",
]
