"""AWS implementors of `BaseAnnounce`. Install with `pip install agent-wait[aws]`.

SnsAnnounce(topic_arn)        # fan out; policy tags become filterable attributes
SqsAnnounce(queue_url)        # one consumer; ordered per thread on FIFO
EventBridgeAnnounce(bus)      # rules and targets decide who cares
DynamoDbAnnounce(table)       # the question *is* a row; write-only
"""

try:
    import boto3  # noqa: F401  # pyright: ignore[reportUnusedImport]
except ImportError as _err:  # pragma: no cover - exercised only without the extra
    raise ImportError("agent_wait.aws needs boto3: pip install 'agent-wait[aws]'") from _err

from .dynamodb import DynamoDbAnnounce
from .eventbridge import EventBridgeAnnounce
from .sns import SnsAnnounce
from .sqs import SqsAnnounce

__all__ = ["DynamoDbAnnounce", "EventBridgeAnnounce", "SnsAnnounce", "SqsAnnounce"]
