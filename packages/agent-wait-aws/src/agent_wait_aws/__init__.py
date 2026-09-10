"""agent-wait-aws: announce adapters for AWS.

Four places a question can land:

    SnsAnnounce(topic_arn)        # fan out; policy tags become filterable attributes
    SqsAnnounce(queue_url)        # one consumer, ordered per thread on FIFO
    EventBridgeAnnounce(bus)      # rules and targets decide who cares
    DynamoDbAnnounce(table)       # the question *is* a row; query it directly

Since v0.2 this package contains nothing but adapters. The DynamoDB wait store, the
token key provider, the SQS run handler and the EventBridge Scheduler timeout all went
away with the machinery that needed them -- see `docs/migrating-from-0.1.md`.
"""

from .announce import DynamoDbAnnounce, EventBridgeAnnounce, SnsAnnounce, SqsAnnounce

__all__ = ["DynamoDbAnnounce", "EventBridgeAnnounce", "SnsAnnounce", "SqsAnnounce"]
