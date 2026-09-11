"""agent-wait-aws: announce adapters for AWS.

Four places a question can land:

    SnsAnnounce(topic_arn)        # fan out; policy tags become filterable attributes
    SqsAnnounce(queue_url)        # one consumer, ordered per thread on FIFO
    EventBridgeAnnounce(bus)      # rules and targets decide who cares
    DynamoDbAnnounce(table)       # the question *is* a row; query it directly

This package contains nothing but adapters and the CDK stack for the example. There
is no store, no key provider and no run handler here, because the library keeps no state
and receives no answers.
"""

from .announce import DynamoDbAnnounce, EventBridgeAnnounce, SnsAnnounce, SqsAnnounce

__all__ = ["DynamoDbAnnounce", "EventBridgeAnnounce", "SnsAnnounce", "SqsAnnounce"]
