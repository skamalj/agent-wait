"""agent-wait: publish an agent's interrupts to the outside world.

A graph node asks a question and pauses:

    from langgraph_wait import ask

    decision = ask({"kind": "refund_approval", "amount": amount},
                   policy=WaitPolicy(timeout="P3D", allowed_actions=("approve", "reject")))

The host runs the graph through a publisher, and whatever the graph parked on goes out
to wherever people can see it:

    agent = WaitPublisher(LangGraphAdapter(graph),
                          announce=[SnsAnnounce(topic_arn)],
                          reply_to=EntryPoint("sqs", queue_url))

    agent.invoke(payload, thread_id)

That is the whole library. It publishes questions and it reports what a thread is parked
on. It does not receive answers, hold state, mint credentials or run timers -- see
`docs/migrating-from-0.1.md` for what that means if you are coming from v0.1.
"""

from .announce import (
    AnnounceAdapter,
    CompositeAnnounce,
    FailingAnnounce,
    InMemoryAnnounce,
    LogAnnounce,
)
from .errors import PolicyError, QuestionTooLarge, WaitError
from .model import (
    MAX_QUESTION_BYTES,
    Clock,
    EntryPoint,
    FakeClock,
    PendingInterrupt,
    SystemClock,
    Transition,
    WaitEnvelope,
    canonical_json,
    check_question_size,
    iso,
    new_ulid,
)
from .policy import WaitPolicy, parse_duration
from .publisher import FrameworkAdapter, WaitPublisher

__version__ = "0.2.0"

__all__ = [
    "MAX_QUESTION_BYTES",
    "AnnounceAdapter",
    "Clock",
    "CompositeAnnounce",
    "EntryPoint",
    "FailingAnnounce",
    "FakeClock",
    "FrameworkAdapter",
    "InMemoryAnnounce",
    "LogAnnounce",
    "PendingInterrupt",
    "PolicyError",
    "QuestionTooLarge",
    "SystemClock",
    "Transition",
    "WaitEnvelope",
    "WaitError",
    "WaitPolicy",
    "WaitPublisher",
    "__version__",
    "canonical_json",
    "check_question_size",
    "iso",
    "new_ulid",
    "parse_duration",
]
