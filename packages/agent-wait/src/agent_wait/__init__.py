"""agent-wait: announce an agent's interrupts to the outside world.

A graph node asks a question and pauses:

    from langgraph_wait import ask

    decision = ask({"kind": "refund_approval", "amount": amount},
                   policy=WaitPolicy(timeout="P3D", allowed_actions=("approve", "reject")))

After the run, the host announces whatever it parked on:

    result = graph.invoke(value, config)
    publish_interrupts(result, thread_id, announce=[SnsAnnounce(topic_arn)])

That is the whole library. It builds one envelope per interrupt and hands it to the
announcers. It does not run the graph, receive answers, hold state or run timers.
"""

from .announce import (
    AnnounceAdapter,
    BaseAnnounce,
    CompositeAnnounce,
    FailingAnnounce,
    InMemoryAnnounce,
    LogAnnounce,
    WebhookAnnounce,
    verify_signature,
)
from .errors import PolicyError, QuestionTooLarge, WaitError
from .model import (
    MAX_QUESTION_BYTES,
    Clock,
    EntryPoint,
    FakeClock,
    Question,
    SystemClock,
    Transition,
    WaitEnvelope,
    canonical_json,
    check_question_size,
    iso,
    new_ulid,
)
from .policy import WaitPolicy, parse_duration
from .publish import build_envelope, publish

__version__ = "0.4.0"

__all__ = [
    "MAX_QUESTION_BYTES",
    "AnnounceAdapter",
    "BaseAnnounce",
    "Clock",
    "CompositeAnnounce",
    "EntryPoint",
    "FailingAnnounce",
    "FakeClock",
    "InMemoryAnnounce",
    "LogAnnounce",
    "PolicyError",
    "Question",
    "QuestionTooLarge",
    "SystemClock",
    "Transition",
    "WaitEnvelope",
    "WaitError",
    "WaitPolicy",
    "WebhookAnnounce",
    "__version__",
    "build_envelope",
    "canonical_json",
    "check_question_size",
    "iso",
    "new_ulid",
    "parse_duration",
    "publish",
    "verify_signature",
]
