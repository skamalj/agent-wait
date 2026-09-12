"""agent-wait: make an agent wait for something outside the process, and get the answer back in.

    from agent_wait import WaitPolicy
    from agent_wait.aws import SnsAnnounce
    from agent_wait.langgraph import wait, publish_interrupts

    @tool
    @wait(WaitPolicy(timeout="P3D", allowed_actions=("approve", "reject")))
    def issue_refund(order_id: str, amount: int) -> str: ...

    result = graph.invoke(value, config)
    publish_interrupts(result, thread_id, announce=[SnsAnnounce(topic_arn)])

The core is framework- and provider-neutral: the policy, the question, the envelope,
`publish()`, and the announcer interface. `agent_wait.langgraph` implements the
`Framework` interface for LangGraph; `agent_wait.aws` implements announcers for AWS.
Install the extras you need: `pip install agent-wait[langgraph,aws]`.
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
from .framework import Framework
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
from .wait import make_publish_interrupts, make_wait, question_id_for

__version__ = "0.5.0"

__all__ = [
    "MAX_QUESTION_BYTES",
    "AnnounceAdapter",
    "BaseAnnounce",
    "Clock",
    "CompositeAnnounce",
    "EntryPoint",
    "FailingAnnounce",
    "FakeClock",
    "Framework",
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
    "make_publish_interrupts",
    "make_wait",
    "new_ulid",
    "parse_duration",
    "publish",
    "question_id_for",
    "verify_signature",
]
