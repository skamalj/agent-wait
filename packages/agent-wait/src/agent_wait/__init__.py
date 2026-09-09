"""agent-wait: durable waits for agent frameworks.

A framework interrupt becomes a parked, addressable, expirable wait that outlives the
process that raised it. The public surface is small on purpose:

    from agent_wait import WaitRuntime, WaitPolicy, EntryPoint

    runtime = WaitRuntime(adapter=..., store=..., tokens=..., announce=[...],
                          entry_point=EntryPoint("sqs", queue_url))

    outcome = runtime.dispatch(payload)      # before the graph
    ...                                      # invoke the graph
    result = runtime.register(out, config, thread_id)   # after the graph

`ask()` lives in the framework adapter package (`langgraph_wait`), because that is the
only part of this that has to know what an interrupt is.
"""

from .announce import (
    AnnounceAdapter,
    CompositeAnnounce,
    FailingAnnounce,
    InMemoryAnnounce,
    LogAnnounce,
)
from .errors import (
    PolicyError,
    QuestionTooLarge,
    StoreConflict,
    TokenExpired,
    TokenInvalid,
    WaitError,
)
from .model import (
    Clock,
    EntryPoint,
    FakeClock,
    Ignore,
    IgnoreReason,
    Lease,
    PendingInterrupt,
    RegisterResult,
    Resume,
    Start,
    Status,
    SystemClock,
    Transition,
    Wait,
    WaitEnvelope,
    binding_hash,
    canonical_json,
    idempotency_key,
    new_ulid,
)
from .policy import WaitPolicy, parse_duration
from .runtime import FrameworkAdapter, WaitRuntime
from .store import InMemoryWaitStore, SqliteWaitStore, WaitStore
from .sweep import sweep
from .token import (
    DEFAULT_TOKEN_TTL,
    EnvKeyProvider,
    KeyProvider,
    StaticKeyProvider,
    TokenClaims,
    TokenCodec,
)

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_TOKEN_TTL",
    "AnnounceAdapter",
    "Clock",
    "CompositeAnnounce",
    "EntryPoint",
    "EnvKeyProvider",
    "FailingAnnounce",
    "FakeClock",
    "FrameworkAdapter",
    "Ignore",
    "IgnoreReason",
    "InMemoryAnnounce",
    "InMemoryWaitStore",
    "KeyProvider",
    "Lease",
    "LogAnnounce",
    "PendingInterrupt",
    "PolicyError",
    "QuestionTooLarge",
    "RegisterResult",
    "Resume",
    "SqliteWaitStore",
    "Start",
    "StaticKeyProvider",
    "Status",
    "StoreConflict",
    "SystemClock",
    "TokenClaims",
    "TokenCodec",
    "TokenExpired",
    "TokenInvalid",
    "Transition",
    "Wait",
    "WaitEnvelope",
    "WaitError",
    "WaitPolicy",
    "WaitRuntime",
    "WaitStore",
    "binding_hash",
    "canonical_json",
    "idempotency_key",
    "new_ulid",
    "parse_duration",
    "sweep",
]
