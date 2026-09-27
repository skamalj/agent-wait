"""`@wait` and `publish_interrupts`, built once over the `Framework` interface.

A framework subpackage binds them:

    _fw = LangGraphFramework()
    wait = make_wait(_fw)
    publish_interrupts = make_publish_interrupts(_fw)

and a user imports the bound names. Nothing in this module knows which framework it is
running on; that is the point.

## `@wait` -- make a function wait for something outside the process

    @tool
    @wait(WaitPolicy(timeout="P1D", default={"action": "reject"}, tags={"approver_group": "finance"}))
    def issue_refund(order_id: str, amount: int) -> str:
        ...

Calling the decorated function inside a run parks the run on `{"function": name,
"args": {...}}`. `publish_interrupts()` after the run announces it. The answer comes back
through the framework's own resume call, and:

    {"action": "approve"}                    -> the body runs with its original args
    {"action": "approve", "args": {...}}     -> the body runs with these args instead
    anything else                            -> the body does NOT run; the answer is
                                                returned in its place, so a model sees why

A function with a `decision` parameter (rename it with `decision=`) **always** runs and
receives the answer there, approve or not. That is how a node, rather than a tool, gets
the answer:

    @wait(FINANCE)
    def review(state, decision=None):
        return {"decision": decision}

## `when=` -- ask only sometimes

Most approvals have a threshold: small refunds go through, large ones need a human.
Without `when=`, `@wait` parks *every* call, so that rule has to live somewhere else --
two tools and a model choosing between them, or a router node in front. Both put the
rule where you cannot enforce it.

    @tool
    @wait(FINANCE, when=lambda order_id, amount: amount > 25_000)
    def issue_refund(order_id: str, amount: int) -> str:
        payments.refund(order_id, amount)
        return "refunded"

One tool. Under the limit the body runs and nobody is asked; over it, the call parks
exactly as an unconditional `@wait` would. The threshold is in code, in one place, and
no model decides whether it applies.

Three things about the predicate:

* **It sees what the approver sees** -- the published `args`, not the framework's
  injected context. What you branch on is what the question shows.
* **It must be deterministic on the same arguments.** The framework re-runs the node
  from the top when the answer comes back, so `when` is called again. One that answered
  differently the second time would let the body run without the answer.
* **If it raises, the call parks anyway.** A refund is not skipped because a lambda had
  a typo. The exception is logged on `agent_wait.wait`.

## `mode="async"` -- the run does not park

    @tool
    @wait(FINANCE, mode="async", announce=[SnsAnnounce(topic), DynamoDbAnnounce(table)])
    def issue_refund(order_id: str, amount: int) -> str: ...

The decorator *is* the publisher. The call announces the question through the announcers
given here and returns `{"status": "pending_approval", "question_id": ...}` without
running the body. Nothing is parked; nothing is called after the run; the run carries on.
The decision arrives later as a **new message** and the graph acts on it however you
designed it. The framework remembers nothing about the question; whatever record you keep
(a `DynamoDbAnnounce` row is one) is the only one.

`question_id` is `sha256(thread | function | args)`, so the same call on the same thread
is the same question and a consumer sees one, not two.

## What waits for what

"Human in the loop" is the common case and not the only one. The same decorator parks a
run for a vendor callback, a payment processor, a KYC check, or another agent -- anything
that answers from outside the process. The envelope carries `correlation` for the
external-job case; who answers is the caller's business.
"""

from __future__ import annotations

import functools
import hashlib
import inspect
import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal, cast

from .announce.base import AnnounceAdapter
from .framework import Framework
from .model import Clock, EntryPoint, Question, WaitEnvelope, canonical_json, check_question_size
from .policy import WaitPolicy
from .publish import publish

Mode = Literal["interrupt", "async"]

_log = logging.getLogger("agent_wait.wait")

WAIT_KEY = "__wait__"
SOURCE_KEY = "__source__"
QUESTION_KEY = "question"


# ------------------------------------------------------------------ the interrupt value
def pack(question: Any, policy: WaitPolicy, source: Mapping[str, Any] | None) -> dict[str, Any]:
    """The value a framework parks on. A framework interrupt carries one payload and no
    side channel, so the policy and the source ride inside it, next to the question."""
    check_question_size(question)
    value: dict[str, Any] = {QUESTION_KEY: question, WAIT_KEY: policy.to_dict()}
    if source:
        value[SOURCE_KEY] = dict(source)
    return value


def _as_dict(value: Any) -> dict[str, Any] | None:
    """`dict(value)` when it is a mapping, else `None`. The one place `Any` is narrowed."""
    if isinstance(value, Mapping):
        return {str(k): v for k, v in cast(Mapping[Any, Any], value).items()}
    return None


def unpack(value: Any) -> tuple[Any, WaitPolicy, dict[str, Any] | None]:
    """`(question, policy, source)` from a parked value of either shape.

    A value without `__wait__` -- a bare framework interrupt from code that has never
    heard of this library -- is a question with the default policy.
    """
    packed = _as_dict(value)
    if packed is not None and WAIT_KEY in packed:
        question: Any = packed.get(QUESTION_KEY)
        return question, WaitPolicy.from_dict(packed.get(WAIT_KEY)), _as_dict(packed.get(SOURCE_KEY))
    return value, WaitPolicy(), None


def question_id_for(thread_id: str, function: str, args: Mapping[str, Any]) -> str:
    """Deterministic: the same call on the same thread is the same question."""
    return hashlib.sha256(f"{thread_id}|{function}|{canonical_json(dict(args))}".encode()).hexdigest()[:32]


# ------------------------------------------------------------------ the decorator


def _asks(when: Callable[..., bool], visible: Mapping[str, Any], function: str) -> bool:
    """Does this call need a question? `when` decides, and a broken `when` says yes.

    The predicate sees exactly the arguments the approver would see -- not the
    framework's injected context -- so what you branch on is what gets published.

    It is called again when the run resumes, because the framework re-runs the node
    from the top. A predicate that answers differently the second time would let the
    body run without the answer, so it has to be deterministic on the same arguments.

    If it raises, we ask. A refund is not skipped because a lambda had a typo.
    """
    try:
        return bool(when(**visible))
    except Exception:
        _log.warning("@wait(when=...) on %s raised; asking anyway", function, exc_info=True)
        return True


def make_wait(framework: Framework) -> Callable[..., Callable[[Callable[..., Any]], Callable[..., Any]]]:
    """Build the `@wait` decorator for one framework."""

    def wait(
        policy: WaitPolicy | None = None,
        *,
        mode: Mode = "interrupt",
        announce: Sequence[AnnounceAdapter] | None = None,
        decision: str = "decision",
        when: Callable[..., bool] | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        resolved = policy or WaitPolicy(allowed_actions=("approve", "reject"))
        if mode == "async" and not announce:
            raise ValueError(
                "@wait(mode='async') publishes from inside the function, so it needs announce=[...]"
            )
        announcers: Sequence[AnnounceAdapter] = list(announce or ())
        hidden = set(framework.hidden_params) | {decision}

        def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
            name = fn.__name__
            signature = inspect.signature(fn)
            wants_decision = decision in signature.parameters
            source = {"function": name}

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                bound = signature.bind_partial(*args, **kwargs)
                bound.apply_defaults()
                call_args: dict[str, Any] = dict(bound.arguments)
                visible = {k: v for k, v in call_args.items() if k not in hidden}

                if when is not None and not _asks(when, visible, name):
                    return fn(**call_args)

                question = {"function": name, "args": visible}

                if mode == "async":
                    thread_id = framework.current_thread_id(call_args)
                    qid = question_id_for(thread_id, name, visible)
                    publish([Question(qid, question, resolved, source=source)], thread_id, announcers)
                    return {"status": "pending_approval", "question_id": qid, "function": name}

                answer: Any = framework.interrupt(pack(question, resolved, source), call_args)
                passthrough = {k: v for k, v in call_args.items() if k != decision}
                if wants_decision:
                    return fn(**passthrough, **{decision: answer})
                verdict = _as_dict(answer)
                if verdict is not None and verdict.get("action") == "approve":
                    edited = _as_dict(verdict.get("args")) or {}
                    return fn(**{**passthrough, **edited})
                return answer

            wrapper.__agent_wait__ = {  # type: ignore[attr-defined]
                "framework": framework.name,
                "mode": mode,
                "policy": resolved,
                "conditional": when is not None,
            }
            return wrapper

        return decorate

    wait.__doc__ = "Make a function wait for an answer from outside the process. See `agent_wait.wait`."
    return wait


# ------------------------------------------------------------------ the after-run call
def make_publish_interrupts(
    framework: Framework,
) -> Callable[..., list[WaitEnvelope]]:
    """Build `publish_interrupts` for one framework."""

    def questions_in(result: Any) -> list[Question]:
        out: list[Question] = []
        for interrupt_id, value in framework.interrupts_in(result):
            question, policy, source = unpack(value)
            out.append(Question(interrupt_id, question, policy, source=source))
        return out

    def publish_interrupts(
        result: Any,
        thread_id: str,
        announce: Sequence[AnnounceAdapter],
        *,
        reply_to: EntryPoint | None = None,
        clock: Clock | None = None,
    ) -> list[WaitEnvelope]:
        """Announce every question `result` parked on. Returns the envelopes; `[]` if none."""
        questions = questions_in(result)
        if not questions:
            return []
        return publish(questions, thread_id, announce, reply_to=reply_to, clock=clock)

    publish_interrupts.questions_in = questions_in  # type: ignore[attr-defined]
    return publish_interrupts
