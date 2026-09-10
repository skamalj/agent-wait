"""The two exceptions agent-wait raises, both at the interrupt site.

There are no runtime errors to speak of. Announce adapters are forbidden from raising
(see `announce/base.py`) and nothing else in the library makes a decision that can fail.
"""

from __future__ import annotations


class WaitError(Exception):
    """Base class, so `except WaitError` catches everything this library raises."""


class PolicyError(WaitError):
    """A `WaitPolicy` that cannot mean anything -- an unparseable timeout, an empty
    `allowed_actions`. Raised at construction, in the graph, where the mistake is."""


class QuestionTooLarge(WaitError):
    """The question would not survive the trip.

    An envelope has to fit through whatever the announce adapter is: SNS caps a message
    at 256 KB, EventBridge at 256 KB, SQS at 256 KB. Finding out at publish time means a
    parked interrupt nobody ever hears about, so the size is checked when the question is
    asked instead.
    """
