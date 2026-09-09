"""Exceptions raised by agent-wait.

Note the asymmetry that the design depends on: the *core* raises only for programmer
errors and for token problems surfaced by `TokenCodec.verify()`. `dispatch()` never
raises for an ordinary bad answer -- it returns `Ignore`. Announce adapters never raise
at all (REQUIREMENTS section 8).
"""

from __future__ import annotations


class WaitError(Exception):
    """Base class for every agent-wait error."""


class TokenInvalid(WaitError):
    """The token is malformed, signed with an unknown key, or fails its MAC check."""


class TokenExpired(WaitError):
    """The token's own `exp` has passed. Distinct from the wait having timed out."""


class QuestionTooLarge(WaitError):
    """Question exceeds the v0.1 inline limit of 200 KB (REQUIREMENTS section 16.6)."""


class PolicyError(WaitError):
    """A WaitPolicy could not be parsed -- e.g. an unusable ISO-8601 duration."""


class StoreConflict(WaitError):
    """A conditional write failed in a way the caller did not anticipate."""
