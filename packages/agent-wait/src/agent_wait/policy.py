"""`WaitPolicy` -- what the author of an interrupt declares about the wait.

Every field here is **advisory**. agent-wait publishes the policy and does nothing else
with it: it does not enforce the timeout, it does not apply the default, it does not
reject a disallowed action. It cannot -- since v0.2 the library never sees the answer.

That is a deliberate trade (see `docs/migrating-from-0.1.md`). What these fields buy you
is that the *world* is told the rules in a machine-readable way, on the same envelope as
the question, so the system that does the enforcing has what it needs:

* `timeout` becomes `expires_at` -- an absolute instant, so a scheduler, a cron sweep or
  a UI countdown can act on it without re-parsing a duration;
* `allowed_actions` tells a UI which buttons to draw;
* `default` tells whoever enforces the timeout what to send when it fires;
* `tags` become SNS message attributes, so filter policies can route on them.

The policy rides inside the interrupt value under the `__wait__` key, because that is the
only channel a framework interrupt gives us, so it must round-trip through JSON.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .errors import PolicyError

# ISO-8601 durations, restricted to the parts that make sense for a wait. Years and
# months are refused on purpose: they are not fixed-length, so "P1M" cannot be turned
# into the definite instant that `expires_at` promises to be.
_ISO_DURATION = re.compile(
    r"^P(?!$)(?:(?P<weeks>\d+(?:\.\d+)?)W)?(?:(?P<days>\d+(?:\.\d+)?)D)?"
    r"(?:T(?!$)(?:(?P<hours>\d+(?:\.\d+)?)H)?(?:(?P<minutes>\d+(?:\.\d+)?)M)?"
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$"
)

_UNIT_SECONDS = {
    "weeks": 604800.0,
    "days": 86400.0,
    "hours": 3600.0,
    "minutes": 60.0,
    "seconds": 1.0,
}


def _no_tags() -> dict[str, str]:
    """A typed factory: a bare `dict` leaves the field's value type unknown."""
    return {}


def parse_duration(value: str | int | float | None) -> float | None:
    """Return seconds for an ISO-8601 duration string, a number, or None.

    >>> parse_duration("P3D")
    259200.0
    >>> parse_duration("PT2H30M")
    9000.0
    """
    if value is None:
        return None
    if isinstance(value, bool):  # bool is an int subclass; almost certainly a mistake
        raise PolicyError(f"timeout must be a duration, got {value!r}")
    if isinstance(value, int | float):
        if value <= 0:
            raise PolicyError(f"timeout must be positive, got {value!r}")
        return float(value)
    match = _ISO_DURATION.match(value.strip().upper())
    if match is None:
        raise PolicyError(
            f"unparseable timeout {value!r}: expected seconds or an ISO-8601 duration "
            "such as 'P3D' or 'PT2H30M' (years and months are not accepted)"
        )
    total = sum(_UNIT_SECONDS[k] * float(v) for k, v in match.groupdict().items() if v is not None)
    if total <= 0:
        raise PolicyError(f"timeout must be positive, got {value!r}")
    return total


@dataclass(frozen=True)
class WaitPolicy:
    """Declared at the interrupt site, by whoever asked the question. All advisory."""

    timeout: str | int | None = None
    default: Any = None
    allowed_actions: tuple[str, ...] = ("resume",)
    tags: Mapping[str, str] = field(default_factory=_no_tags)
    correlation: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        # Fail here, in the graph, at the line that got it wrong -- not three days later
        # in whatever consumer tried to read `expires_at`.
        parse_duration(self.timeout)
        if not self.allowed_actions:
            raise PolicyError("allowed_actions must not be empty")

    @property
    def timeout_seconds(self) -> float | None:
        return parse_duration(self.timeout)

    def to_dict(self) -> dict[str, Any]:
        return {
            "timeout": self.timeout,
            "default": self.default,
            "allowed_actions": list(self.allowed_actions),
            "tags": dict(self.tags),
            "correlation": dict(self.correlation) if self.correlation is not None else None,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> WaitPolicy:
        if not data:
            return cls()
        correlation = data.get("correlation")
        return cls(
            timeout=data.get("timeout"),
            default=data.get("default"),
            allowed_actions=tuple(data.get("allowed_actions") or ("resume",)),
            tags=dict(data.get("tags") or {}),
            correlation=dict(correlation) if correlation else None,
        )
