"""`WaitPolicy` -- what the author of an interrupt declares about the wait.

The policy rides inside the interrupt value under the `__wait__` key (REQUIREMENTS
section 16.2), because that is the only channel a framework interrupt gives us. It must
therefore round-trip through JSON without loss.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from .errors import PolicyError

OnTimeout = Literal["resume_default", "fail"]

# ISO-8601 durations, restricted to the parts that make sense for a wait. Years and
# months are refused on purpose: they are not fixed-length, and a wait's expiry has to
# be a definite instant we can hand to EventBridge Scheduler.
_ISO_DURATION = re.compile(
    r"^P(?!$)(?:(?P<weeks>\d+(?:\.\d+)?)W)?(?:(?P<days>\d+(?:\.\d+)?)D)?"
    r"(?:T(?!$)(?:(?P<hours>\d+(?:\.\d+)?)H)?(?:(?P<minutes>\d+(?:\.\d+)?)M)?"
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$"
)


def _no_tags() -> dict[str, str]:
    """A typed factory: bare `dict` leaves the field's value type unknown."""
    return {}


_UNIT_SECONDS = {
    "weeks": 604800.0,
    "days": 86400.0,
    "hours": 3600.0,
    "minutes": 60.0,
    "seconds": 1.0,
}


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
    """Declared, at the interrupt site, by whoever asked the question."""

    timeout: str | int | None = None
    on_timeout: OnTimeout = "resume_default"
    default: Any = None
    allowed_actions: tuple[str, ...] = ("resume",)
    tags: Mapping[str, str] = field(default_factory=_no_tags)
    correlation: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        # Fail at construction, not three days later when the scheduler should fire.
        parse_duration(self.timeout)
        if self.on_timeout not in ("resume_default", "fail"):
            raise PolicyError(f"on_timeout must be 'resume_default' or 'fail', got {self.on_timeout!r}")
        if not self.allowed_actions:
            raise PolicyError("allowed_actions must not be empty")
        if "timeout" in self.allowed_actions:
            raise PolicyError(
                "'timeout' is reserved for the scheduler and is always permitted; "
                "do not list it in allowed_actions"
            )

    @property
    def timeout_seconds(self) -> float | None:
        return parse_duration(self.timeout)

    def to_dict(self) -> dict[str, Any]:
        return {
            "timeout": self.timeout,
            "on_timeout": self.on_timeout,
            "default": self.default,
            "allowed_actions": list(self.allowed_actions),
            "tags": dict(self.tags),
            "correlation": dict(self.correlation) if self.correlation is not None else None,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> WaitPolicy:
        if not data:
            return cls()
        actions = data.get("allowed_actions") or ("resume",)
        correlation = data.get("correlation")
        return cls(
            timeout=data.get("timeout"),
            on_timeout=data.get("on_timeout", "resume_default"),
            default=data.get("default"),
            allowed_actions=tuple(actions),
            tags=dict(data.get("tags") or {}),
            correlation=dict(correlation) if correlation else None,
        )

    def permits(self, action: str) -> bool:
        """`timeout` is always allowed -- it is the scheduler's action, not a human's."""
        return action == "timeout" or action in self.allowed_actions

    def correlation_key(self) -> str | None:
        if not self.correlation:
            return None
        provider = self.correlation.get("provider", "")
        ident = self.correlation.get("id", "")
        return f"{provider}:{ident}" if ident else None
