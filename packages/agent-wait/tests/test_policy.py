"""WaitPolicy: duration parsing and JSON round-tripping."""

from __future__ import annotations

import pytest
from agent_wait import PolicyError, WaitPolicy, parse_duration


@pytest.mark.parametrize(
    ("value", "seconds"),
    [
        ("P3D", 259200.0),
        ("PT2H", 7200.0),
        ("PT2H30M", 9000.0),
        ("PT90S", 90.0),
        ("P1W", 604800.0),
        ("P1DT12H", 129600.0),
        ("pt15m", 900.0),
        (120, 120.0),
        (0.5, 0.5),
        (None, None),
    ],
)
def test_parse_duration(value: object, seconds: float | None) -> None:
    assert parse_duration(value) == seconds  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "value",
    ["P1M", "P1Y", "3 days", "", "P", "PT", "-PT1H", 0, -5, True],
)
def test_unparseable_durations_fail_loudly(value: object) -> None:
    """Months and years are refused on purpose: a wait's expiry has to be a definite
    instant we can hand to a scheduler."""
    with pytest.raises(PolicyError):
        parse_duration(value)  # type: ignore[arg-type]


def test_a_bad_timeout_fails_at_construction_not_three_days_later() -> None:
    with pytest.raises(PolicyError):
        WaitPolicy(timeout="P1M")


def test_round_trip_through_json_shape() -> None:
    policy = WaitPolicy(
        timeout="P3D",
        default={"action": "reject"},
        allowed_actions=("approve", "reject"),
        tags={"approver_group": "finance"},
        correlation={"provider": "vendor-api", "id": "job-77"},
    )

    restored = WaitPolicy.from_dict(policy.to_dict())

    assert restored == policy
    assert restored.timeout_seconds == 259200.0


def test_defaults() -> None:
    policy = WaitPolicy()
    assert policy.allowed_actions == ("resume",)
    assert policy.on_timeout == "resume_default"
    assert policy.timeout_seconds is None
    assert WaitPolicy.from_dict(None) == policy
    assert WaitPolicy.from_dict({}) == policy


def test_timeout_is_always_permitted_but_may_not_be_declared() -> None:
    policy = WaitPolicy(allowed_actions=("approve",))
    assert policy.permits("approve") is True
    assert policy.permits("reject") is False
    assert policy.permits("timeout") is True, "the scheduler's action is always allowed"

    with pytest.raises(PolicyError, match="reserved"):
        WaitPolicy(allowed_actions=("approve", "timeout"))


def test_empty_allowed_actions_is_refused() -> None:
    with pytest.raises(PolicyError):
        WaitPolicy(allowed_actions=())


def test_bad_on_timeout_is_refused() -> None:
    with pytest.raises(PolicyError):
        WaitPolicy(on_timeout="explode")  # type: ignore[arg-type]


def test_correlation_key() -> None:
    assert WaitPolicy().correlation_key() is None
    assert WaitPolicy(correlation={"provider": "p", "id": "77"}).correlation_key() == "p:77"
    assert WaitPolicy(correlation={"provider": "p"}).correlation_key() is None
