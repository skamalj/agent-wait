"""WaitPolicy: duration parsing and JSON round-tripping.

Every field is advisory since v0.2 -- the library publishes the policy and enforces none
of it. What still has to hold is that a malformed policy fails *at the interrupt site*,
in the graph, on the line that got it wrong, rather than reaching a consumer as an
`expires_at` nobody can parse.
"""

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
    """Months and years are refused on purpose: `expires_at` promises a definite
    instant, and "one month from now" is not one."""
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
    assert policy.timeout_seconds is None
    assert policy.default is None
    assert WaitPolicy.from_dict(None) == policy
    assert WaitPolicy.from_dict({}) == policy


def test_empty_allowed_actions_is_refused() -> None:
    """It would publish a question with no answers, which is a question nobody can
    close."""
    with pytest.raises(PolicyError):
        WaitPolicy(allowed_actions=())


def test_an_unknown_action_is_not_refused_here() -> None:
    """The counterpart to the above, and the v0.2 trade in one test.

    `allowed_actions` is published so a UI knows which buttons to draw. Nothing in this
    library checks an answer against it, because no answer ever reaches this library --
    whoever owns the entry point owns that check now. A test asserting a rejection here
    would be asserting a guarantee we no longer make.
    """
    policy = WaitPolicy(allowed_actions=("approve",))

    assert policy.to_dict()["allowed_actions"] == ["approve"]


def test_a_policy_survives_the_trip_through_an_interrupt_value() -> None:
    """The policy rides inside the interrupt payload, so JSON is the only channel it
    has. A field that does not round-trip is a field the consumer never sees."""
    import json

    policy = WaitPolicy(timeout="P3D", default={"action": "reject"}, tags={"team": "finance"})

    assert WaitPolicy.from_dict(json.loads(json.dumps(policy.to_dict()))) == policy


def test_a_duration_that_parses_to_zero_is_still_refused() -> None:
    """`PT0S` matches the grammar but means "already expired", which no author intends."""
    with pytest.raises(PolicyError, match="positive"):
        parse_duration("PT0S")
