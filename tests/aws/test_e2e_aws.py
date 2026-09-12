"""The end-to-end level, against a real deployed stack.

Opt in:

    AGENT_WAIT_E2E=1 AGENT_WAIT_E2E_STACK=agent-wait-poc-ks uv run pytest -m e2e

Skipped by default and in CI, because it needs credentials, a deployed stack, and several
minutes of real waiting for a real deadline to pass. The same scenarios can be run
directly with `examples/refund_agent/demo_scenarios.py`, which is what the deploy script
does and what produces the evidence in `reports/`.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.e2e

ENABLED = os.environ.get("AGENT_WAIT_E2E") == "1"
STACK = os.environ.get("AGENT_WAIT_E2E_STACK", "agent-wait-poc")
REGION = os.environ.get("AGENT_WAIT_E2E_REGION", "ap-south-1")
TIMEOUT_SECONDS = int(os.environ.get("AGENT_WAIT_E2E_TIMEOUT", "120"))

pytest.importorskip("boto3")

skip_unless_enabled = pytest.mark.skipif(
    not ENABLED, reason="set AGENT_WAIT_E2E=1 and deploy the stack to run the end-to-end level"
)


@pytest.fixture(scope="module")
def deployment():  # type: ignore[no-untyped-def]
    """Note: this fixture deliberately does *not* use the moto autouse fixtures from
    `conftest.py`; those are function-scoped and this module talks to real AWS."""
    from refund_agent.demo_scenarios import Deployment

    return Deployment(STACK, REGION)


@pytest.fixture()
def journal():  # type: ignore[no-untyped-def]
    from refund_agent.demo_scenarios import Journal

    return Journal()


@skip_unless_enabled
def test_scenario_a_the_happy_path_and_every_stale_answer(deployment, journal) -> None:  # type: ignore[no-untyped-def]
    from refund_agent.demo_scenarios import scenario_a

    deployment.drain_announcements()
    scenario_a(deployment, journal)
    assert journal.failures == []


@skip_unless_enabled
def test_scenario_b_the_consumer_enforces_the_timeout(deployment, journal) -> None:  # type: ignore[no-untyped-def]
    from refund_agent.demo_scenarios import scenario_b

    deployment.drain_announcements()
    scenario_b(deployment, journal, timeout_seconds=TIMEOUT_SECONDS)
    assert journal.failures == []


@skip_unless_enabled
def test_scenario_c_a_redelivered_answer_does_not_refund_twice(deployment, journal) -> None:  # type: ignore[no-untyped-def]
    from refund_agent.demo_scenarios import scenario_c

    deployment.drain_announcements()
    scenario_c(deployment, journal)
    assert journal.failures == []


@skip_unless_enabled
def test_scenario_d_a_lost_announce_is_repaired_by_the_redelivery(deployment, journal) -> None:  # type: ignore[no-untyped-def]
    from refund_agent.demo_scenarios import scenario_d

    deployment.drain_announcements()
    scenario_d(deployment, journal)
    assert journal.failures == []
