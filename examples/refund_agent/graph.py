"""The customer's graph. Nothing in here knows about DynamoDB, SQS or Lambda.

    load_order -> (auto_approve | review) -> (issue_refund | notify_customer) -> END

`review` is a `@hitl` node: calling it parks the graph on a question for finance. That
is the entire integration -- one decorator, on the node where the decision belongs. The
graph has no idea where the answer will come from, and would run identically in a
notebook, on a laptop, or on Lambda behind SQS. Small amounts route around it.

`issue_refund` appends to `PAYMENTS_CALLED`. It stands in for the irreversible call, and
the integration tests end by asserting how many entries it has.
"""

from __future__ import annotations

import os
from typing import Any, TypedDict

from agent_wait import WaitPolicy
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph_wait import hitl

PAYMENTS_CALLED: list[str] = []
"""The side effect that must never happen twice."""

APPROVAL_THRESHOLD = 25_000

# Three days in production; the end-to-end run overrides this to a few minutes so a
# scripted scenario can actually watch a deadline pass.
DEMO_TIMEOUT = os.environ.get("AGENT_WAIT_TIMEOUT", "P3D")


class RefundState(TypedDict, total=False):
    order_id: str
    amount: int
    customer: str
    decision: dict[str, Any]
    status: str


def reset_side_effects() -> None:
    PAYMENTS_CALLED.clear()


def load_order(state: RefundState) -> RefundState:
    return {"customer": f"customer-of-{state['order_id']}"}


FINANCE = WaitPolicy(
    timeout=DEMO_TIMEOUT,
    # Advisory, all of it. `timeout` is published as an absolute `expires_at` and
    # `default` as the value to send when it lapses -- but nothing in agent-wait acts
    # on either. Whoever consumes the envelope decides when the deadline has passed.
    default={"action": "reject", "reason": f"no response within {DEMO_TIMEOUT}"},
    allowed_actions=("approve", "reject"),
    tags={"approver_group": "finance"},
)


def auto_approve(state: RefundState) -> RefundState:
    return {"decision": {"action": "approve", "by": "policy:auto"}}


@hitl(FINANCE)
def review(state: RefundState, decision: dict[str, Any] | None = None) -> RefundState:
    """Parks the graph. Runs only once the answer is in -- `decision` is it, verbatim."""
    return {"decision": decision or {}}


def issue_refund(state: RefundState) -> RefundState:
    PAYMENTS_CALLED.append(state["order_id"])  # <- the irreversible call
    _record_refund(state["order_id"])
    return {"status": "refunded"}


def _record_refund(order_id: str) -> None:
    """Mirror the side effect into DynamoDB when deployed, so the end-to-end run can
    assert on it.

    An in-memory list proves nothing about a Lambda you are not inside. The update is a
    deliberate unconditional `ADD calls 1`: if the node ever ran twice, the counter says
    2, which is exactly the failure the whole library exists to prevent.
    """
    table_name = os.environ.get("AGENT_WAIT_SIDE_EFFECT_TABLE")
    if not table_name:
        return
    import boto3

    boto3.resource("dynamodb").Table(table_name).update_item(
        Key={"pk": f"REFUND#{order_id}", "sk": "#"},
        UpdateExpression="ADD #calls :one",
        ExpressionAttributeNames={"#calls": "calls"},
        ExpressionAttributeValues={":one": 1},
    )


def notify_customer(state: RefundState) -> RefundState:
    return {"status": state.get("status", "rejected")}


def needs_review(state: RefundState) -> str:
    return "review" if state["amount"] > APPROVAL_THRESHOLD else "auto_approve"


def route(state: RefundState) -> str:
    return "issue_refund" if state["decision"].get("action") == "approve" else "notify_customer"


def build_graph(checkpointer: Any = None) -> Any:
    graph = StateGraph(RefundState)
    graph.add_node("load_order", load_order)
    graph.add_node("auto_approve", auto_approve)
    graph.add_node("review", review)
    graph.add_node("issue_refund", issue_refund)
    graph.add_node("notify_customer", notify_customer)
    graph.add_edge(START, "load_order")
    graph.add_conditional_edges("load_order", needs_review, ["review", "auto_approve"])
    graph.add_conditional_edges("auto_approve", route, ["issue_refund", "notify_customer"])
    graph.add_conditional_edges("review", route, ["issue_refund", "notify_customer"])
    graph.add_edge("issue_refund", "notify_customer")
    graph.add_edge("notify_customer", END)
    return graph.compile(checkpointer=checkpointer or InMemorySaver())
