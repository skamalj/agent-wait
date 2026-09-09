"""The customer's graph. Nothing in here knows about DynamoDB, SQS, tokens or Lambda.

    load_order -> review -> (issue_refund | notify_customer) -> END

`review` asks a human when the amount is large enough to matter. That is the entire
integration: one `ask()` call, in the node where the decision belongs. The graph does not
import `agent_wait`, has no idea where the answer will come from, and would run
identically in a notebook, on a laptop, or on Lambda behind SQS.

`issue_refund` appends to `PAYMENTS_CALLED`. That list is the point of the whole project:
it stands in for the irreversible call, and every scenario in REQUIREMENTS section 13
ends by asserting that it has exactly one entry.
"""

from __future__ import annotations

import os
from typing import Any, TypedDict

from agent_wait import WaitPolicy
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph_wait import ask

PAYMENTS_CALLED: list[str] = []
"""The side effect that must never happen twice."""

APPROVAL_THRESHOLD = 25_000

# Three days in production; the end-to-end run overrides this to two minutes so a
# scripted scenario can actually watch a timeout fire (section 12).
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


def review(state: RefundState) -> RefundState:
    if state["amount"] <= APPROVAL_THRESHOLD:
        return {"decision": {"action": "approve", "by": "policy:auto"}}

    decision = ask(
        {
            "kind": "refund_approval",
            "order_id": state["order_id"],
            "amount": state["amount"],
            "customer": state.get("customer"),
        },
        policy=WaitPolicy(
            timeout=DEMO_TIMEOUT,
            on_timeout="resume_default",
            default={"action": "reject", "reason": f"no response within {DEMO_TIMEOUT}"},
            allowed_actions=("approve", "reject"),
            tags={"approver_group": "finance"},
        ),
    )
    return {"decision": decision}


def issue_refund(state: RefundState) -> RefundState:
    PAYMENTS_CALLED.append(state["order_id"])  # <- the irreversible call
    return {"status": "refunded"}


def notify_customer(state: RefundState) -> RefundState:
    return {"status": state.get("status", "rejected")}


def route(state: RefundState) -> str:
    return "issue_refund" if state["decision"]["action"] == "approve" else "notify_customer"


def build_graph(checkpointer: Any = None) -> Any:
    graph = StateGraph(RefundState)
    graph.add_node("load_order", load_order)
    graph.add_node("review", review)
    graph.add_node("issue_refund", issue_refund)
    graph.add_node("notify_customer", notify_customer)
    graph.add_edge(START, "load_order")
    graph.add_edge("load_order", "review")
    graph.add_conditional_edges("review", route, ["issue_refund", "notify_customer"])
    graph.add_edge("issue_refund", "notify_customer")
    graph.add_edge("notify_customer", END)
    return graph.compile(checkpointer=checkpointer or InMemorySaver())
