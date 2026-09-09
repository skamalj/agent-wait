"""The customer's graph. Nothing in here knows about DynamoDB, SQS or tokens."""
from typing import TypedDict

from langgraph.checkpoint.memory import InMemorySaver   # stands in for DynamoDBSaver in the sim
from langgraph.graph import END, START, StateGraph

from agent_wait.core import WaitPolicy
from agent_wait.langgraph_adapter import ask

PAYMENTS_CALLED: list[str] = []          # the side effect we must never repeat


class RefundState(TypedDict, total=False):
    order_id: str
    amount: int
    customer: str
    decision: dict
    status: str


def load_order(state: RefundState) -> RefundState:
    return {"customer": f"customer-of-{state['order_id']}"}


def review(state: RefundState) -> RefundState:
    if state["amount"] <= 25_000:
        return {"decision": {"action": "approve", "by": "policy:auto"}}
    decision = ask(
        {"kind": "refund_approval", "order_id": state["order_id"], "amount": state["amount"]},
        policy=WaitPolicy(
            timeout_s=3 * 86400,
            on_timeout="resume_default",
            default={"action": "reject", "reason": "no response in 3 days"},
            allowed_actions=["approve", "reject"],
            tags={"approver_group": "finance"},
        ),
    )
    return {"decision": decision}


def issue_refund(state: RefundState) -> RefundState:
    PAYMENTS_CALLED.append(state["order_id"])          # <- the irreversible call
    return {"status": "refunded"}


def notify_customer(state: RefundState) -> RefundState:
    return {"status": state.get("status", "rejected")}


def route(state: RefundState) -> str:
    return "issue_refund" if state["decision"]["action"] == "approve" else "notify_customer"


def build_graph(checkpointer=None):
    g = StateGraph(RefundState)
    g.add_node("load_order", load_order)
    g.add_node("review", review)
    g.add_node("issue_refund", issue_refund)
    g.add_node("notify_customer", notify_customer)
    g.add_edge(START, "load_order")
    g.add_edge("load_order", "review")
    g.add_conditional_edges("review", route, ["issue_refund", "notify_customer"])
    g.add_edge("issue_refund", "notify_customer")
    g.add_edge("notify_customer", END)
    return g.compile(checkpointer=checkpointer or InMemorySaver())
