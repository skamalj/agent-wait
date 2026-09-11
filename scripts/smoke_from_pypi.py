"""Prove the *published* packages work: run in a clean venv with nothing but PyPI installs.

    uv venv .smoke && . .smoke/bin/activate
    uv pip install agent-wait langgraph-wait agent-wait-aws
    python scripts/smoke_from_pypi.py

Not an import check. A real graph parks on a question, the envelope is published through
three announcers (in-memory, a signed webhook to a local server, and the AWS package's
adapters constructed against a fake client), the answer is routed back through
`is_answer()` / `resume_command()`, and the graph finishes with the answer verbatim.
Exits non-zero on the first thing that is wrong.
"""

from __future__ import annotations

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, TypedDict

import agent_wait
from agent_wait import InMemoryAnnounce, WaitPolicy, WaitPublisher, WebhookAnnounce, verify_signature
from agent_wait_aws import DynamoDbAnnounce, EventBridgeAnnounce, SnsAnnounce, SqsAnnounce
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph_wait import LangGraphAdapter, ask, is_answer, resume_command

SECRET = b"smoke"
PAID: list[str] = []


def check(condition: bool, what: str) -> None:
    print(f"  {'ok ' if condition else 'FAIL'}  {what}")
    if not condition:
        sys.exit(1)


# ---------------------------------------------------------------- a graph
class State(TypedDict, total=False):
    claim: str
    amount: int
    decision: Any
    status: str


def review(state: State) -> State:
    if state["amount"] <= 100:
        return {"decision": {"action": "approve"}}
    return {
        "decision": ask(
            {"kind": "expense", "claim": state["claim"], "amount": state["amount"]},
            policy=WaitPolicy(
                timeout="PT1H", default={"action": "reject"}, allowed_actions=("approve", "reject")
            ),
        )
    }


def pay(state: State) -> State:
    PAID.append(state["claim"])
    return {"status": "paid"}


def build() -> Any:
    g = StateGraph(State)
    g.add_node("review", review)
    g.add_node("pay", pay)
    g.add_node("decline", lambda s: {"status": "declined"})
    g.add_edge(START, "review")
    g.add_conditional_edges("review", lambda s: "pay" if s["decision"]["action"] == "approve" else "decline")
    g.add_edge("pay", END)
    g.add_edge("decline", END)
    return g.compile(checkpointer=InMemorySaver())


# ---------------------------------------------------------------- a webhook receiver
received: list[dict[str, Any]] = []


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        received.append({"headers": dict(self.headers), "body": body})
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args: object) -> None:
        pass


# ---------------------------------------------------------------- a fake AWS client
class FakeClient:
    """Records calls; the AWS adapters are constructed and driven without a network."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        def call(**kwargs: Any) -> dict[str, Any]:
            self.calls.append(name)
            return {"FailedEntryCount": 0}

        return call


def main() -> None:
    print(
        f"agent-wait {agent_wait.__version__} · langgraph-wait · agent-wait-aws  (from {agent_wait.__file__})"
    )

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}/hook"

    memory = InMemoryAnnounce()
    aws = FakeClient()
    agent = WaitPublisher(
        LangGraphAdapter(build()),
        announce=[
            memory,
            WebhookAnnounce(url, secret=SECRET),
            SnsAnnounce("arn:aws:sns:ap-south-1:000000000000:t", client=aws),
            SqsAnnounce("https://sqs.example/q.fifo", client=aws),
            EventBridgeAnnounce("bus", client=aws),
            DynamoDbAnnounce("approvals", table=aws),
        ],
    )

    print("small claim: no question")
    agent.invoke({"claim": "c-1", "amount": 50}, "c-1")
    check(PAID == ["c-1"], "paid without asking")
    check(memory.events == [], "nothing published")

    print("large claim: parks")
    agent.invoke({"claim": "c-2", "amount": 5000}, "c-2")
    check([t for t, _ in memory.events] == ["created"], "wait.created published")
    envelope = memory.last()
    assert envelope is not None
    check(envelope.question == {"kind": "expense", "claim": "c-2", "amount": 5000}, "question intact")
    check(envelope.expires_at is not None, "expires_at set from PT1H")
    check(envelope.reply_with["interrupt_id"] == envelope.interrupt_id, "reply_with names the interrupt")
    check(len(agent.pending("c-2")) == 1, "pending() sees it")

    print("webhook")
    check(len(received) == 1, "one POST")
    check(received[0]["headers"]["X-Agent-Wait-Event"] == "wait.created", "event header")
    check(
        verify_signature(SECRET, received[0]["body"], received[0]["headers"]["X-Agent-Wait-Signature"]),
        "signature verifies",
    )
    check(json.loads(received[0]["body"])["interrupt_id"] == envelope.interrupt_id, "body matches")

    print("aws adapters")
    check(
        aws.calls == ["publish", "send_message", "put_events", "put_item"],
        f"each adapter delivered: {aws.calls}",
    )

    print("redelivered start: republish, do not re-ask")
    before = envelope.interrupt_id
    if agent.pending("c-2"):
        agent.republish("c-2")
    check(memory.events[-1][1].interrupt_id == before, "same interrupt id republished")
    check(memory.events[-1][1].dedupe_key == envelope.dedupe_key, "same dedupe key")

    print("answer arrives")
    answer = {**dict(envelope.reply_with), "answer": {"action": "approve", "note": "smoke"}}
    check(is_answer(answer), "is_answer() recognises it")
    still_open = any(p.interrupt_id == answer["interrupt_id"] for p in agent.pending("c-2"))
    check(still_open, "still open before resume")
    agent.invoke(resume_command(answer), "c-2")
    check(PAID == ["c-1", "c-2"], "paid exactly once")
    check(agent.pending("c-2") == [], "no longer pending")
    check([t for t, _ in memory.events][-1] == "resumed", "wait.resumed published")
    state = agent.adapter.graph.get_state(agent.adapter.config_for("c-2"))
    check(
        state.values["decision"] == {"action": "approve", "note": "smoke"}, "answer reached the node verbatim"
    )

    print("late duplicate answer")
    still_open = any(p.interrupt_id == answer["interrupt_id"] for p in agent.pending("c-2"))
    check(not still_open, "router would drop it")
    check(PAID == ["c-1", "c-2"], "still paid once")

    server.shutdown()
    print("\nsmoke test passed against the installed packages")


if __name__ == "__main__":
    main()
