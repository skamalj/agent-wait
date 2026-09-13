"""Prove the *published* package works: run in a clean venv with nothing but PyPI installs.

    uv venv .smoke && . .smoke/bin/activate
    uv pip install "agent-wait[langgraph,pydantic-ai,strands,aws]"
    python scripts/smoke_from_pypi.py

Not an import check. Real graphs, both modes, every announcer, the round trip, and the
same decorator on Pydantic AI and Strands agents.
Exits non-zero on the first thing that is wrong.
"""

from __future__ import annotations

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command
from pydantic_ai import Agent, DeferredToolRequests, DeferredToolResults, RunContext, ToolApproved
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import FunctionModel
from strands import Agent as StrandsAgent
from strands import tool as strands_tool
from strands.models import Model as StrandsModel
from strands.types.tools import ToolContext

import agent_wait
from agent_wait import InMemoryAnnounce, WaitPolicy, WebhookAnnounce, verify_signature
from agent_wait.aws import DynamoDbAnnounce, EventBridgeAnnounce, SnsAnnounce, SqsAnnounce
from agent_wait.langgraph import publish_interrupts, wait
from agent_wait.pydantic_ai import publish_interrupts as publish_pai
from agent_wait.pydantic_ai import wait as wait_pai
from agent_wait.strands import publish_interrupts as publish_strands
from agent_wait.strands import wait as wait_strands

SECRET = b"smoke"
PAID: list[str] = []
received: list[dict[str, Any]] = []


def check(condition: bool, what: str) -> None:
    print(f"  {'ok ' if condition else 'FAIL'}  {what}")
    if not condition:
        sys.exit(1)


class State(TypedDict, total=False):
    claim: str
    amount: int
    decision: Any
    status: str


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        received.append({"headers": dict(self.headers), "body": body})
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args: object) -> None:
        pass


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        def call(**kwargs: Any) -> dict[str, Any]:
            self.calls.append(name)
            return {"FailedEntryCount": 0}

        return call


POLICY = WaitPolicy(timeout="PT1H", default={"action": "reject"}, allowed_actions=("approve", "reject"))


@wait(POLICY)
def review(state: State, decision: Any = None) -> State:
    return {"decision": decision}


def interrupt_graph() -> Any:
    def pay(s: State) -> State:
        PAID.append(s["claim"])
        return {"status": "paid"}

    g = StateGraph(State)
    g.add_node("triage", lambda s: {})
    g.add_node("review", review)
    g.add_node("auto", lambda s: {"decision": {"action": "approve"}})
    g.add_node("pay", pay)
    g.add_node("decline", lambda s: {"status": "declined"})
    g.add_edge(START, "triage")
    g.add_conditional_edges("triage", lambda s: "auto" if s["amount"] <= 100 else "review")
    g.add_conditional_edges("auto", lambda s: "pay")
    g.add_conditional_edges("review", lambda s: "pay" if s["decision"]["action"] == "approve" else "decline")
    g.add_edge("pay", END)
    g.add_edge("decline", END)
    return g.compile(checkpointer=InMemorySaver())


def pydantic_ai_section(memory: InMemoryAnnounce) -> None:
    """The same decorator on a Pydantic AI agent: park, publish, approve with edited args."""

    def model(messages: Any, info: Any) -> ModelResponse:
        for message in reversed(messages):
            for part in getattr(message, "parts", []):
                if isinstance(part, ToolReturnPart):
                    return ModelResponse(parts=[TextPart(f"done: {part.content}")])
        return ModelResponse(
            parts=[ToolCallPart(tool_name="issue_refund", args={"order_id": "o-1", "amount": 250})]
        )

    agent: Any = Agent(FunctionModel(model), output_type=[str, DeferredToolRequests])
    paid: list[int] = []

    @agent.tool
    @wait_pai(POLICY)
    def issue_refund(ctx: RunContext[None], order_id: str, amount: int) -> str:
        paid.append(amount)
        return f"refunded {amount}"

    print("pydantic-ai: the same @wait parks a run")
    parked = agent.run_sync("refund o-1")
    check(
        isinstance(parked.output, DeferredToolRequests) and paid == [],
        "run ended on DeferredToolRequests, body not run",
    )
    (envelope,) = publish_pai(parked, "p-1", [memory])
    (call,) = parked.output.approvals
    check(envelope.question_id == call.tool_call_id, "question_id is the tool_call_id")
    check(
        envelope.question == {"function": "issue_refund", "args": {"order_id": "o-1", "amount": 250}},
        "ctx hidden",
    )

    done = agent.run_sync(
        None,
        message_history=parked.all_messages(),
        deferred_tool_results=DeferredToolResults(
            approvals={call.tool_call_id: ToolApproved()},
            metadata={call.tool_call_id: {"action": "approve", "args": {"amount": 100}}},
        ),
    )
    check(done.output == "done: refunded 100" and paid == [100], "approved with edited args, body ran once")


class ScriptedStrandsModel(StrandsModel):
    """First turn: call issue_refund. Next turn: say done."""

    def __init__(self) -> None:
        self.turn = 0

    def update_config(self, **model_config: Any) -> None:
        pass

    def get_config(self) -> Any:
        return {}

    async def structured_output(
        self, output_model: Any, prompt: Any, system_prompt: Any = None, **kw: Any
    ) -> Any:
        yield {}

    async def stream(
        self, messages: Any, tool_specs: Any = None, system_prompt: Any = None, **kw: Any
    ) -> Any:
        yield {"messageStart": {"role": "assistant"}}
        if self.turn == 0:
            yield {"contentBlockStart": {"start": {"toolUse": {"name": "issue_refund", "toolUseId": "tu-1"}}}}
            yield {
                "contentBlockDelta": {
                    "delta": {"toolUse": {"input": json.dumps({"order_id": "o-1", "amount": 250})}}
                }
            }
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "tool_use"}}
        else:
            yield {"contentBlockStart": {"start": {}}}
            yield {"contentBlockDelta": {"delta": {"text": "done"}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "end_turn"}}
        self.turn += 1


def strands_section(memory: InMemoryAnnounce) -> None:
    """The same decorator on a Strands agent: park, publish, approve with edited args."""
    paid: list[int] = []

    @strands_tool(context=True)
    @wait_strands(POLICY)
    def issue_refund(order_id: str, amount: int, tool_context: ToolContext) -> str:
        paid.append(amount)
        return f"refunded {amount}"

    print("strands: the same @wait parks a run")
    agent = StrandsAgent(model=ScriptedStrandsModel(), tools=[issue_refund], callback_handler=None)
    parked = agent("refund o-1")
    check(parked.stop_reason == "interrupt" and paid == [], "run stopped on interrupt, body not run")
    (envelope,) = publish_strands(parked, agent.session_id, [memory])
    (interrupt,) = parked.interrupts
    check(envelope.question_id == interrupt.id, "question_id is the interrupt id")
    check(
        envelope.question == {"function": "issue_refund", "args": {"order_id": "o-1", "amount": 250}},
        "tool_context hidden",
    )

    done = agent(
        [
            {
                "interruptResponse": {
                    "interruptId": interrupt.id,
                    "response": {"action": "approve", "args": {"amount": 100}},
                }
            }
        ]
    )
    check(done.stop_reason == "end_turn" and paid == [100], "approved with edited args, body ran once")


def main() -> None:
    print(f"agent-wait {agent_wait.__version__}  (from {agent_wait.__file__})")
    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}/hook"

    memory, aws = InMemoryAnnounce(), FakeClient()
    announce = [
        memory,
        WebhookAnnounce(url, secret=SECRET),
        SnsAnnounce("arn:aws:sns:ap-south-1:000000000000:t", client=aws),
        SqsAnnounce("https://sqs.example/q.fifo", client=aws),
        EventBridgeAnnounce("bus", client=aws),
        DynamoDbAnnounce("approvals", table=aws),
    ]
    cfg = lambda t: {"configurable": {"thread_id": t}}  # noqa: E731

    print("interrupt mode: small claim, no question")
    graph = interrupt_graph()
    check(
        publish_interrupts(graph.invoke({"claim": "c-1", "amount": 50}, cfg("c-1")), "c-1", announce) == [],
        "nothing published",
    )
    check(PAID == ["c-1"], "paid without asking")

    print("interrupt mode: large claim parks")
    (envelope,) = publish_interrupts(
        graph.invoke({"claim": "c-2", "amount": 5000}, cfg("c-2")), "c-2", announce
    )
    check(
        envelope.question["function"] == "review" and envelope.question["args"]["state"]["amount"] == 5000,
        "question intact",
    )
    check(envelope.expires_at is not None, "expires_at set from PT1H")
    check(envelope.reply_with["question_id"] == envelope.question_id, "reply_with names the question")
    check(envelope.to_dict()["reply_to"] is None, "reply_to is optional and null")

    print("announcers")
    check(
        len(received) == 1 and received[0]["headers"]["X-Agent-Wait-Event"] == "wait.created",
        "webhook posted",
    )
    check(
        verify_signature(SECRET, received[0]["body"], received[0]["headers"]["X-Agent-Wait-Signature"]),
        "signature verifies",
    )
    check(json.loads(received[0]["body"])["question_id"] == envelope.question_id, "webhook body matches")
    check(
        aws.calls == ["publish", "send_message", "put_events", "put_item"],
        f"aws adapters delivered: {aws.calls}",
    )

    print("the answer comes back through the stub")
    reply = {**dict(envelope.reply_with), "answer": {"action": "approve", "note": "smoke"}}
    result = graph.invoke(Command(resume={reply["question_id"]: reply["answer"]}), cfg(reply["thread_id"]))
    check(publish_interrupts(result, "c-2", announce) == [], "nothing left parked")
    check(PAID == ["c-1", "c-2"], "paid exactly once")
    check(
        graph.get_state(cfg("c-2")).values["decision"] == {"action": "approve", "note": "smoke"},
        "answer verbatim",
    )
    graph.invoke(Command(resume={reply["question_id"]: reply["answer"]}), cfg("c-2"))
    check(PAID == ["c-1", "c-2"], "a duplicate answer runs nothing")

    print("async mode: the decorator publishes, the graph carries on")
    inbox = InMemoryAnnounce()

    @wait(POLICY, mode="async", announce=[inbox])
    def big_transfer(account: str, amount: int) -> str:
        PAID.append(account)
        return "transferred"

    g = StateGraph(State)
    g.add_node("transfer", lambda s: {"decision": big_transfer(account="acc-9", amount=5000)})
    g.add_node("after", lambda s: {"status": "carried on"})
    g.add_edge(START, "transfer")
    g.add_edge("transfer", "after")
    g.add_edge("after", END)
    out = g.compile(checkpointer=InMemorySaver()).invoke({}, cfg("t"))
    check("__interrupt__" not in out, "nothing parked")
    check(out["decision"]["status"] == "pending_approval", "tool returned pending")
    check(out["status"] == "carried on", "next node ran in the same invoke")
    check(PAID == ["c-1", "c-2"], "tool body did not run")
    check(
        len(inbox.events) == 1 and inbox.events[0][1].source == {"function": "big_transfer"},
        "decorator published it",
    )

    pydantic_ai_section(memory)
    strands_section(memory)

    server.shutdown()
    print("\nsmoke test passed against the installed package")


if __name__ == "__main__":
    main()
