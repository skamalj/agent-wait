# Architecture

## The problem

Every agent framework can pause. LangGraph has `interrupt()`, Strands has interrupts,
Pydantic AI has `DeferredToolRequests`, ADK has long-running tools. None of them owns what
happens *between* the pause and the resume when the gap is three days and the process
that paused is gone.

The maintainers have said so, in writing. LangChain forum #2541: scheduling and timing are
"left to the application layer … last write wins". claude-agent-sdk-python #871: "we don't
want to keep the subprocess alive while waiting for the user". crewAI #2051: async
human-in-the-loop is Enterprise-only. langgraph #8039: after a crash, whether a node is
replayed or re-executed is host-dependent — so you cannot lean on checkpoint resume alone
for side-effect safety.

So everybody builds the same thing badly: a table of pending approvals, a cron job that
might fire twice, a webhook with no idempotency, and a refund that occasionally goes out
twice. agent-wait is that layer, built once.

## The shape

```mermaid
flowchart TB
    subgraph agent["the agent (your code, unchanged)"]
        NODE["node calls<br/><b>ask(question, policy)</b>"]
        GRAPH["graph.invoke()"]
    end

    subgraph runtime["agent-wait"]
        DISPATCH["<b>dispatch(payload)</b><br/>start? resume? ignore?"]
        REGISTER["<b>register(result)</b><br/>persist · announce"]
        STORE[("WaitStore<br/>conditional writes")]
    end

    subgraph world["the world"]
        SLACK["approvals UI<br/>Slack · email · ticket"]
        SCHED["EventBridge Scheduler<br/><i>delivery to the future</i>"]
    end

    ENTRY{{"the agent's existing entry point<br/>SQS FIFO · HTTP · Step Functions"}}

    ENTRY -->|"every inbound payload"| DISPATCH
    DISPATCH -->|"Start · Resume"| GRAPH
    DISPATCH -->|"Ignore: duplicate, stale,<br/>forged, already answered"| ACK["acknowledge<br/>and do nothing"]
    GRAPH --> NODE
    NODE -->|"interrupt"| GRAPH
    GRAPH -->|"result"| REGISTER
    REGISTER <--> STORE
    DISPATCH <--> STORE
    REGISTER -->|"wait envelope"| SLACK
    REGISTER -->|"arm the timeout"| SCHED
    SLACK -->|"answer envelope"| ENTRY
    SCHED -->|"answer envelope,<br/>action=timeout, 3 days later"| ENTRY

    classDef quiet fill:#f6f6f6,stroke:#bbb,color:#444
    class ACK quiet
```

Read the arrows into `ENTRY`. The approvals UI and the scheduler send **the same kind of
message to the same place** — the queue the agent already had. That is the central idea,
and everything else follows from it.

## What that buys

**No second entry point.** No answer API to build, secure, monitor and rate-limit. One
queue policy, one authoriser, one thing that can be misconfigured.

**Timeouts stop being special.** A timeout is an answer with `action: "timeout"`,
delivered by a messenger that is three days slow. It goes through the identical code path
as a human clicking Approve — which is why "the timer fired at the same instant somebody
clicked" needs no handling at all. It is one conditional write; exactly one wins.

**No timer Lambda.** `SchedulerAnnounce` is an *announce adapter*. Telling the future is
the same act as telling Slack, so it lives behind the same one-method interface.

**Ordering is free.** SQS FIFO with `MessageGroupId = thread_id` gives one in-flight
message per conversation. That is the answer to LangChain forum #772 — "how do I ensure
only one thread_id runs at a time across many pods?" — unanswered for fourteen months.

## The three functions

```python
decision = ask(question, policy)  # in your node, instead of interrupt()

outcome = runtime.dispatch(payload)  # before graph.invoke()
result = runtime.register(out, config, thread_id)  # after it
```

That is the whole public surface, plus the `AnnounceAdapter` protocol and the two
[message formats](message-formats.md).

### `dispatch()` — the gate

Every inbound payload goes through it, and its job is to say **no** correctly. A payload
with no `token` is a start. A payload with one is an answer, and it survives: token
verification (no store read, so forged tokens cost one HMAC), the binding check (this
token is for *this* question), the allowed-actions check, the status check, the thread
lease, and finally a conditional write.

It never raises for a bad answer. A double click, a stale approval, a late timer and a
forged token are all `Ignore` — because a healthy system receives all four every day. Only
`lease_held` asks the caller to retry.

### `register()` — the ledger

Called after *every* run, including successful ones, because that is when waits left
behind by an earlier interrupt get closed out.

It computes `sha256(thread_id | interrupt_id | checkpoint_id)` and creates the wait
idempotently. Crash after the checkpoint but before the announce, and the redelivery finds
the same wait rather than making a second one. Announce once — `notified_at` is the flag —
then check whether an answer arrived before the wait existed, and if so apply it straight
away.

### The sweeper — the repair pass

`register()` does three things that are not one atomic act: create, announce, arm. A crash
in between leaves a wait nobody knows about and no timer will fire for — a thread parked
forever. The sweeper runs every minute, finds pending waits with no `notified_at`, and
announces them. It needs no graph and no checkpointer; it only ever pushes waits back onto
the announce path and lets `dispatch()` do the deciding.

## Why the store is the only hard dependency

`WaitStore` needs exactly one primitive: **a conditional write that tells you whether you
won**. `transition()` returns a bool, not an exception, and every state change goes through
it. The loser re-reads and reports what actually happened.

That is why there are three implementations — in-memory, SQLite, DynamoDB — and one
conformance suite that runs against all three. A rule that passes only against the
in-memory store has not been tested, it has been asserted.

## What is deliberately not here

**Authentication.** `actor` in an answer envelope is a label. Anyone holding the token can
write any string there. Trust comes from the entry point — the queue policy, the API
authoriser, the IAM role. Pretending otherwise would be worse than not trying.

**A UI.** Envelopes carry `tags`, which become SNS message attributes, so filter policies
route to whatever already exists. The agent never learns your service is there.

**Cross-framework generality, yet.** `FrameworkAdapter` is four methods and the core
imports no framework, so Strands or Pydantic AI is an afternoon. But v0.1 ships one
adapter that is genuinely tested rather than four that are plausible.

**Step Functions hosting.** Designed for, not built. A Park state
(`sqs:sendMessage.waitForTaskToken` with `TimeoutSecondsPath`) would announce and time out
natively and feed `SendTaskSuccess` output back to `dispatch()` unchanged. Nothing in the
core prevents it.

## The one framework bug we absorb

LangGraph's `get_state().tasks[*].interrupts` over-reports: after resuming one of two
parallel interrupts, the *finished* task still advertises its interrupt id
(langgraph #4796 / #6792). A naive `still_pending()` would therefore say "yes, still
parked" about an interrupt the graph has already moved past.

`task.result` is the discriminator — populated for a finished task, `None` for a parked
one — and `LangGraphAdapter.still_pending()` checks both. The behaviour is pinned by a
test, so if LangGraph fixes it we find out from a failure rather than from silence.

It is a second line of defence in any case. The first is the status compare-and-set in
`dispatch()`, which rejects a redelivered answer before the graph is ever invoked.
