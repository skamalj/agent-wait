# Supported frameworks and providers

One distribution, `agent-wait`. The bare install is the framework- and provider-free core
and depends on nothing. Each extra adds one subpackage and exactly one dependency.
Importing a subpackage without its extra raises an `ImportError` that names the extra.

```bash
pip install agent-wait                                # core only
pip install "agent-wait[langgraph]"                   # + agent_wait.langgraph
pip install "agent-wait[pydantic-ai]"                 # + agent_wait.pydantic_ai
pip install "agent-wait[aws]"                         # + agent_wait.aws
pip install "agent-wait[langgraph,pydantic-ai,aws]"   # any combination; [all] is all of them
```

## Frameworks

A framework subpackage binds the two names a user imports — `wait` and
`publish_interrupts` — to one implementor of the three-method `Framework` interface.
The decorator, the envelope, the announcers and the answer contract are the same on
every framework; only the framework's own resume call differs.

| Framework | Extra · subpackage | Dependency | Status | Since |
|---|---|---|---|---|
| **LangGraph** | `[langgraph]` · `agent_wait.langgraph` | `langgraph>=1.2,<2` | supported; tested against 1.2.11 and on a deployed Lambda | 0.1 |
| **Pydantic AI** | `[pydantic-ai]` · `agent_wait.pydantic_ai` | `pydantic-ai-slim>=2.43,<3` | supported; tested against 2.43.0 with real agents (`FunctionModel`) | 0.6 |
| Strands Agents | — | — | designed against, not built | — |
| CrewAI | — | — | considered and declined: OSS Flows already hand the interrupt to a provider you write, and the semantics differ (the decorated method always runs; the human reviews its output) | — |

### What each framework gives, and what it leaves to you

| | LangGraph | Pydantic AI |
|---|---|---|
| how a `@wait` call parks | `langgraph.types.interrupt(value)` inside the call | `raise ApprovalRequired(metadata=value)` inside the call |
| where `publish_interrupts` reads | `result["__interrupt__"]` → `(Interrupt.id, Interrupt.value)` | `result.output` when it is a `DeferredToolRequests` → `(tool_call_id, metadata[id])` |
| `question_id` | `Interrupt.id` | `tool_call_id` |
| `thread_id` | `config["configurable"]["thread_id"]` — the framework's | yours: the name you store `message_history` under; for async mode, `deps.thread_id` |
| persistence | the checkpointer you gave the graph | none — `message_history` is a list the host stores |
| resume (host code) | `graph.invoke(Command(resume={id: answer}), config)` | `agent.run(None, message_history=h, deferred_tool_results=DeferredToolResults(approvals={id: ToolApproved()/ToolDenied(...)}, metadata={id: answer}))` |
| duplicate answer | ignored by the framework; nothing to check | refused only if the stored history already holds the tool's return — **store the post-run history before acknowledging the answer** |
| "anything but approve" | the wrapper returns the answer in place of the body | `ToolDenied` — the framework skips the tool and shows the model the message |
| tools that never heard of this library | a bare `interrupt(value)` is published with the default policy | `requires_approval=True` and `CallDeferred` tools are published with the default policy |
| one thing to name | — | the `RunContext` parameter is `ctx` |
| known limits | one `interrupt()` per node on 1.2.x (#6626); not compatible with `HumanInTheLoopMiddleware` on the same tool | `message_history` must be stored by the host; a `@wait` tool without `RunContext` is refused up front |

Both are covered by the same core contract test (`tests/core/test_framework.py`), which
drives `@wait` and `publish_interrupts` through a stub framework with neither installed.
That test is what a new implementor has to satisfy, and nothing in the core changes to
add one — Pydantic AI was added without touching it.

## Providers

A provider subpackage is implementors of `BaseAnnounce` — one class per place a question
can land. The core ships the provider-free ones.

| Provider | Extra · subpackage | Dependency | Announcers | Status |
|---|---|---|---|---|
| **core** | — · `agent_wait` | none | `WebhookAnnounce` (signed JSON POST, stdlib), `LogAnnounce`, `InMemoryAnnounce`, `CompositeAnnounce` | supported |
| **AWS** | `[aws]` · `agent_wait.aws` | `boto3>=1.35` | `SnsAnnounce`, `SqsAnnounce`, `EventBridgeAnnounce`, `DynamoDbAnnounce` | supported; tested over moto and on a deployed stack in `ap-south-1` |
| GCP | — | — | not built | — |
| Azure | — | — | not built | — |

Every announcer, its constructor and where the question lands: [Announcers](announcers.md).
Your own, in one method: [Writing an announcer](writing-an-announcer.md). A Redis key, a
Postgres row, a Slack message — none of these needs a provider extra; a `BaseAnnounce`
subclass in your own code is the whole integration.

## Version and support policy

- Each framework extra pins one major version of its framework. A new major of the
  framework is a new pin here, after its behaviour is re-verified by the tests above.
- Python `>=3.12`.
- The pre-0.5 distributions `langgraph-wait` and `agent-wait-aws` are frozen at 0.4.1 and
  are not updated; their content is `agent-wait[langgraph]` and `agent-wait[aws]`.
