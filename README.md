# agent-wait

Durable waits for agent frameworks. Turns a framework interrupt into a parked, addressable, expirable wait that survives the process exiting and can be answered days later from anywhere.

Public surface: `ask()` · `WaitRuntime.dispatch()` · `WaitRuntime.register()` · the `AnnounceAdapter` protocol · two JSON message formats.

Packages: `agent-wait` (core) · `langgraph-wait` (LangGraph adapter) · `agent-wait-aws` (DynamoDB, SQS, SNS, EventBridge, Scheduler, Lambda wrapper, CDK).

Start with `REQUIREMENTS.md`.
