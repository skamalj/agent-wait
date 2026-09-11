# Changelog

## 0.2.0 — 2026-09-11

First public release.

**The library publishes interrupts and stops.** One call — `WaitPublisher.invoke()` —
runs the graph and publishes an envelope for each question it parked on, by diffing the
framework's own pending set before and after. `pending()` reads that set; `republish()`
re-announces it. `ask()` declares a question and its advisory policy at the interrupt
site. There is no store, no inbound handling, no credential, no timer.

**Announcers.** `BaseAnnounce` implements the never-raise contract once; subclasses write
`deliver()`. Shipped: `WebhookAnnounce` (signed, stdlib), `LogAnnounce`,
`InMemoryAnnounce`, and in `agent-wait-aws`: `SnsAnnounce`, `SqsAnnounce`,
`EventBridgeAnnounce`, `DynamoDbAnnounce`.

**Verified against langgraph 1.2.11**, with the observations recorded in
`reports/langgraph-spike-observations.txt` and pinned by tests: interrupt ids are stable
across re-entry; `tasks[*].interrupts` over-reports after a partial parallel resume
(`task.result` is the discriminator); two interrupting tools in one `ToolNode` share an id.

### Coming from 0.1

0.1 (tagged, never published) owned the whole round trip — tokens, a wait store, leases,
a scheduler-driven timeout, an inbound `dispatch()`. All of it was removed. See
[Migrating from 0.1](https://skamalj.github.io/agent-wait/migrating-from-0.1/) for what
moved to the caller and why.
