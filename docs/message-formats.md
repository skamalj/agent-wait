# Message formats

This is the contract. Two JSON documents: one agent-wait sends, one it receives. A team
that has read only this file should be able to write a working consumer.

Everything else in this repository is an implementation detail. These are not.

---

## 1. The wait envelope (outbound)

Emitted by every announce adapter, on every transition. This is what arrives on your SNS
topic, your SQS queue or your EventBridge bus.

```json
{
  "type": "wait.created",
  "event_id": "01JAV3K2R8Q9WZ7M4X1YB6N0PD",
  "wait_id": "01JAV3K2R7C5H8F2G9J4K1L3M6",
  "thread_id": "order-4471",
  "question": {
    "kind": "refund_approval",
    "order_id": "order-4471",
    "amount": 41000,
    "customer": "customer-of-order-4471"
  },
  "allowed_actions": ["approve", "reject"],
  "expires_at": "2026-09-12T09:00:00Z",
  "token": "aw1.k1.01JAV3K2R7C5H8F2G9J4K1L3M6.1789344000.9f2a1c4e7b0d3856.approve+reject.KpQ3rZ1xN8vL0aTfB6cWdYs",
  "reply_to": { "kind": "sqs", "url": "https://sqs.ap-south-1.amazonaws.com/…/agent-runs.fifo" },
  "correlation": null,
  "tags": { "approver_group": "finance" },
  "transition_detail": {}
}
```

| Field | Meaning |
|---|---|
| `type` | `wait.created`, `wait.answered`, `wait.expired`, `wait.resumed`, `wait.cancelled` |
| `event_id` | ULID, unique per transition. **Dedupe on this.** |
| `wait_id` | ULID identifying the wait itself. Stable across its whole life. |
| `thread_id` | The agent's conversation identity. |
| `question` | Whatever the agent asked. Opaque to every adapter; only your UI interprets it. |
| `allowed_actions` | The only actions that will be accepted. Anything else is refused. |
| `expires_at` | RFC 3339 UTC, or `null` when the wait has no timeout. |
| `token` | The credential for answering. See below. |
| `reply_to` | Where to send the answer. The agent's own entry point. |
| `correlation` | `{"provider": …, "id": …}` when the wait is tied to an external job; otherwise `null`. |
| `tags` | Routing hints from the policy. Become SNS message attributes, so filter policies can use them. |
| `transition_detail` | Empty on `created`. On `answered`/`expired`: `action`, `actor`, `answered_at`. On `cancelled`: `reason`. |

### What a consumer should do with it

1. **Dedupe on `event_id`.** Announce delivery is at-least-once, and the sweeper
   deliberately re-announces a wait it thinks was never heard.
2. **Render `question` and offer exactly `allowed_actions`.** Do not invent a third
   button; it will be refused.
3. **Send the answer to `reply_to`.** Do not hardcode an endpoint — `reply_to` is how the
   agent tells you where it listens, and it can differ per environment.
4. **Treat `wait.answered` / `wait.expired` / `wait.cancelled` as "close the ticket".**
   They may arrive because *somebody else* answered, or because the timeout fired.

---

## 2. The answer envelope (inbound)

Send this to `reply_to`. For an SQS FIFO entry point, use `MessageGroupId = thread_id`.

```json
{
  "token": "aw1.k1.01JAV3K2R7C5H8F2G9J4K1L3M6.1789344000.9f2a1c4e7b0d3856.approve+reject.KpQ3rZ1xN8vL0aTfB6cWdYs",
  "action": "approve",
  "payload": { "note": "within budget" },
  "actor": "priya@corp",
  "answer_id": "click-9f1"
}
```

| Field | Required | Meaning |
|---|---|---|
| `token` | yes | Copied verbatim from the envelope. Identifies and authorises. |
| `action` | no (default `resume`) | Must be in `allowed_actions`. `timeout` is reserved for the scheduler. |
| `payload` | no | Merged with `action` to form the value the agent's `ask()` returns. |
| `actor` | no | **Informational only.** It grants nothing — see below. |
| `answer_id` | **yes** | Your idempotency id. |

### `answer_id` is the one that catches people out

It is required, and it must be stable for a given user intent. Send the same
`answer_id` twice and the second is a `duplicate` — success, nothing happens twice. Send
a *different* one after the wait is settled and you get `already_answered` — a conflict,
because a second person is trying to make a different decision.

Use something derived from the interaction: the button-click id, the Slack message ts,
`sha256(user + wait_id + action)`. Do **not** use a fresh UUID per HTTP retry, or a retry
will read as a second person disagreeing.

### `actor` is not authentication

`actor` is a label for the audit trail. Anyone who holds the token can put any string
there. Authentication is your entry point's job — the queue policy, the API Gateway
authoriser, the IAM role that is allowed to `SendMessage`. agent-wait deliberately does
not pretend otherwise.

---

## 3. The start message (inbound)

The ordinary "please run" message, unchanged from whatever you had before.

```json
{
  "thread_id": "order-4471",
  "input": { "order_id": "order-4471", "amount": 41000 },
  "message_id": "evt-return-4471"
}
```

`message_id` is optional; on SQS the `messageId` is used when it is absent. It is what
makes a redelivered start message replay from the checkpoint instead of applying the same
input twice.

**Answers and starts arrive at the same place.** `dispatch()` tells them apart by the
presence of `token`. There is no second endpoint to build or secure.

---

## 4. The token

```
aw1.<kid>.<wait_id>.<exp>.<binding16>.<actions>.<mac>
```

`mac` is `base64url(HMAC-SHA256(key[kid], everything-before-the-mac))[:27]`.

Three things are worth knowing:

* **It authorises one wait, with those actions, until that instant.** It is not a
  session, not a user, not a bearer token for anything else.
* **Its `exp` is not the wait's timeout.** Tokens live seven days by default; the wait
  might time out in two hours or never. The store decides what is final — a token can
  verify perfectly and still be refused because the wait was answered an hour ago.
* **Treat it as a secret.** Anyone holding it can answer that one question. It is safe in
  an email or a Slack message to the approver group; it is not safe in a public channel,
  a URL that gets logged, or a screenshot.

---

## 5. What you get back

`dispatch()` classifies every inbound payload. Consumers do not see these directly, but
the operator watching the logs does, and they are the vocabulary for "why did nothing
happen":

| Reason | What it means | Should you retry? |
|---|---|---|
| `duplicate` | Same `answer_id`, already applied. | No — this is success. |
| `already_answered` | A *different* answer got there first, or it timed out. | No. Tell the user. |
| `token_invalid` | Malformed, wrong key, or tampered. | No. |
| `expired` | The token's own seven days are up. | No. Ask the agent to re-announce. |
| `binding_mismatch` | The question changed since the token was minted. | No. The link is stale. |
| `action_not_allowed` | Not in `allowed_actions`. | No. |
| `not_pending` | The thread already moved past this interrupt. | No — this is success. |
| `parked` | The wait does not exist *yet*; the answer was stored and will be applied. | No. |
| `unknown_payload` | Not a start and not an answer — e.g. no `answer_id`. | No. Fix the sender. |
| `lease_held` | Another worker is running this thread right now. | **Yes.** |

`lease_held` is the only one worth retrying. Everything else is a decision, not a failure.
