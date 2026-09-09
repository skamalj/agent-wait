# Integrating a consumer

You have been handed an SNS topic (or an EventBridge bus, or a queue) that emits wait
envelopes, and you need to build the thing a human actually clicks. This is everything you
need. You do not need to install agent-wait, read its source, or know what LangGraph is.

There are exactly two moves: **listen**, and **reply**.

---

## 1. Listen

Subscribe to the topic. You will receive [wait envelopes](message-formats.md#1-the-wait-envelope-outbound).

```python
def on_envelope(envelope: dict) -> None:
    if already_seen(envelope["event_id"]):  # at-least-once delivery
        return
    mark_seen(envelope["event_id"])

    match envelope["type"]:
        case "wait.created":
            open_approval(envelope)
        case "wait.answered" | "wait.expired" | "wait.cancelled":
            close_approval(envelope["wait_id"], envelope["transition_detail"])
        case "wait.resumed":
            pass  # bookkeeping; the agent moved on
```

When you open an approval, store three things: `wait_id`, `token`, and `expires_at`.
Render `question` however suits you, and offer **exactly** the buttons in
`allowed_actions` — any other action is refused.

> **Close the ticket on the terminal events too.** `wait.answered` may arrive because a
> colleague approved it in a different channel, and `wait.expired` because the timeout
> fired. If you only ever close on your own button, your queue fills with ghosts.

---

## 2. Reply

Send an [answer envelope](message-formats.md#2-the-answer-envelope-inbound) to
`reply_to`. That is the agent's own entry point — the same place its ordinary work
arrives. Do not hardcode it.

```python
import json, boto3


def approve(record, user: str, note: str) -> None:
    body = {
        "token": record.token,
        "action": "approve",
        "payload": {"note": note},
        "actor": user,
        # Stable per click. Not a fresh uuid per HTTP retry.
        "answer_id": f"{record.wait_id}:{user}:approve",
    }
    boto3.client("sqs").send_message(
        QueueUrl=record.reply_to["url"],
        MessageBody=json.dumps(body),
        MessageGroupId=record.thread_id,  # FIFO: one in-flight run per thread
        MessageDeduplicationId=body["answer_id"],
    )
```

That is the integration. There is no acknowledgement to wait for and no second call to
make.

---

## 3. The four things that actually go wrong

### `answer_id` generated per retry

This is the one. `answer_id` is how the agent tells "the same person clicked twice" from
"a second person disagrees". Derive it from the *intent* — the click, the Slack message
ts, `sha256(user + wait_id + action)`. If you generate a UUID inside a retry loop, your
own retry looks like a colleague overruling the decision, and you get
`already_answered` instead of a clean `duplicate`.

### Assuming a reply means the agent finished

It does not. You are handing the answer to a durable queue; the agent may take a minute,
or may be resumed on a machine that is not yet running. You will hear about it through
`wait.answered` and then `wait.resumed`. Do not block a web request on it.

### Treating a duplicate as an error

If the user double-clicks, the second answer is a `duplicate` — that is *success*. Show
them the same confirmation. If your UI shows an error, users learn to click again.

### Building a second endpoint

You may be tempted to ask for "an API to answer waits". There isn't one, on purpose. The
world answers where the agent already listens, so there is one thing to secure, one thing
to monitor, and one ordering guarantee. `reply_to` tells you where that is.

---

## 4. Routing without the agent knowing about you

The wait's policy can carry `tags`. They become SNS message attributes, so a subscription
filter policy can route without a line of code:

```json
{ "approver_group": ["finance"] }
```

The agent said `tags={"approver_group": "finance"}` at the interrupt site. It has no idea
your service exists, and never needs to.

---

## 5. Expiry, and what to show the user

`expires_at` is when the agent will give up and apply its declared default — often
"reject". Two consequences for your UI:

* Show a countdown. "Decide by Friday 09:00, or this is automatically rejected" is a very
  different message from "pending".
* When the deadline passes, expect `wait.expired` and close the item. Do not let a user
  click Approve on something the agent has already moved past — they will get
  `already_answered`, and they will be annoyed.

The `token` has its own separate seven-day life. A token can be perfectly valid for a
wait that was settled an hour ago; the store is what decides, not the token.

---

## 6. Checklist

- [ ] Dedupe on `event_id`
- [ ] Store `wait_id`, `token`, `expires_at`
- [ ] Offer only `allowed_actions`
- [ ] Send to `reply_to`, never a hardcoded endpoint
- [ ] `answer_id` stable per user intent
- [ ] Treat `duplicate` as success
- [ ] Close on `wait.answered` / `wait.expired` / `wait.cancelled`, not just your own click
- [ ] Never log the `token`
