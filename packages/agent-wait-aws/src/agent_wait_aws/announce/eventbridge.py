"""`EventBridgeAnnounce` -- `PutEvents` onto a bus, `DetailType = wait.<transition>`.

The most useful of the four for a large organisation: rules on the bus can fan a
`wait.created` out to Slack, a ticketing system and an audit log without the agent, or
this library, knowing that any of them exist.
"""

from __future__ import annotations

import json
from typing import Any

import boto3
from agent_wait.announce import BaseAnnounce
from agent_wait.model import Transition, WaitEnvelope


class EventBridgeAnnounce(BaseAnnounce):
    name = "eventbridge"

    def __init__(
        self,
        bus_name: str,
        *,
        source: str = "agent-wait",
        client: Any = None,
        region_name: str | None = None,
        only: tuple[Transition, ...] | None = None,
    ) -> None:
        super().__init__(only=only)
        self.bus_name = bus_name
        self.source = source
        self._client = client or boto3.client("events", region_name=region_name)

    def deliver(self, envelope: WaitEnvelope, transition: Transition) -> None:
        response = self._client.put_events(
            Entries=[
                {
                    "EventBusName": self.bus_name,
                    "Source": self.source,
                    "DetailType": envelope.type,
                    "Detail": envelope.to_json(),
                }
            ]
        )
        # PutEvents reports per-entry failures in the body with a 200 status, so a naive
        # call looks successful even when nothing was delivered.
        if response.get("FailedEntryCount"):
            self._log.error(
                "EventBridge rejected the envelope for interrupt %s: %s",
                envelope.interrupt_id,
                json.dumps(response.get("Entries", []), default=str),
            )
