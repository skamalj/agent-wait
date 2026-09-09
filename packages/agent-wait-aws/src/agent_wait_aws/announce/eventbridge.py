"""`EventBridgeAnnounce` -- `PutEvents` onto a bus, `DetailType = wait.<transition>`.

The most useful of the three for a large organisation: rules on the bus can fan a
`wait.created` out to Slack, a ticketing system and an audit log without the agent, or
this library, knowing that any of them exist.
"""

from __future__ import annotations

import json
import logging

import boto3
from agent_wait.model import Transition, WaitEnvelope

_log = logging.getLogger("agent_wait_aws.announce.eventbridge")


class EventBridgeAnnounce:
    name = "eventbridge"

    def __init__(
        self,
        bus_name: str,
        *,
        source: str = "agent-wait",
        client: object | None = None,
        region_name: str | None = None,
        only: tuple[Transition, ...] | None = None,
    ) -> None:
        self.bus_name = bus_name
        self.source = source
        self._client = client or boto3.client("events", region_name=region_name)
        self._only = only

    def supports(self, transition: Transition) -> bool:
        return self._only is None or transition in self._only

    def announce(self, envelope: WaitEnvelope, transition: Transition) -> None:
        try:
            response = self._client.put_events(  # type: ignore[attr-defined]
                Entries=[
                    {
                        "EventBusName": self.bus_name,
                        "Source": self.source,
                        "DetailType": f"wait.{transition}",
                        "Detail": envelope.to_json(),
                    }
                ]
            )
            # PutEvents reports per-entry failures in the body with a 200 status, so a
            # naive call looks successful even when nothing was delivered.
            if response.get("FailedEntryCount"):
                _log.error(
                    "EventBridge rejected the envelope for wait %s: %s",
                    envelope.wait_id,
                    json.dumps(response.get("Entries", []), default=str),
                )
        except Exception:
            _log.exception("EventBridgeAnnounce failed for wait %s (%s)", envelope.wait_id, transition)
