"""chaski: publish to a Colca node, process its streams, or be one.

``Service`` publishes data on a node's local door, or on its published door
with an enrolled key. ``ConnectorService`` is a ``Service`` that polls a source
through a driver. ``Node`` runs an embedded colcad and hands out services on its
local door. ``DataOpsService`` is a ``Service`` that runs producers
(``chaski.dataops``); it is resolved lazily because it needs the
``chaski[dataops]`` extra. ``CommandSender`` sends commands and waits for
their ``_Ack`` on any franzmq session.
"""

from typing import Any

from colca_data_contracts.payload import DataTag

from .clock import Clock, ClockNotReady, ClockStatus
from .command import CommandSender, SentCommand, is_progress
from .connector import (
    ConnectorService,
    Discovery,
    Driver,
    Reading,
    SourceDisconnectedError,
    Target,
    Telemetry,
)
from .door import Door, Gap, KvEntry, Page, Record, Stream, StreamGapError
from .doorbell import Doorbell
from .failures import HandlerHealth, Reject
from .node import Node
from .retained_view import ViewScope
from .service import LocalDoor, NotEnrolled, NotSent, Service, write_deadline
from .topic_wakeup import TopicWakeup

__all__ = [
    "Clock",
    "ClockNotReady",
    "ClockStatus",
    "CommandSender",
    "ConnectorService",
    "DataOpsService",
    "DataTag",
    "Discovery",
    "Door",
    "Doorbell",
    "Driver",
    "Gap",
    "HandlerHealth",
    "KvEntry",
    "LocalDoor",
    "Node",
    "NotEnrolled",
    "NotSent",
    "Page",
    "Reading",
    "Record",
    "Reject",
    "SentCommand",
    "Service",
    "SourceDisconnectedError",
    "Stream",
    "StreamGapError",
    "Target",
    "Telemetry",
    "TopicWakeup",
    "ViewScope",
    "is_progress",
    "write_deadline",
]


def __getattr__(name: str) -> Any:
    # Imported on first use, so `import chaski` does not pull in pandas, and a
    # missing extra raises an ImportError that names it.
    if name == "DataOpsService":
        try:
            from .dataops import DataOpsService
        except ImportError as exc:
            raise ImportError('chaski.DataOpsService needs the dataops extra: pip install "chaski[dataops]"') from exc
        return DataOpsService
    raise AttributeError(f"module 'chaski' has no attribute {name!r}")
