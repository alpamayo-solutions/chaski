"""colca: speak to a Colca node, or be one (SDK design 2026-09-02, §3.3).

``Client`` is a person, or a PAT keyed on one -- ergonomic, authenticated
access to the editor API: read the tree, run commands (rename, move,
annotate, acknowledge an alarm), and watch live values. ``Service`` and
``Node`` publish data: a ``Service`` on the local or external door, a
``Node`` an embedded colcad. ``DataOpsService`` is a ``Service`` that runs
producers (``chaski.dataops``) -- resolved lazily because it needs the
``chaski[dataops]`` extra.
"""

from typing import Any

from .client import Client
from .door import Door, Gap, KvEntry, Page, Record, Stream
from .errors import CommandRejected, Forbidden, NotANode, NotFound, NotLoggedIn, ChaskiError
from .handles import Alarm, Annotation, Element, Sample, Samples, Signal, Watch
from .node import Node
from .profiles import Profile, ProfileStore
from .service import LocalDoor, NotEnrolled, Service

__all__ = [
    "Client",
    "Service",
    "Node",
    "Door",
    "Stream",
    "Record",
    "Page",
    "Gap",
    "KvEntry",
    "Profile",
    "ProfileStore",
    "Signal",
    "Element",
    "Alarm",
    "Annotation",
    "Sample",
    "Samples",
    "Watch",
    "ChaskiError",
    "NotLoggedIn",
    "Forbidden",
    "NotFound",
    "NotANode",
    "CommandRejected",
    "LocalDoor",
    "NotEnrolled",
    "DataOpsService",
]


def __getattr__(name: str) -> Any:
    # `chaski.DataOpsService` without importing pandas/APScheduler into every
    # `import colca` — a process that only wants Client or Service pays
    # nothing, and one without the extra gets an ImportError that names it.
    if name == "DataOpsService":
        try:
            from .dataops import DataOpsService
        except ImportError as exc:
            raise ImportError(
                "chaski.DataOpsService needs the dataops extra: pip install \"chaski[dataops]\""
            ) from exc
        return DataOpsService
    raise AttributeError(f"module 'colca' has no attribute {name!r}")
