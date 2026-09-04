"""colca: speak to a Colca node, or be one (SDK design 2026-09-02, §3.3).

``Client`` is a person, or a PAT keyed on one -- ergonomic, authenticated
access to the editor API: read the tree, run commands (rename, move,
annotate, acknowledge an alarm), and watch live values. ``Service`` and
``Node`` publish data: a ``Service`` on the local or external door, a
``Node`` an embedded colcad.
"""

from .client import Client
from .errors import CommandRejected, Forbidden, NotANode, NotFound, NotLoggedIn, ChaskiError
from .handles import Alarm, Annotation, Element, Sample, Samples, Signal, Watch
from .node import Node
from .profiles import Profile, ProfileStore
from .service import LocalDoor, NotEnrolled, Service

__all__ = [
    "Client",
    "Service",
    "Node",
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
]
