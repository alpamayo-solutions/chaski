"""``chaski.dataops``: computed signals and annotations as a service family.

The evaluator runtime as a library, so any process can run producers:

    from chaski.dataops import DataOpsService, Producer, SignalRangeInput, SignalOutput, every

Installs with the ``dataops`` extra (``pip install "chaski[dataops]"``) —
APScheduler, pandas and python-decouple are what this package needs beyond
the base SDK; it needs no database driver (the historian is a port, see
:class:`Historian`). ``DataOpsService`` is a :class:`chaski.Service`: the
node sees one more local service, not a kind of its own.
"""

from chaski.failures import Reject

from .backfill import Backfill
from .base import Producer, Runtime, save_checkpoint
from .buffer import Buffer
from .commands import Command, CommandRejected, CommandResult
from .ingest import Ingest
from .inputs import Historian, SignalRangeInput, WindowExceedsRetentionError, validate_windows
from .outputs import AnnotationOutput, SignalOutput, bind_annotation_outputs, build_catalogue
from .service import DataOpsService, build_dispatch, import_directory, import_package
from .triggers import cron, every, on_command, on_constant, on_metric, on_signal, parse_duration

__all__ = [
    "AnnotationOutput",
    "Backfill",
    "Buffer",
    "Command",
    "CommandRejected",
    "CommandResult",
    "DataOpsService",
    "Historian",
    "Ingest",
    "Producer",
    "Reject",
    "Runtime",
    "SignalOutput",
    "SignalRangeInput",
    "WindowExceedsRetentionError",
    "bind_annotation_outputs",
    "build_catalogue",
    "build_dispatch",
    "cron",
    "every",
    "import_directory",
    "import_package",
    "on_command",
    "on_constant",
    "on_metric",
    "on_signal",
    "parse_duration",
    "save_checkpoint",
    "validate_windows",
]
