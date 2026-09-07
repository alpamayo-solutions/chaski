"""``chaski.dataops``: computed signals and annotations as a service family.

The evaluator runtime of the dataops evaluator design,
lifted out of the shipped ``dataops`` service into the SDK (service families
design 2026-09-07 §3.5, D9) so any process can run producers:

    from chaski.dataops import DataOpsService, Producer, SignalRangeInput, SignalOutput, every

Installs with the ``dataops`` extra (``pip install "chaski[dataops]"``) —
APScheduler, pandas and python-decouple are what this package needs beyond
the base SDK; it needs no database driver (the historian is a port, see
:class:`Historian`). ``DataOpsService`` is a :class:`chaski.Service`: the
node sees one more local service, not a kind of its own.
"""

from .base import Producer, Runtime
from .buffer import Buffer
from .ingest import Ingest
from .inputs import Historian, SignalRangeInput, WindowExceedsRetentionError, validate_windows
from .outputs import AnnotationOutput, SignalOutput, bind_annotation_outputs, build_catalogue
from .service import DataOpsService, build_dispatch, import_directory, import_package
from .triggers import cron, every, on_metric, parse_duration

__all__ = [
    "DataOpsService",
    "Producer",
    "Runtime",
    "SignalRangeInput",
    "SignalOutput",
    "AnnotationOutput",
    "Historian",
    "Buffer",
    "Ingest",
    "WindowExceedsRetentionError",
    "validate_windows",
    "build_catalogue",
    "bind_annotation_outputs",
    "build_dispatch",
    "import_package",
    "import_directory",
    "every",
    "cron",
    "on_metric",
    "parse_duration",
]
