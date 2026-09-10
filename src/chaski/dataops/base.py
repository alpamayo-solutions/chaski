"""Producer base class, auto-discovery, and the runtime a producer runs in.

Any concrete subclass of :class:`Producer` is recorded on class definition
(``__init_subclass__``) so :meth:`chaski.dataops.DataOpsService.discover`
can find what a module defined; which producers a service actually RUNS is
that service's own state (``DataOpsService.add`` / ``discover``), never this
module-level record.

Abstract intermediate classes (e.g. contract/interface ABCs that concrete
producers subclass) are NOT recorded — they carry abstract methods, so
``__abstractmethods__`` is non-empty and the class is skipped.

**Where the wiring lives.** A producer never opens a door, a buffer or a
database itself (evaluator design §3, §5). It is *attached* to a
:class:`Runtime` — the :class:`~chaski.dataops.DataOpsService` that
instantiated it, or a small stand-in in a test — and every declared input,
output and its own watermark reach the door, the buffer and the optional
historian through that one reference (``self.runtime``). This used to be
three module globals in the shipped ``dataops`` service
(``inputs.bind(door, buffer)`` / ``Producer.bind_buffer(buffer)``), correct
for one container and wrong for an SDK where two services may share a
process (service families design §3.5). The runtime is instance
state now, and nothing here is process-global except the discovery record.
"""

from __future__ import annotations

import logging
import threading
from abc import ABC
from typing import TYPE_CHECKING, ClassVar, Optional, Protocol, runtime_checkable

if TYPE_CHECKING:
    from chaski.door import Door

    from .buffer import Buffer
    from .inputs import Historian

log = logging.getLogger("chaski.dataops")


@runtime_checkable
class Runtime(Protocol):
    """What a producer's inputs, outputs and watermark need from whoever
    runs it: the node's door, the one local :class:`~chaski.dataops.Buffer`,
    and an optional read-only :class:`~chaski.dataops.Historian` (evaluator
    design §7 — absent by default, and never written).

    :class:`~chaski.dataops.DataOpsService` is the runtime a deployed
    producer runs in; the shipped ``dataops`` backfill CLI builds a
    door-only one; a level-2 test builds one from a fake door and a
    tmp-file buffer.
    """

    @property
    def door(self) -> "Door": ...

    @property
    def buffer(self) -> "Buffer": ...

    @property
    def historian(self) -> "Optional[Historian]": ...


class Producer(ABC):
    """Base class for all DataOps producers.

    A concrete producer:

    * sets ``name`` (stable identifier for logging / registry lookup)
    * sets ``system_element_name`` (the element its inputs resolve against
      by default)
    * declares at least one method decorated with ``@every`` / ``@cron`` /
      ``@on_metric`` (:mod:`chaski.dataops.triggers`)
    * may declare ``async def setup(self)`` for state initialisation

    Discovery happens via ``__init_subclass__``. Only concrete classes
    (``__abstractmethods__`` empty AND ``system_element_name`` set) end up in
    the record :meth:`all` returns.

    **Watermark persistence.** ``self.watermark`` / ``self.advance_watermark(t)``
    are backed by the runtime's :class:`~chaski.dataops.Buffer` (design §3,
    §10) — the framework owns cursor-resume state so a producer never
    hand-rolls its own (e.g. no bespoke "last processed timestamp" row in a
    private table). :meth:`attach` is called once by the service that
    instantiates the producer, before any producer reads or advances its
    watermark.
    """

    # Set by the user on concrete subclasses
    name: ClassVar[str]
    system_element_name: ClassVar[str | None] = None

    # Optional: the decouple-config env var names this producer's own
    # calculation logic depends on (design §10 — a canonical dump of these
    # values is folded into `chaski.dataops.codehash.compute_code_hash`, so a
    # threshold change via env var triggers hash-triggered replay exactly
    # like a source change does). Empty by default — most producers declare
    # none.
    config_keys: ClassVar[tuple[str, ...]] = ()

    # Populated by __init_subclass__
    _triggers: ClassVar[list]  # actual type: list[TriggerSpec], avoiding circular import

    # Record of concrete producers defined so far, keyed by ``name`` — the
    # discovery convenience `DataOpsService.discover` diffs. Not a
    # service's run list.
    _registry: ClassVar[dict[str, type["Producer"]]] = {}

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)

        # Gather trigger specs from methods that carry the marker attribute
        cls._triggers = []
        for attr_name in dir(cls):
            attr = getattr(cls, attr_name, None)
            triggers = getattr(attr, "__colca_triggers__", None)
            if triggers:
                for spec in triggers:
                    cls._triggers.append((attr_name, spec))

        # Skip intermediate / abstract classes — they're contracts, not runnables.
        # `__abstractmethods__` may not be populated yet during __init_subclass__
        # (ABCMeta sets it later); default to empty if absent.
        abstract = getattr(cls, "__abstractmethods__", frozenset())
        if abstract:
            log.debug("Skipping abstract producer %s (abstract methods: %s)",
                      cls.__name__, sorted(abstract))
            return

        if not getattr(cls, "system_element_name", None):
            log.debug("Skipping producer %s — no system_element_name set", cls.__name__)
            return

        if not getattr(cls, "name", None):
            log.warning("Producer %s has no `name` set — skipping registration", cls.__name__)
            return

        if not cls._triggers:
            log.warning("Producer %s has no @trigger.* methods — won't fire", cls.name)
            # still register so the user sees it in the listing
        else:
            log.debug("Registered producer %s with %d trigger(s)", cls.name, len(cls._triggers))

        if cls.name in Producer._registry:
            log.warning("Duplicate producer name %r — overwriting registration", cls.name)
        Producer._registry[cls.name] = cls

    def __new__(cls, *args, **kwargs) -> "Producer":
        # A producer's `@on_metric` handlers run on the ingest worker
        # thread; its `@every`/`@cron` ticks run in APScheduler's own
        # executor thread pool (design §4: "handlers and ticks on one
        # producer serialize on the producer's own lock"). This is the lock
        # that promise depends on — taken by the dispatch/tick wrappers in
        # `chaski.dataops.service` (`make_handler`, `off_loop`) around every
        # call into producer code, never by producer code itself.
        #
        # It is attached in `__new__`, not `__init__`, so that it exists on
        # EVERY instance regardless of what the subclass does: a producer that
        # overrides `__init__` for its own state (the level-4 `OvenWatch`
        # fixture does, and so will user code) and never calls
        # `super().__init__()` would otherwise lose the lock and fail on its
        # first dispatched metric. The runtime slot lives here for the same
        # reason.
        self = super().__new__(cls)
        self._lock = threading.RLock()
        self._runtime: Runtime | None = None
        return self

    # ------------------------------------------------------------------ lifecycle

    async def setup(self) -> None:
        """Override to load initial state (e.g. cursor from DB). Default no-op."""

    async def teardown(self) -> None:
        """Override for shutdown cleanup. Default no-op."""

    # ------------------------------------------------------------------ classmethods

    @classmethod
    def all(cls) -> list[type["Producer"]]:
        """Every concrete producer defined so far, sorted by name."""
        return [Producer._registry[n] for n in sorted(Producer._registry)]

    # ------------------------------------------------------------------ runtime

    def attach(self, runtime: Runtime) -> "Producer":
        """Bind this instance to the runtime its inputs, outputs and
        watermark go through. Called once by the service that instantiated
        it (``DataOpsService``), before ``setup()``; a test attaches a
        stand-in the same way. Returns ``self``."""
        self._runtime = runtime
        return self

    @property
    def runtime(self) -> Runtime:
        """The runtime this producer is attached to — raises if none is,
        naming the fix, rather than failing two modules away on a
        ``None`` door."""
        runtime = self._runtime
        if runtime is None:
            raise RuntimeError(
                f"Producer {type(self).__name__} is not attached to a runtime — "
                "DataOpsService attaches every producer it instantiates; a test "
                "attaches one with producer.attach(runtime)"
            )
        return runtime

    # ------------------------------------------------------------------ watermark

    @property
    def watermark(self) -> float | None:
        """This producer's last-processed position, or ``None`` if never set.

        Persisted in the runtime's :class:`~chaski.dataops.Buffer` — reading
        it never touches this producer's own instance state, so it survives
        a service restart the same way the buffer itself does.
        """
        return self.runtime.buffer.watermark(self.name)

    def advance_watermark(self, position: float) -> None:
        """Persist this producer's progress at ``position``.

        Preserves whatever code hash the buffer already has on file for
        this producer (empty string if none has ever been recorded) —
        advancing the watermark is not the act that decides whether the
        producer's code changed; that is the hash-triggered replay check
        (design §10), which calls ``Buffer.set_watermark`` directly with
        the freshly computed hash.
        """
        buffer = self.runtime.buffer
        existing_hash = buffer.code_hash(self.name) or ""
        buffer.set_watermark(self.name, position, existing_hash)
