"""Producer base class, auto-discovery, and the runtime a producer runs in.

Every concrete subclass of :class:`Producer` is recorded when it is defined, so
:meth:`chaski.dataops.DataOpsService.discover` can find what a module defined.
Which producers a service runs is the service's own state. Abstract classes
are not recorded.

A producer never opens a door, a buffer or a database itself. It is attached to
a :class:`Runtime`, usually the :class:`~chaski.dataops.DataOpsService` that
created it, and its inputs, outputs and watermark go through ``self.runtime``.
Nothing here is process-global except the discovery record.
"""

from __future__ import annotations

import logging
import threading
from abc import ABC
from typing import TYPE_CHECKING, ClassVar, Protocol, runtime_checkable

if TYPE_CHECKING:
    from chaski.door import Door

    from .buffer import Buffer
    from .inputs import Historian

log = logging.getLogger("chaski.dataops")


@runtime_checkable
class Runtime(Protocol):
    """What a producer's inputs, outputs and watermark need from whoever runs
    it: the node's door, the local :class:`~chaski.dataops.Buffer`, and an
    optional read-only :class:`~chaski.dataops.Historian`.

    :class:`~chaski.dataops.DataOpsService` is the usual runtime; a test can
    build one from a fake door and a temporary buffer.
    """

    @property
    def door(self) -> Door: ...

    @property
    def buffer(self) -> Buffer: ...

    @property
    def historian(self) -> Historian | None: ...


class Producer(ABC):
    """Base class for all DataOps producers.

    A concrete producer:

    * sets ``name`` (stable identifier for logging / registry lookup)
    * sets ``system_element_name`` (the element its inputs resolve against
      by default)
    * declares at least one method decorated with ``@every`` / ``@cron`` /
      ``@on_metric`` (:mod:`chaski.dataops.triggers`)
    * may declare ``async def setup(self)`` for state initialisation

    Discovery happens in ``__init_subclass__``. Only concrete classes (no
    abstract methods, ``system_element_name`` set) are recorded; see :meth:`all`.

    **Watermark.** ``self.watermark`` and ``self.advance_watermark(t)`` are
    stored in the runtime's :class:`~chaski.dataops.Buffer`, so a producer
    needs no progress table of its own. The service calls :meth:`attach`
    before the producer touches its watermark.
    """

    # Set by the user on concrete subclasses
    name: ClassVar[str]
    system_element_name: ClassVar[str | None] = None

    # Environment variables the producer's calculation depends on. Their
    # values are part of the code hash, so changing one triggers a replay like
    # a code change does.
    config_keys: ClassVar[tuple[str, ...]] = ()

    # Populated by __init_subclass__
    _triggers: ClassVar[list]  # actual type: list[TriggerSpec], avoiding circular import

    # Record of concrete producers defined so far, keyed by ``name`` — the
    # discovery convenience `DataOpsService.discover` diffs. Not a
    # service's run list.
    _registry: ClassVar[dict[str, type[Producer]]] = {}

    # Set on every instance in __new__.
    _lock: threading.RLock
    _runtime: Runtime | None

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
        abstract: frozenset[str] = getattr(cls, "__abstractmethods__", frozenset())
        if abstract:
            log.debug("Skipping abstract producer %s (abstract methods: %s)", cls.__name__, sorted(abstract))
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

    def __new__(cls, *args, **kwargs) -> Producer:
        # Handlers run on the ingest thread and ticks in APScheduler's pool; the
        # service's wrappers take this lock around every call, so a producer's
        # handlers and ticks never overlap. Set in __new__, like the runtime
        # slot, so a subclass that overrides __init__ without super() has both.
        self = super().__new__(cls)
        self._lock = threading.RLock()
        self._runtime = None
        return self

    # ------------------------------------------------------------------ lifecycle

    async def setup(self) -> None:  # noqa: B027 - optional hook
        """Override to load initial state (e.g. cursor from DB). Default no-op.

        Runs before this producer's ``SignalOutput``s are bound
        (``DataOpsService.serve``'s own startup order) — a ``publish()``
        called from here raises. Use :meth:`on_ready` for startup work that
        needs to publish.
        """

    async def on_ready(self) -> None:  # noqa: B027 - optional hook
        """Override for startup work that needs outputs already bound.
        Default no-op.

        Called once per run, after every producer's outputs are catalogued
        and bound and before any trigger — ``@every``/``@cron``,
        ``@on_metric``, ``@on_constant``, ``@on_signal`` — can fire. A
        producer that wants to compute and publish once at startup does it
        here instead of retrying ``publish()`` on the ``RuntimeError`` it
        raises before binding, or racing its own first trigger.

        An exception here is logged and does not stop this producer's
        triggers from being wired; a producer whose startup compute may fail
        should catch what it expects to fail and complain through its own
        outputs, the way a trigger handler would.
        """

    async def teardown(self) -> None:  # noqa: B027 - optional hook
        """Override for shutdown cleanup. Default no-op."""

    # ------------------------------------------------------------------ classmethods

    @classmethod
    def all(cls) -> list[type[Producer]]:
        """Every concrete producer defined so far, sorted by name."""
        return [Producer._registry[n] for n in sorted(Producer._registry)]

    # ------------------------------------------------------------------ runtime

    def attach(self, runtime: Runtime) -> Producer:
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

        Keeps the code hash already on file; only the replay check at startup
        records a new one.
        """
        buffer = self.runtime.buffer
        existing_hash = buffer.code_hash(self.name) or ""
        buffer.set_watermark(self.name, position, existing_hash)
