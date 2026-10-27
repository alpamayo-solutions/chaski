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

import asyncio
import json
import logging
import math
import threading
import time
from abc import ABC
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, runtime_checkable

from chaski.failures import Reject

if TYPE_CHECKING:
    from chaski.door import Door

    from .backfill import Backfill
    from .buffer import Buffer
    from .inputs import Historian

log = logging.getLogger("chaski.dataops")

# The instant an @on_metric handler or a backfilled tick is evaluated at, and
# the task it was set in (None: set outside a loop).
_event_time: ContextVar[tuple[float, asyncio.Task | None] | None] = ContextVar("chaski_event_time", default=None)


def _current_task() -> asyncio.Task | None:
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


@contextmanager
def event_time(timestamp: float) -> Iterator[None]:
    """Evaluate one callback at its event's time: an ``@on_metric`` handler at
    its sample's timestamp, a backfilled tick at its due time.

    Context-local: other threads and tasks keep the wall clock. Entered in a
    task, it holds for that task only; a task it starts (a trailing run, a
    retry) runs later, on the wall clock.
    """
    if not math.isfinite(timestamp):
        raise ValueError("event time must be finite")
    token = _event_time.set((timestamp, _current_task()))
    try:
        yield
    finally:
        _event_time.reset(token)


def current_time() -> float:
    """The event time of the callback being evaluated, else the wall clock."""
    instant = _event_time.get()
    if instant is not None and (instant[1] is None or instant[1] is _current_task()):
        return instant[0]
    return time.time()


@runtime_checkable
class Runtime(Protocol):
    """What a producer's inputs, outputs and watermark need from whoever runs
    it: the node's door to read, ``send``/``retract``/``command`` to write
    over MQTT, the local :class:`~chaski.dataops.Buffer`, and an optional read-only
    :class:`~chaski.dataops.Historian`.

    :class:`~chaski.dataops.DataOpsService` is the usual runtime; a test can
    build one from a fake door and a temporary buffer.
    """

    @property
    def door(self) -> Door: ...

    def send(self, topic: str, payload: str, *, retain: bool = False) -> None: ...

    def retract(self, topic: str) -> None: ...

    def command(
        self,
        contract: str,
        path: str,
        fields: dict[str, Any] | None = None,
        *,
        lifetime: float | None,
        node: str | None = None,
        timeout: float = 30.0,
    ) -> dict[str, Any]: ...

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

    # Opt in to process history older than live intake, once, before live
    # dispatch starts: ``backfill = Backfill(horizon="400d", window="6h")``.
    # See chaski.dataops.backfill.
    backfill: ClassVar[Backfill | None] = None

    # Opt in for state changed by on_metric handlers. Both hooks are required.
    # Timers/commands with state need their own explicit recovery transaction.
    state_version: ClassVar[int | None] = None

    # Populated by __init_subclass__
    _triggers: ClassVar[list]  # actual type: list[TriggerSpec], avoiding circular import

    # Record of concrete producers defined so far, keyed by ``name`` — the
    # discovery convenience `DataOpsService.discover` diffs. Not a
    # service's run list.
    _registry: ClassVar[dict[str, type[Producer]]] = {}

    # Set on every instance in __new__.
    _lock: threading.RLock
    _runtime: Runtime | None
    _handled_offsets: dict[str, int]
    _checkpoint_hash: str | None

    @property
    def now(self) -> float:
        """The time of the event being handled (a sample's timestamp in an
        ``@on_metric`` handler, the due time of a backfilled tick), otherwise
        the host's wall clock."""
        return current_time()

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
        self._handled_offsets = {}
        self._checkpoint_hash = None
        if cls.state_version is not None and (
            cls.snapshot_state is Producer.snapshot_state or cls.restore_state is Producer.restore_state
        ):
            raise TypeError("state_version requires snapshot_state() and restore_state()")
        return self

    # ------------------------------------------------------------------ lifecycle

    def snapshot_state(self) -> Any:
        """Return JSON state sufficient to resume every on_metric handler."""
        raise NotImplementedError

    def restore_state(self, state: Any) -> None:
        """Replace in-memory state from snapshot_state; must not publish effects."""
        raise NotImplementedError

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


# The checkpoint machinery is chaski's, not the producer's: module functions,
# so a producer's own method of the same name never replaces it.


def state_copy(producer: Producer) -> Any:
    """A detached copy of ``producer.snapshot_state()``."""
    return json.loads(json.dumps(producer.snapshot_state(), allow_nan=False))


def restore_checkpoint(producer: Producer) -> bool:
    """Restore the saved checkpoint of ``producer`` if its code hash and
    ``state_version`` match. Returns whether it was restored."""
    if producer.state_version is None:
        return False
    from .codehash import compute_code_hash

    producer._checkpoint_hash = compute_code_hash(type(producer))
    saved = producer.runtime.buffer.checkpoint(producer.name)
    if saved is None or saved[:2] != (producer._checkpoint_hash, producer.state_version):
        return False
    producer.restore_state(saved[2])
    producer._handled_offsets = saved[3]
    return True


class RewindFailed(Exception):
    """A checkpointed producer could not be brought back to the record
    before a failed one (:class:`PageCheckpoints`). ``record`` is the first
    record it handled in the page: the page is acknowledged only up to the
    one before it, so the producer handles the rest again."""

    def __init__(self, consumer: str, record: Any) -> None:
        super().__init__(f"{consumer}: could not keep the state of the records before offset={record.offset}")
        self.consumer = consumer
        self.record = record


@dataclass
class _PageEntry:
    """One checkpointed producer within one page."""

    #: State and handled offsets at the page's start, held while the
    #: producer's lock is.
    start_state: Any
    start_offsets: dict[str, int]
    #: The first record the producer handled in the page.
    first: Any = None
    #: What a failed or rejected record rewinds to, and the handler calls
    #: made since: ``(method, metric, key, record)``.
    base_state: Any = None
    base_offsets: dict[str, int] = field(default_factory=dict)
    since: list[tuple[Any, Any, str, Any]] = field(default_factory=list)


_page: ContextVar[tuple[PageCheckpoints, asyncio.Task | None] | None] = ContextVar("chaski_page", default=None)


class PageCheckpoints:
    """The checkpoints of the producers one ingest page reaches, saved once
    when the page ends instead of after every record.

    A checkpointed producer (``state_version``) is locked from its first
    record in the page to the page's end, so no tick or command changes its
    state between two of its records. Its state at that first record is kept;
    the checkpoint and handled offsets are saved once, in the page's commit,
    before the page is acknowledged.

    A handler that fails or rejects a record must leave the producer as it
    was before that record. The producer is put back to the kept state and
    the handler calls it made since run again; their effects repeat, as on a
    redelivered page, which handlers already tolerate. The state after a
    rejection becomes the new point to rewind to. When that fails too, or the
    checkpoint cannot be saved, the producer is put back to the page's start
    and :class:`RewindFailed` names its first record of the page.
    """

    def __init__(self) -> None:
        self._entries: dict[int, tuple[Producer, _PageEntry]] = {}
        #: The earliest checkpoint that could not be saved when the page ended.
        self.failed: RewindFailed | None = None

    @staticmethod
    def current() -> PageCheckpoints | None:
        """The page being handled in this task, if any."""
        active = _page.get()
        if active is None or active[1] is not _current_task():
            return None
        return active[0]

    @contextmanager
    def active(self) -> Iterator[PageCheckpoints]:
        """Collect this task's handler calls until the block ends, then save
        every checkpoint and release the producers (see :meth:`finish`)."""
        token = _page.set((self, _current_task()))
        try:
            yield self
        finally:
            _page.reset(token)
            self.finish()

    async def handle(self, producer: Producer, method: Any, metric: Any, record: Any) -> None:
        """Run one ``@on_metric`` call of a checkpointed producer at ``record``."""
        entry = self._enter(producer)
        key = method.__name__
        if record.offset <= producer._handled_offsets.get(key, -1):
            return
        if entry.first is None:
            entry.first = record
        try:
            with event_time(float(metric.timestamp)):
                await method(metric)
        except Exception as exc:
            await self._rewind(producer, entry)
            if isinstance(exc, Reject):
                # Set aside: the page goes on, and a later failure rewinds to here.
                entry.base_state = state_copy(producer)
                entry.base_offsets = dict(producer._handled_offsets)
                entry.since.clear()
            raise
        except BaseException:
            self._to_start(producer, entry)  # cancelled: no handler runs again
            raise
        producer._handled_offsets[key] = record.offset
        entry.since.append((method, metric, key, record))

    def _enter(self, producer: Producer) -> _PageEntry:
        held = self._entries.get(id(producer))
        if held is not None:
            return held[1]
        producer._lock.acquire()
        try:
            state = state_copy(producer)
        except BaseException:
            producer._lock.release()
            raise
        offsets = dict(producer._handled_offsets)
        entry = _PageEntry(state, offsets, base_state=state, base_offsets=dict(offsets))
        self._entries[id(producer)] = (producer, entry)
        return entry

    async def _rewind(self, producer: Producer, entry: _PageEntry) -> None:
        """Put ``producer`` back to before the record that just failed."""
        producer.restore_state(json.loads(json.dumps(entry.base_state, allow_nan=False)))
        producer._handled_offsets = dict(entry.base_offsets)
        consumer = producer.name
        try:
            for method, metric, key, record in entry.since:
                consumer = f"{producer.name}.{key}"
                with event_time(float(metric.timestamp)):
                    await method(metric)
                producer._handled_offsets[key] = record.offset
        except Exception as exc:
            self._to_start(producer, entry)
            raise RewindFailed(consumer, entry.first) from exc

    @staticmethod
    def _to_start(producer: Producer, entry: _PageEntry) -> None:
        producer.restore_state(json.loads(json.dumps(entry.start_state, allow_nan=False)))
        producer._handled_offsets = dict(entry.start_offsets)
        entry.base_state, entry.base_offsets = entry.start_state, dict(entry.start_offsets)
        entry.since.clear()

    def finish(self) -> None:
        """Save each producer's checkpoint and release it. A producer whose
        checkpoint cannot be saved goes back to the page's start, and
        :attr:`failed` keeps the earliest such failure."""
        entries, self._entries = list(self._entries.values()), {}
        for producer, entry in entries:
            try:
                if entry.first is not None:
                    save_checkpoint(producer)
            except Exception as exc:
                self._to_start(producer, entry)
                failed = RewindFailed(producer.name, entry.first)
                failed.__cause__ = exc
                if self.failed is None or entry.first.offset < self.failed.record.offset:
                    self.failed = failed
            finally:
                producer._lock.release()


def save_checkpoint(producer: Producer) -> None:
    """Save the state of ``producer`` and its handled offsets; nothing for a
    producer without ``state_version``.

    Chaski calls it after every ``@on_metric`` handler, once per page in live
    ingest (:class:`PageCheckpoints`). A callback it does not
    cover (``@every``/``@cron``, ``@on_constant``, ``@on_command``) that
    changes checkpointed state calls ``save_checkpoint(self)`` itself, after
    the change."""
    if producer.state_version is None:
        return
    if producer._checkpoint_hash is None:
        from .codehash import compute_code_hash

        producer._checkpoint_hash = compute_code_hash(type(producer))
    producer.runtime.buffer.save_checkpoint(
        producer.name,
        producer._checkpoint_hash,
        producer.state_version,
        producer.snapshot_state(),
        producer._handled_offsets,
    )
