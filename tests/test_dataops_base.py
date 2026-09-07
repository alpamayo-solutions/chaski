"""Unit tests for Producer discovery, trigger metadata and the watermark."""

import pytest
from dataops_fakes import FakeDoor, FakeRuntime

from chaski.dataops.base import Producer
from chaski.dataops.buffer import Buffer
from chaski.dataops.triggers import IntervalSpec, cron, every


@pytest.fixture(autouse=True)
def _isolate_registry():
    """Snapshot + restore the discovery record around each test."""
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


def test_concrete_subclass_is_registered():
    class P(Producer):
        name = "test_p"
        system_element_name = "SE-1"

        @every("10s")
        async def run(self):
            pass

    assert Producer._registry["test_p"] is P
    assert P in Producer.all()


def test_abstract_subclass_is_not_registered():
    from abc import abstractmethod

    class Intermediate(Producer):
        @abstractmethod
        async def required(self): ...

    # No name, no system_element, has abstractmethods → skipped
    assert "Intermediate" not in Producer._registry
    assert Intermediate not in Producer.all()


def test_producer_without_system_element_is_not_registered():
    class P(Producer):
        name = "no_se"
        # system_element_name intentionally absent

        @every("10s")
        async def run(self):
            pass

    assert "no_se" not in Producer._registry


def test_multiple_triggers_stack():
    class P(Producer):
        name = "stacked"
        system_element_name = "SE-1"

        @cron("0 6 * * *")
        @every("15m")
        async def run(self):
            pass

    triggers = [s for _, s in P._triggers]
    kinds = [type(s).__name__ for s in triggers]
    assert "CronSpec" in kinds
    assert "IntervalSpec" in kinds


def test_every_accepts_string_units():
    @every("500ms")
    def m1(): ...
    @every("2m")
    def m2(): ...
    @every("1h")
    def m3(): ...

    spec_m1 = m1.__colca_triggers__[0]
    spec_m2 = m2.__colca_triggers__[0]
    spec_m3 = m3.__colca_triggers__[0]
    assert isinstance(spec_m1, IntervalSpec) and spec_m1.seconds == 0.5
    assert spec_m2.seconds == 120
    assert spec_m3.seconds == 3600


def test_every_rejects_invalid_input():
    with pytest.raises(ValueError):
        every("not-a-duration")
    with pytest.raises(ValueError):
        every(0)
    with pytest.raises(ValueError):
        every(-1)
    with pytest.raises(TypeError):
        every([])  # type: ignore[arg-type]


def test_cron_rejects_invalid_expression():
    with pytest.raises(ValueError):
        cron("0 6 * *")  # only 4 fields
    with pytest.raises(ValueError):
        cron("0 6 * * * *")  # 6 fields


# ─── watermark (design §3, §10 — the framework, not the producer, owns it) ──


def _make_producer(name: str) -> Producer:
    """An instantiable Producer with just enough identity for the
    watermark tests. Deliberately not registered (no @trigger.* method) —
    these tests exercise watermark persistence, not discovery."""

    class P(Producer):
        pass

    P.name = name
    P.system_element_name = "SE-1"
    return P()


def test_watermark_requires_an_attached_runtime():
    inst = _make_producer("wm_unbound")
    with pytest.raises(RuntimeError, match="attach"):
        _ = inst.watermark


def test_watermark_is_none_before_ever_advanced(tmp_path):
    buffer = Buffer(tmp_path / "buffer.sqlite3")
    try:
        inst = _make_producer("wm_fresh").attach(FakeRuntime(FakeDoor(), buffer))
        assert inst.watermark is None
    finally:
        buffer.close()


def test_advance_watermark_persists_the_position(tmp_path):
    buffer = Buffer(tmp_path / "buffer.sqlite3")
    try:
        inst = _make_producer("wm_advance").attach(FakeRuntime(FakeDoor(), buffer))

        inst.advance_watermark(100.0)
        assert inst.watermark == 100.0

        inst.advance_watermark(200.0)
        assert inst.watermark == 200.0
    finally:
        buffer.close()


def test_advance_watermark_preserves_the_existing_code_hash(tmp_path):
    buffer = Buffer(tmp_path / "buffer.sqlite3")
    try:
        inst = _make_producer("wm_hash").attach(FakeRuntime(FakeDoor(), buffer))
        buffer.set_watermark(inst.name, 0.0, "hash-from-replay-check")

        inst.advance_watermark(50.0)

        assert buffer.watermark(inst.name) == 50.0
        assert buffer.code_hash(inst.name) == "hash-from-replay-check"
    finally:
        buffer.close()


def test_watermark_survives_a_restart(tmp_path):
    """Not just the in-memory Buffer object — a FRESH Producer over a FRESH
    Buffer instance opened on the same file must see the persisted value."""
    db_path = tmp_path / "buffer.sqlite3"

    buffer1 = Buffer(db_path)
    producer1 = _make_producer("wm_restart").attach(FakeRuntime(FakeDoor(), buffer1))
    producer1.advance_watermark(42.5)
    buffer1.close()

    buffer2 = Buffer(db_path)
    try:
        producer2 = _make_producer("wm_restart").attach(FakeRuntime(FakeDoor(), buffer2))
        assert producer2.watermark == 42.5
    finally:
        buffer2.close()


def test_two_services_in_one_process_keep_their_producers_apart(tmp_path):
    """The reason the runtime is instance state (service families design
    §3.5): two producers of the SAME class, attached to two runtimes,
    persist their watermarks in two buffers — nothing module-global routes
    one service's writes into the other's state."""

    class P(Producer):
        name = "shared_class"
        system_element_name = "SE-1"

        @every("10s")
        async def tick(self):
            pass

    buffer_a = Buffer(tmp_path / "a.sqlite3")
    buffer_b = Buffer(tmp_path / "b.sqlite3")
    try:
        a = P().attach(FakeRuntime(FakeDoor(), buffer_a))
        b = P().attach(FakeRuntime(FakeDoor(), buffer_b))
        a.advance_watermark(1.0)
        b.advance_watermark(2.0)
        assert (a.watermark, b.watermark) == (1.0, 2.0)
        assert buffer_a.watermark("shared_class") == 1.0
        assert buffer_b.watermark("shared_class") == 2.0
    finally:
        buffer_a.close()
        buffer_b.close()
