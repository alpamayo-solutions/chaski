"""Level-2 pin for the consume lane on ``chaski.Service`` (service families
design 2026-09-07 §3.3, D7): ``stream()`` and ``kv()`` against a fake door
that models what colcad actually does with a named cursor — stores its
position server-side, serves ``/fetch`` from it, moves it only on ``/ack``.

Hermetic: the MQTT side is the same fake client and monkeypatched
connection functions ``test_service_lifecycle.py`` uses; the HTTP side is
``_FakeDoor`` installed in place of ``chaski.service.Door``.
"""

from __future__ import annotations

from pathlib import Path

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import pytest
from colca_data_contracts.local_service import LocalServiceIdentity

from chaski.door import Gap, KvEntry, Page, Record, Stream
from chaski.service import Service


class _FakeReasonCode:
    is_failure = False


class _FakeClient:
    def __init__(self) -> None:
        self.published: list[tuple[str, object]] = []
        self.on_connect = None

    def loop_start(self) -> None:
        if self.on_connect is not None:
            self.on_connect(self, None, None, _FakeReasonCode())

    def loop_stop(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def subscribe(self, topic, qos: int = 0, callback=None) -> None:  # noqa: ARG002
        pass

    def publish(self, topic, payload, qos: int = 0, retain: bool = False, wait: bool = True) -> None:  # noqa: ARG002
        self.published.append((str(topic), payload))

    def publish_tombstone(self, topic, qos: int = 0, wait: bool = True) -> None:  # noqa: ARG002
        pass


def _record(offset: int, stream: str = "annotations") -> Record:
    return Record(
        offset=offset, origin_offset=offset, topic=f"colca/v1/_X/n-1/{stream}/{offset}",
        payload={"n": offset}, ts=float(offset) * 1000, written_by="svc",
        actor_id="a", actor_label="svc", actor_kind="local",
    )


class _FakeDoor:
    """A door with a real cursor table: ``fetch`` serves records strictly
    after the cursor's stored position, ``ack`` moves it (monotonic),
    ``delete_cursor`` forgets it. ``streams`` maps a stream name to its
    retained records; ``lwm`` (per stream) makes fetches below it report a
    gap like a pruned stream does."""

    instances: list["_FakeDoor"] = []

    def __init__(self, base_url: str, service: str, *, timeout: float = 10.0, cert=None) -> None:
        self.base_url = base_url
        self.service = service
        self.cert = cert
        self.streams: dict[str, list[Record]] = {}
        self.lwm: dict[str, int] = {}
        self.cursors: dict[tuple[str, str], int] = {}
        self.calls: list[tuple] = []
        self.kv_entries: list[KvEntry] = []
        self.closed = False
        type(self).instances.append(self)

    def close(self) -> None:
        self.closed = True

    def fetch(self, stream, cursor, *, max=1000, signal_ids=None):  # noqa: A002
        self.calls.append(("fetch", stream, cursor, max, signal_ids))
        position = self.cursors.get((stream, cursor), 0)
        lwm = self.lwm.get(stream, 1)
        gap = None
        if position + 1 < lwm:
            gap = Gap(stream=stream, from_offset=position + 1, to_offset=lwm - 1,
                      first_ts=None, last_ts=None, approx=False)
        records = [r for r in self.streams.get(stream, []) if r.offset > position and r.offset >= lwm][:max]
        if records:
            next_offset = records[-1].offset + 1
        else:
            next_offset = position + 1 if position + 1 > lwm else lwm
        return Page(records=records, next=next_offset, gap=gap)

    def ack(self, stream, cursor, offset) -> bool:
        self.calls.append(("ack", stream, cursor, offset))
        key = (stream, cursor)
        if offset <= self.cursors.get(key, 0):
            return False
        self.cursors[key] = offset
        return True

    def delete_cursor(self, stream, cursor) -> None:
        self.calls.append(("delete", stream, cursor))
        self.cursors.pop((stream, cursor), None)

    def kv(self, prefix="", *, contract=None):
        self.calls.append(("kv", prefix, contract))
        return [e for e in self.kv_entries if e.path.startswith(prefix)]

    def acks(self) -> list[tuple[str, str, int]]:
        return [c[1:] for c in self.calls if c[0] == "ack"]


@pytest.fixture
def fake_door(monkeypatch):
    _FakeDoor.instances = []
    monkeypatch.setattr("chaski.service.Door", _FakeDoor)
    return _FakeDoor


def _local_service(tmp_path: Path, monkeypatch, *, mount: str = "line1") -> Service:
    identity = LocalServiceIdentity(
        service_id="svc-ulid", service_name="erp-bridge", node_id="n-edge1",
        system_element_id="el-1", mount=mount,
    )
    monkeypatch.setattr("chaski.service.resolve_local_identity", lambda *a, **k: identity)
    monkeypatch.setattr("chaski.service.connect_local_mqtt", lambda *a, **k: (_FakeClient(), identity))
    monkeypatch.setattr("chaski.service.attach_log_publisher", lambda *a, **k: None)
    return Service("erp-bridge", mount, state_dir=tmp_path).start()


def _external_service(tmp_path: Path, monkeypatch) -> Service:
    monkeypatch.setattr("chaski.service._read_node_id", lambda url, timeout=10.0: "n-ext")  # noqa: ARG005
    monkeypatch.setattr("chaski.service._connect_external_mqtt", lambda *a, **k: _FakeClient())
    monkeypatch.setattr("chaski.service.attach_log_publisher", lambda *a, **k: None)
    return Service("erp-bridge", "site1/erp", node="https://edge1.example", state_dir=tmp_path).start()


# -- which door, which identity ---------------------------------------------


def test_local_service_opens_the_local_door_under_its_own_name(tmp_path, monkeypatch, fake_door):
    svc = _local_service(tmp_path, monkeypatch)
    (door,) = fake_door.instances
    assert door.base_url == "http://colca:80"
    assert door.service == "erp-bridge"
    assert door.cert is None
    assert svc.cursor_prefix == "c/erp-bridge/"


def test_external_service_opens_the_published_door_with_its_pinned_certificate(tmp_path, monkeypatch, fake_door):
    svc = _external_service(tmp_path, monkeypatch)
    (door,) = fake_door.instances
    assert door.base_url == "https://edge1.example:443"
    assert door.service == svc.ulid
    cert_path, key_path = door.cert
    assert cert_path.exists() and key_path.exists()
    assert svc.cursor_prefix == f"{svc.ulid}/"


def test_stream_and_kv_require_start(tmp_path, fake_door):
    svc = Service("erp-bridge", "line1", state_dir=tmp_path)
    with pytest.raises(RuntimeError, match="start\\(\\).*stream"):
        svc.stream("annotations")
    with pytest.raises(RuntimeError, match="start\\(\\).*kv"):
        svc.kv()


def test_close_closes_the_door_client(tmp_path, monkeypatch, fake_door):
    svc = _local_service(tmp_path, monkeypatch)
    (door,) = fake_door.instances
    svc.close()
    assert door.closed is True
    with pytest.raises(RuntimeError, match="closed"):
        svc.kv()


# -- stream(): a named cursor with the ingest loop's contract ---------------


def test_stream_names_its_cursor_inside_the_services_namespace(tmp_path, monkeypatch, fake_door):
    svc = _local_service(tmp_path, monkeypatch)
    assert isinstance(svc.stream("annotations"), Stream)
    assert svc.stream("annotations").cursor == "c/erp-bridge/annotations"
    assert svc.stream("metrics", cursor="ingest-02").cursor == "c/erp-bridge/ingest-02"


def test_drain_yields_every_record_in_order_and_acks_per_page_after_consumption(
    tmp_path, monkeypatch, fake_door,
):
    svc = _local_service(tmp_path, monkeypatch)
    (door,) = fake_door.instances
    door.streams["annotations"] = [_record(n) for n in range(1, 6)]

    seen: list[int] = []
    acks_when_seen: list[list] = []
    for record in svc.stream("annotations", max=2):
        seen.append(record.offset)
        acks_when_seen.append(door.acks())

    assert seen == [1, 2, 3, 4, 5]
    # Page 1 = offsets 1,2 — its ack lands only after the consumer came back
    # for offset 3, i.e. after processing 2; likewise page 2 before offset 5.
    assert acks_when_seen[1] == [], "record 2 was handed out with nothing acked yet"
    assert acks_when_seen[2] == [("annotations", "c/erp-bridge/annotations", 2)]
    assert acks_when_seen[4] == [
        ("annotations", "c/erp-bridge/annotations", 2),
        ("annotations", "c/erp-bridge/annotations", 4),
    ]
    assert door.acks()[-1] == ("annotations", "c/erp-bridge/annotations", 5)
    assert door.cursors[("annotations", "c/erp-bridge/annotations")] == 5
    fetches = [c for c in door.calls if c[0] == "fetch"]
    assert fetches[0][1:] == ("annotations", "c/erp-bridge/annotations", 2, None)
    assert len(fetches) == 4, "three pages plus the empty one that ends the drain"


def test_a_second_drain_resumes_from_the_acked_position(tmp_path, monkeypatch, fake_door):
    """The cursor is durable and server-side: what the first drain acked,
    the second never sees — and what arrived in between, it does."""
    svc = _local_service(tmp_path, monkeypatch)
    (door,) = fake_door.instances
    door.streams["alarms"] = [_record(1, "alarms"), _record(2, "alarms")]
    stream = svc.stream("alarms")

    assert [r.offset for r in stream] == [1, 2]
    door.streams["alarms"].append(_record(3, "alarms"))
    assert [r.offset for r in stream] == [3]
    assert [r.offset for r in stream] == [], "an empty page ends a drain and acks nothing"
    assert door.acks() == [("alarms", "c/erp-bridge/alarms", 2), ("alarms", "c/erp-bridge/alarms", 3)]


def test_a_consumer_that_dies_mid_page_never_acks_it_so_the_page_is_redelivered(
    tmp_path, monkeypatch, fake_door,
):
    svc = _local_service(tmp_path, monkeypatch)
    (door,) = fake_door.instances
    door.streams["annotations"] = [_record(1), _record(2), _record(3)]
    stream = svc.stream("annotations", max=2)

    processed: list[int] = []
    with pytest.raises(RuntimeError, match="crash"):
        for record in stream:
            if record.offset == 2:
                raise RuntimeError("crash while processing 2")
            processed.append(record.offset)
    assert door.acks() == [], "page 1 was never fully processed, so it was never acked"

    assert [r.offset for r in stream] == [1, 2, 3], "at-least-once: the whole page comes back"


def test_explicit_ack_commits_earlier_than_the_page_boundary(tmp_path, monkeypatch, fake_door):
    svc = _local_service(tmp_path, monkeypatch)
    (door,) = fake_door.instances
    door.streams["annotations"] = [_record(1), _record(2)]
    stream = svc.stream("annotations")

    for record in stream:
        assert stream.ack(record) is True
    assert stream.ack(2) is False, "the page's own ack afterwards is a no-op, and so is a stale one"
    assert door.cursors[("annotations", "c/erp-bridge/annotations")] == 2


def test_a_gap_is_acked_at_its_bound_when_nothing_survived(tmp_path, monkeypatch, fake_door, caplog):
    """Retention pruned everything this cursor had yet to read: the drain
    acks the gap's bound (or the next fetch reports the same gap forever),
    warns, and yields nothing."""
    svc = _local_service(tmp_path, monkeypatch)
    (door,) = fake_door.instances
    door.lwm["metrics"] = 50
    stream = svc.stream("metrics")

    with caplog.at_level("WARNING", logger="chaski.door"):
        assert list(stream) == []
    assert door.acks() == [("metrics", "c/erp-bridge/metrics", 49)]
    assert "offsets 1..49 were pruned" in caplog.text

    door.streams["metrics"] = [_record(50, "metrics")]
    assert [r.offset for r in stream] == [50]


def test_a_new_cursor_name_starts_over_and_retire_deletes_the_old_one(tmp_path, monkeypatch, fake_door):
    """dataops' generational cursor, on the SDK: a rebuilt local state opens
    a fresh cursor (which walks the whole retained window again) and
    retires the previous generation's."""
    svc = _local_service(tmp_path, monkeypatch)
    (door,) = fake_door.instances
    door.streams["metrics"] = [_record(1, "metrics"), _record(2, "metrics")]

    gen1 = svc.stream("metrics", cursor="ingest-01")
    assert [r.offset for r in gen1] == [1, 2]

    gen2 = svc.stream("metrics", cursor="ingest-02")
    assert [r.offset for r in gen2] == [1, 2], "a new cursor name knows nothing of the old one's acks"
    gen1.retire()
    assert ("delete", "metrics", "c/erp-bridge/ingest-01") in door.calls
    assert ("metrics", "c/erp-bridge/ingest-01") not in door.cursors
    assert door.cursors[("metrics", "c/erp-bridge/ingest-02")] == 2


def test_stream_passes_signal_ids_through_to_the_metrics_fetch(tmp_path, monkeypatch, fake_door):
    svc = _local_service(tmp_path, monkeypatch)
    (door,) = fake_door.instances
    list(svc.stream("metrics", signal_ids=["sig-1", "sig-2"], max=10))
    assert door.calls[0] == ("fetch", "metrics", "c/erp-bridge/metrics", 10, ["sig-1", "sig-2"])


def test_follow_drains_until_stopped(tmp_path, monkeypatch, fake_door):
    import threading

    svc = _local_service(tmp_path, monkeypatch)
    (door,) = fake_door.instances
    door.streams["annotations"] = [_record(1)]
    stop = threading.Event()
    seen: list[int] = []
    for record in svc.stream("annotations").follow(poll_interval=0.01, stop=stop):
        seen.append(record.offset)
        door.streams["annotations"].append(_record(2))
        if len(seen) == 2:
            stop.set()
    assert seen == [1, 2]


# -- kv(): a bounded snapshot -------------------------------------------------


def test_kv_passes_prefix_and_contract_filter_to_the_door(tmp_path, monkeypatch, fake_door):
    svc = _local_service(tmp_path, monkeypatch)
    (door,) = fake_door.instances
    door.kv_entries = [
        KvEntry(path="line1/press1", node_id="n", topic="colca/v1/_SystemElement/n/line1/press1",
                payload={}, ts=0.0, offset=1),
        KvEntry(path="line2/x", node_id="n", topic="colca/v1/_Signal/n/line2/x", payload={}, ts=0.0, offset=2),
    ]

    assert [e.path for e in svc.kv()] == ["line1/press1", "line2/x"]
    assert [e.path for e in svc.kv("line1", contract="_SystemElement")] == ["line1/press1"]
    assert door.calls[-1] == ("kv", "line1", "_SystemElement")
    svc.kv(contract=["_Signal", "_Group"])
    assert door.calls[-1] == ("kv", "", ["_Signal", "_Group"])
