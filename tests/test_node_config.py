"""Level-2 pins for SDK design §3.1/§7 gaps 2/4: the embedded-node config
generator, TOFU parent pinning, binary resolution order, status() derivation
from a stubbed /healthz, crash detection, and enroll_hint() text. Hermetic —
no real colcad anywhere in this file; a fake parent uses monkeypatched
``chaski.node._fetch_json`` and a fake colcad uses a subprocess this test
controls end to end."""

from __future__ import annotations

import json
import sys
import textwrap
import types

import pytest
import yaml

import chaski.node as node_mod
from chaski.node import Node, NodeCrashed, NodeStatus


def _load(node: Node) -> dict:
    return yaml.safe_load(node._config_path().read_text(encoding="utf-8"))


# -- config generation ---------------------------------------------------


def test_a_fresh_node_writes_loopback_config_with_autobind_and_no_tls(tmp_path):
    node = Node("erp-bridge", data_dir=tmp_path / "n")
    node._write_config()

    doc = _load(node)
    assert doc["ulid"] == node.ulid
    assert doc["name"] == "erp-bridge"
    assert doc["plugin"]["autobind"] == "on_new_connector"
    for door in ("api", "mqtt", "mqtt_local", "repl"):
        addr = doc[door]["addr"] if door != "api" else doc[door]["local_addr"]
        assert addr.startswith("127.0.0.1:")
    # No TLS doors except the uplink (spec §3.1): no human MQTT/WS, no tls
    # block, no auth block.
    assert "mqtt_human" not in doc
    assert "tls" not in doc
    assert "auth" not in doc
    assert "parent" not in doc


def test_no_parent_means_no_parent_block(tmp_path):
    node = Node("standalone", data_dir=tmp_path / "n")
    node._write_config()
    assert "parent" not in _load(node)


def test_an_explicit_parent_tuple_is_carried_into_the_uplink_block(tmp_path):
    node = Node(
        "erp-bridge",
        parent=("https://hub.example:19743", "abc123"),
        data_dir=tmp_path / "n",
    )
    node._write_config()

    doc = _load(node)
    # An explicit pin names the parent's REPLICATION door as reachable from
    # here (a remapped host port, a tunnel) and is carried VERBATIM into the
    # config — no ":9443" convention applied; only a bare-URL TOFU parent
    # derives the fixed door. node.parent_url is that host's API/enrollment
    # door (enroll_hint()/retire_hint() print that one, not the repl one).
    assert doc["parent"] == {"url": "https://hub.example:19743", "pubkey": "abc123"}
    assert node.parent_url == "https://hub.example"
    assert node.parent_pubkey == "abc123"


def test_retention_maps_onto_the_metrics_stream(tmp_path):
    node = Node("erp-bridge", data_dir=tmp_path / "n", retention="336h")
    node._write_config()

    doc = _load(node)
    assert doc["retention"]["streams"]["metrics"]["max_age"] == "336h"


def test_no_retention_argument_means_no_retention_block(tmp_path):
    node = Node("erp-bridge", data_dir=tmp_path / "n")
    node._write_config()
    assert "retention" not in _load(node)


def test_identity_ports_and_admin_token_survive_a_restart(tmp_path):
    data_dir = tmp_path / "n"
    first = Node("erp-bridge", data_dir=data_dir)
    first._write_config()
    ulid, token, ports = first.ulid, first.admin_token, dict(first._ports)

    # "restart": a brand new Node instance pointed at the same data_dir.
    second = Node("erp-bridge", data_dir=data_dir)
    second._write_config()

    assert second.ulid == ulid
    assert second.admin_token == token
    assert second._ports == ports


def test_two_nodes_at_different_data_dirs_never_collide_on_ports(tmp_path):
    a = Node("node-a", data_dir=tmp_path / "a")
    b = Node("node-b", data_dir=tmp_path / "b")
    a._write_config()
    b._write_config()

    assert set(a._ports.values()).isdisjoint(b._ports.values())


def test_default_data_dir_is_under_colca_nodes(monkeypatch, tmp_path):
    monkeypatch.setattr(node_mod.Path, "home", classmethod(lambda cls: tmp_path))
    node = Node("erp-bridge")
    assert node.data_dir == tmp_path / ".colca" / "nodes" / "erp-bridge"


# -- TOFU parent pinning ---------------------------------------------------


def test_a_bare_url_pins_the_parents_pubkey_on_first_use(tmp_path, monkeypatch):
    monkeypatch.setattr(node_mod, "_fetch_json", lambda url, timeout: {"pubkey": "beef01", "ulid": "n-hub"})
    node = Node("erp-bridge", parent="https://hub.example", data_dir=tmp_path / "n")
    node._write_config()

    assert node.parent_pubkey == "beef01"
    pin = json.loads((tmp_path / "n" / "parent.pin.json").read_text())
    assert pin == {"url": "https://hub.example", "pubkey": "beef01"}
    # The pin file (and enroll_hint()) keep the API door; the CONFIG's
    # parent.url is that host's fixed replication door (9443).
    assert _load(node)["parent"] == {"url": "https://hub.example:9443", "pubkey": "beef01"}


def test_repl_url_derives_the_fixed_replication_port_from_the_api_host(monkeypatch):
    assert node_mod._repl_url("https://hub.example") == "https://hub.example:9443"
    assert node_mod._repl_url("https://hub.example:443") == "https://hub.example:9443"
    assert node_mod._repl_url("hub.example") == "https://hub.example:9443"


def test_a_second_start_reuses_the_pin_without_a_mismatch(tmp_path, monkeypatch):
    calls = {"n": 0}

    def fake_fetch(url, timeout):
        calls["n"] += 1
        return {"pubkey": "beef01", "ulid": "n-hub"}

    monkeypatch.setattr(node_mod, "_fetch_json", fake_fetch)
    data_dir = tmp_path / "n"
    Node("erp-bridge", parent="https://hub.example", data_dir=data_dir)._write_config()
    second = Node("erp-bridge", parent="https://hub.example", data_dir=data_dir)
    second._write_config()

    assert calls["n"] == 2  # TOFU re-verifies on every start; both agreed
    assert second.parent_pubkey == "beef01"


def test_a_changed_key_at_the_same_url_is_refused(tmp_path, monkeypatch):
    data_dir = tmp_path / "n"
    monkeypatch.setattr(node_mod, "_fetch_json", lambda url, timeout: {"pubkey": "beef01", "ulid": "n-hub"})
    Node("erp-bridge", parent="https://hub.example", data_dir=data_dir)._write_config()

    monkeypatch.setattr(node_mod, "_fetch_json", lambda url, timeout: {"pubkey": "danger02", "ulid": "n-hub"})
    with pytest.raises(RuntimeError, match="different key"):
        Node("erp-bridge", parent="https://hub.example", data_dir=data_dir)._write_config()


def test_an_explicit_tuple_re_pins_over_a_mismatched_existing_pin(tmp_path, monkeypatch):
    data_dir = tmp_path / "n"
    monkeypatch.setattr(node_mod, "_fetch_json", lambda url, timeout: {"pubkey": "beef01", "ulid": "n-hub"})
    Node("erp-bridge", parent="https://hub.example", data_dir=data_dir)._write_config()

    # An explicit (url, pubkey) is itself the re-pin mechanism — no error.
    reset = Node("erp-bridge", parent=("https://hub.example", "danger02"), data_dir=data_dir)
    reset._write_config()
    assert reset.parent_pubkey == "danger02"
    pin = json.loads((data_dir / "parent.pin.json").read_text())
    assert pin["pubkey"] == "danger02"


def test_an_unreachable_parent_falls_back_to_the_existing_pin(tmp_path, monkeypatch):
    data_dir = tmp_path / "n"
    monkeypatch.setattr(node_mod, "_fetch_json", lambda url, timeout: {"pubkey": "beef01", "ulid": "n-hub"})
    Node("erp-bridge", parent="https://hub.example", data_dir=data_dir)._write_config()

    def unreachable(url, timeout):
        raise OSError("connection refused")

    monkeypatch.setattr(node_mod, "_fetch_json", unreachable)
    second = Node("erp-bridge", parent="https://hub.example", data_dir=data_dir)
    second._write_config()  # must not raise: falls back to the pin
    assert second.parent_pubkey == "beef01"


def test_a_never_before_seen_unreachable_parent_raises(tmp_path, monkeypatch):
    def unreachable(url, timeout):
        raise OSError("connection refused")

    monkeypatch.setattr(node_mod, "_fetch_json", unreachable)
    with pytest.raises(RuntimeError, match="could not reach"):
        Node("erp-bridge", parent="https://hub.example", data_dir=tmp_path / "n")._write_config()


# -- binary resolution ---------------------------------------------------


def test_binary_resolution_prefers_explicit_argument_over_everything(monkeypatch, tmp_path):
    monkeypatch.setenv(node_mod._COLCAD_ENV, "/from/env/colcad")
    node = Node("erp-bridge", data_dir=tmp_path / "n", binary="/explicit/colcad")
    assert node._binary == "/explicit/colcad"


def test_binary_resolution_order_env_then_wheel_then_path(monkeypatch):
    monkeypatch.delenv(node_mod._COLCAD_ENV, raising=False)
    monkeypatch.setattr(node_mod.shutil, "which", lambda name: "/usr/local/bin/colcad")
    assert node_mod._resolve_colcad_binary() == "/usr/local/bin/colcad"

    monkeypatch.setenv(node_mod._COLCAD_ENV, "/from/env/colcad")
    assert node_mod._resolve_colcad_binary() == "/from/env/colcad"


def test_binary_resolution_falls_through_a_wheel_with_no_binary_populated(monkeypatch, tmp_path):
    # A locally path-sourced colcad (dev/[tool.uv.sources], never
    # scripts/build_colcad_wheels.py) imports fine but ships no bin/colcad —
    # this must fall through to PATH, not hand Popen a path that can never
    # exist.
    monkeypatch.delenv(node_mod._COLCAD_ENV, raising=False)
    fake_module = types.ModuleType("colcad")
    fake_module.BINARY_PATH = tmp_path / "bin" / "colcad"  # deliberately absent
    monkeypatch.setitem(sys.modules, "colcad", fake_module)
    monkeypatch.setattr(node_mod.shutil, "which", lambda name: "/usr/local/bin/colcad")

    assert node_mod._resolve_colcad_binary() == "/usr/local/bin/colcad"


def test_binary_resolution_failure_names_the_node_extra(monkeypatch):
    monkeypatch.delenv(node_mod._COLCAD_ENV, raising=False)
    monkeypatch.setattr(node_mod.shutil, "which", lambda name: None)
    with pytest.raises(RuntimeError, match=r"chaski\[node\]"):
        node_mod._resolve_colcad_binary()


# -- status() derivation ---------------------------------------------------


class _FakeProcess:
    """Stands in for subprocess.Popen: poll() answers None while "running"."""

    def __init__(self):
        self.returncode = None

    def poll(self):
        return self.returncode


def _node_with_fake_process(tmp_path, *, parent="https://hub.example"):
    node = Node("erp-bridge", parent=parent, data_dir=tmp_path / "n")
    node._process = _FakeProcess()
    node._ports = {"api_local": 1}
    node.ulid, node.pubkey = "n-child", "childpub"
    node.parent_url, node.parent_pubkey = parent, "hubpub"
    return node


def test_status_is_stopped_before_start(tmp_path):
    node = Node("erp-bridge", data_dir=tmp_path / "n")
    assert node.status() == NodeStatus("stopped")


def test_status_is_starting_while_healthz_is_unreachable(tmp_path, monkeypatch):
    node = _node_with_fake_process(tmp_path)
    monkeypatch.setattr(node_mod, "_fetch_json", lambda url, timeout: (_ for _ in ()).throw(OSError("refused")))
    assert node.status().state == "starting"


def test_status_is_awaiting_enrollment_when_uplink_is_unauthorized(tmp_path, monkeypatch):
    node = _node_with_fake_process(tmp_path)
    monkeypatch.setattr(
        node_mod,
        "_fetch_json",
        lambda url, timeout: {
            "ok": True,
            "ulid": "n-child",
            "pubkey": "childpub",
            "uplink": {"state": "unauthorized"},
        },
    )
    status = node.status()
    assert status.state == "awaiting_enrollment"
    assert "POST /enroll" in status.detail


def test_status_is_enrolled_once_uplink_connects(tmp_path, monkeypatch):
    node = _node_with_fake_process(tmp_path)
    monkeypatch.setattr(
        node_mod,
        "_fetch_json",
        lambda url, timeout: {
            "ok": True,
            "ulid": "n-child",
            "pubkey": "childpub",
            "uplink": {"state": "connected"},
        },
    )
    assert node.status().state == "enrolled"


def test_status_is_offline_after_having_connected_and_then_losing_the_link(tmp_path, monkeypatch):
    node = _node_with_fake_process(tmp_path)
    monkeypatch.setattr(
        node_mod,
        "_fetch_json",
        lambda url, timeout: {
            "ok": True,
            "ulid": "n-child",
            "pubkey": "childpub",
            "uplink": {"state": "connected"},
        },
    )
    assert node.status().state == "enrolled"  # marks _ever_connected

    monkeypatch.setattr(
        node_mod,
        "_fetch_json",
        lambda url, timeout: {
            "ok": True,
            "ulid": "n-child",
            "pubkey": "childpub",
            "uplink": {"state": "connecting"},
        },
    )
    assert node.status().state == "offline"


def test_status_is_awaiting_enrollment_when_never_connected_and_connecting(tmp_path, monkeypatch):
    node = _node_with_fake_process(tmp_path)
    monkeypatch.setattr(
        node_mod,
        "_fetch_json",
        lambda url, timeout: {
            "ok": True,
            "ulid": "n-child",
            "pubkey": "childpub",
            "uplink": {"state": "connecting"},
        },
    )
    assert node.status().state == "awaiting_enrollment"


def test_status_is_enrolled_for_a_root_node_with_no_parent(tmp_path, monkeypatch):
    node = _node_with_fake_process(tmp_path, parent=None)
    monkeypatch.setattr(
        node_mod,
        "_fetch_json",
        lambda url, timeout: {
            "ok": True,
            "ulid": "n-root",
            "pubkey": "rootpub",
            "uplink": {"state": "none"},
        },
    )
    assert node.status().state == "enrolled"


# -- crash detection ---------------------------------------------------


def test_status_is_crashed_after_the_process_exits_on_its_own(tmp_path):
    node = _node_with_fake_process(tmp_path)
    node._log_tail.append("FATAL: could not open store")
    node._process.returncode = 1

    status = node.status()
    assert status.state == "crashed"
    assert "could not open store" in status.detail


def test_service_raises_node_crashed_after_a_crash(tmp_path):
    node = _node_with_fake_process(tmp_path)
    node._process.returncode = 1

    with pytest.raises(NodeCrashed):
        node.service("erp")


def test_wait_enrolled_raises_node_crashed_immediately_on_a_crashed_process(tmp_path):
    node = _node_with_fake_process(tmp_path)
    node._process.returncode = 1

    with pytest.raises(NodeCrashed):
        node.wait_enrolled(timeout=5.0)


def test_wait_enrolled_times_out_with_the_status_detail(tmp_path, monkeypatch):
    node = _node_with_fake_process(tmp_path)
    monkeypatch.setattr(
        node_mod,
        "_fetch_json",
        lambda url, timeout: {
            "ok": True,
            "ulid": "n-child",
            "pubkey": "childpub",
            "uplink": {"state": "unauthorized"},
        },
    )
    monkeypatch.setattr(node_mod.time, "sleep", lambda s: None)
    with pytest.raises(TimeoutError, match="POST /enroll"):
        node.wait_enrolled(timeout=0.01)


# -- enroll_hint() / retire_hint() ---------------------------------------------------


def test_enroll_hint_names_pubkey_ulid_and_a_default_mount(tmp_path):
    node = _node_with_fake_process(tmp_path, parent="https://hub.example")
    hint = node.enroll_hint()
    assert hint == (
        "enroll this node at https://hub.example: author an element at 'erp-bridge' there, then "
        "POST /enroll with the admin token ($COLCA_ADMIN_TOKEN) and "
        '{"ulid": "n-child", "kind": "node", "element": "<that element id>", "pubkey": "childpub"}'
    )


def test_enroll_hint_accepts_an_explicit_mount(tmp_path):
    node = _node_with_fake_process(tmp_path)
    assert "'site1/erp'" in node.enroll_hint(mount="site1/erp")


def test_enroll_hint_without_a_parent_is_refused(tmp_path):
    node = Node("erp-bridge", data_dir=tmp_path / "n")
    with pytest.raises(RuntimeError, match="no parent"):
        node.enroll_hint()


def test_enroll_hint_before_start_is_refused(tmp_path):
    node = Node("erp-bridge", parent="https://hub.example", data_dir=tmp_path / "n")
    node.parent_url = "https://hub.example"  # resolved (as _write_config would), but never started
    with pytest.raises(RuntimeError, match="started node"):
        node.enroll_hint()


def test_retire_hint_names_the_parent_and_this_nodes_ulid(tmp_path):
    node = _node_with_fake_process(tmp_path, parent="https://hub.example")
    assert (
        node.retire_hint()
        == 'curl -sk -X DELETE https://hub.example/enroll/n-child -H "X-Colca-Token: $COLCA_ADMIN_TOKEN"'
    )


def test_retire_hint_without_a_parent_is_refused(tmp_path):
    node = Node("erp-bridge", data_dir=tmp_path / "n")
    with pytest.raises(RuntimeError, match="no parent"):
        node.retire_hint()


# -- rotating log writer ---------------------------------------------------


def test_the_log_writer_rotates_past_its_size_cap(tmp_path):
    path = tmp_path / "colcad.log"
    writer = node_mod._RotatingLogWriter(path, max_bytes=16)
    writer.write(b"0123456789")
    writer.write(b"0123456789")  # crosses 16 bytes -> rotates
    writer.close()

    assert path.with_name("colcad.log.1").exists()
    assert path.with_name("colcad.log.1").stat().st_size == 20
    assert path.exists()  # a fresh (now empty) file is open for the next write


# -- a real (fake-colcad-binary) subprocess lifecycle ---------------------


FAKE_COLCAD = textwrap.dedent(
    """\
    import http.server
    import json
    import sys
    import threading

    config_path = sys.argv[1]

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/healthz":
                body = json.dumps({"ok": True, "ulid": "n-fake", "pubkey": "fakepub",
                                   "uplink": {"state": "none"}}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *a):
            pass

    # Like the real colcad: every door is configured `:0`, the fake binds
    # what the kernel gives it and REPORTS the result through addr_file.
    import os
    addr_file = None
    with open(config_path) as f:
        for line in f:
            if line.startswith("addr_file:"):
                addr_file = line.split(":", 1)[1].strip().strip("'").strip(chr(34))
    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    addresses = {door: f"127.0.0.1:{port}" for door in ("api", "api_local", "mqtt", "mqtt_local", "repl")}
    with open(addr_file + ".tmp", "w") as f:
        json.dump(addresses, f)
    os.replace(addr_file + ".tmp", addr_file)
    print("fake colcad up", flush=True)
    server.serve_forever()
    """
)


@pytest.fixture()
def fake_colcad(tmp_path):
    script = tmp_path / "fake_colcad.py"
    script.write_text(FAKE_COLCAD)
    return [sys.executable, str(script)]


def test_start_wait_healthy_stop_against_a_fake_binary(tmp_path, fake_colcad):
    # Node.start() invokes `[binary, config_path]` — the fake colcad above
    # is `[python, script.py]`, so a tiny shell wrapper stands in as "the
    # colcad binary" the same way a real platform wheel's entry point would.
    wrapper = tmp_path / "colcad"
    wrapper.write_text(f'#!/bin/sh\nexec {fake_colcad[0]} {fake_colcad[1]} "$@"\n')
    wrapper.chmod(0o755)
    node = Node("fake", data_dir=tmp_path / "n", binary=str(wrapper))

    node.start(timeout=10.0)
    try:
        assert node.status().state == "enrolled"  # uplink state "none": no parent
        assert node.pubkey == "fakepub"
        assert node.ulid == "n-fake"
        log_text = (tmp_path / "n" / "colcad.log").read_text()
        assert "fake colcad up" in log_text
    finally:
        node.stop()
    assert node.status().state == "stopped"


def test_a_crashing_binary_is_reported_as_crashed(tmp_path):
    script = tmp_path / "dies.py"
    script.write_text("import sys; sys.stderr.write('boom: could not bind\\n'); sys.exit(7)\n")
    wrapper = tmp_path / "colcad"
    wrapper.write_text(f'#!/bin/sh\nexec {sys.executable} {script} "$@"\n')
    wrapper.chmod(0o755)
    node = Node("fake", data_dir=tmp_path / "n", binary=str(wrapper))

    with pytest.raises((RuntimeError, TimeoutError), match="boom|exit|healthy"):
        node.start(timeout=5.0)


# -- the contracts bundle -----------------------------------------------------


def test_the_config_points_colcad_at_the_contracts_bundle(tmp_path, monkeypatch):
    """A colcad without a bundle runs on the builtin floor and rejects every
    Colca contract (the embedded node enrolled fine and then refused its own
    service's _DataTags, level 4): the config names the bundle, from the env
    override here, from the colcad package otherwise."""
    monkeypatch.setenv("COLCAD_CONTRACTS_BUNDLE", "/from/env/contracts-bundle.json")
    node = Node("erp-bridge", data_dir=tmp_path / "n")
    node._write_config()
    assert _load(node)["contracts"] == {"bundle": "/from/env/contracts-bundle.json"}

    explicit = Node("erp-bridge", data_dir=tmp_path / "m", contracts_bundle="/explicit/bundle.json")
    explicit._write_config()
    assert _load(explicit)["contracts"] == {"bundle": "/explicit/bundle.json"}


def test_a_missing_contracts_bundle_is_refused_before_colcad_starts(tmp_path, monkeypatch):
    monkeypatch.delenv("COLCAD_CONTRACTS_BUNDLE", raising=False)
    monkeypatch.setitem(sys.modules, "colcad", None)  # not installed
    node = Node("erp-bridge", data_dir=tmp_path / "n")
    with pytest.raises(RuntimeError, match="contracts bundle"):
        node._write_config()
