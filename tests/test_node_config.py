"""Level-2 pin for SDK design §3.1/§7 gap 4: the embedded-node config
generator — loopback addresses on high ports, no TLS doors except the
uplink, autobind: on_new_connector, and a ulid/ports/token stable across a
restart (a second Node instance pointed at the same data_dir must not
reshuffle the node's own identity)."""

from __future__ import annotations

import yaml

from chaski.node import Node


def _load(node: Node) -> dict:
    return yaml.safe_load(node._config_path().read_text(encoding="utf-8"))


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


def test_a_parent_is_carried_into_the_uplink_block(tmp_path):
    node = Node(
        "erp-bridge",
        parent={"url": "https://hub.example", "pubkey": "abc123"},
        data_dir=tmp_path / "n",
    )
    node._write_config()

    doc = _load(node)
    assert doc["parent"] == {"url": "https://hub.example", "pubkey": "abc123"}


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
