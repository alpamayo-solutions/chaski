"""Tests for chaski.dataops.codehash.

Real ``.py`` files under ``tmp_path`` play a producer module and a local helper
it imports, imported with ``importlib`` so they are genuine ``sys.modules``
entries. Tests rewrite the files and hash again; codehash reads the source from
disk on every call.
"""

from __future__ import annotations

import importlib
import sys
import textwrap

import pytest

from chaski.dataops import codehash


@pytest.fixture(autouse=True)
def _isolate_sys_path_and_modules():
    """Every test gets its own throwaway module namespace on sys.path and
    never leaks an imported test module into another test or into the real
    shipped producer package."""
    saved_path = list(sys.path)
    saved_modules = set(sys.modules)
    yield
    sys.path[:] = saved_path
    for name in set(sys.modules) - saved_modules:
        del sys.modules[name]


def _write(tmp_path, name: str, source: str):
    path = tmp_path / f"{name}.py"
    path.write_text(textwrap.dedent(source))
    return path


def _import(tmp_path, name: str):
    if str(tmp_path) not in sys.path:
        sys.path.insert(0, str(tmp_path))
    return importlib.import_module(name)


def _producer(module, *, class_name: str = "P", **class_body):
    """A minimal class standing in for a Producer subclass — compute_code_hash
    only ever looks at ``__module__``, ``name``, and ``config_keys``, so a
    plain class avoids dragging in the full registration/trigger machinery."""
    return type(class_name, (), {"__module__": module.__name__, "name": "p", **class_body})


# ─── stability across restarts ──────────────────────────────────────────────


def test_hash_is_stable_across_repeated_calls_with_unchanged_source(tmp_path):
    _write(tmp_path, "prod_stable", "X = 1\n")
    module = _import(tmp_path, "prod_stable")
    cls = _producer(module)

    assert codehash.compute_code_hash(cls) == codehash.compute_code_hash(cls)


# ─── changes that must change the hash ──────────────────────────────────────


def test_hash_changes_when_the_producers_own_module_source_changes(tmp_path):
    path = _write(tmp_path, "prod_own", "X = 1\n")
    module = _import(tmp_path, "prod_own")
    cls = _producer(module)
    before = codehash.compute_code_hash(cls)

    path.write_text("X = 2\n")
    after = codehash.compute_code_hash(cls)

    assert before != after


def test_hash_changes_when_an_imported_local_module_changes(tmp_path):
    """The first named trap: silently ignoring an imported local module
    means a real calculation change never triggers replay."""
    calc_path = _write(tmp_path, "prod_calc_helper", "def compute(x):\n    return x + 1\n")
    _write(tmp_path, "prod_with_calc", "import prod_calc_helper\n")
    module = _import(tmp_path, "prod_with_calc")
    _import(tmp_path, "prod_calc_helper")
    cls = _producer(module)
    before = codehash.compute_code_hash(cls)

    calc_path.write_text("def compute(x):\n    return x + 2\n")
    after = codehash.compute_code_hash(cls)

    assert before != after, "a change to an imported LOCAL module must change the hash"


def test_hash_changes_when_a_declared_config_value_changes(tmp_path, monkeypatch):
    _write(tmp_path, "prod_cfg", "X = 1\n")
    module = _import(tmp_path, "prod_cfg")
    cls = _producer(module, config_keys=("DATAOPS_TEST_THRESHOLD",))

    monkeypatch.setenv("DATAOPS_TEST_THRESHOLD", "10")
    before = codehash.compute_code_hash(cls)
    monkeypatch.setenv("DATAOPS_TEST_THRESHOLD", "20")
    after = codehash.compute_code_hash(cls)

    assert before != after


# ─── denominators: proving the traps in the OTHER direction ────────────────


def test_hash_unaffected_by_config_value_when_producer_declares_no_config_keys(tmp_path, monkeypatch):
    """Denominator for the config test above: an env var the producer never
    named via config_keys must not perturb its hash."""
    _write(tmp_path, "prod_cfg_none", "X = 1\n")
    module = _import(tmp_path, "prod_cfg_none")
    cls = _producer(module)  # config_keys defaults to ()

    monkeypatch.setenv("DATAOPS_TEST_UNRELATED", "a")
    before = codehash.compute_code_hash(cls)
    monkeypatch.setenv("DATAOPS_TEST_UNRELATED", "b")
    after = codehash.compute_code_hash(cls)

    assert before == after


def test_a_site_packages_dependency_is_excluded_from_the_project_local_set(tmp_path):
    """An installed dependency is not part of the hash. The traversal set is
    checked directly, because changing an import line would change the
    producer's own source too."""
    _write(tmp_path, "prod_dep_yes", "import decouple\n")
    module = _import(tmp_path, "prod_dep_yes")

    project_local = codehash._transitive_project_local_modules(module)

    assert "decouple" not in project_local
    assert module.__name__ in project_local, "denominator: the producer's own module IS included"


def test_a_stdlib_module_is_excluded_from_the_project_local_set(tmp_path):
    """Same trap, the standard-library half."""
    _write(tmp_path, "prod_std_yes", "import json\n")
    module = _import(tmp_path, "prod_std_yes")

    project_local = codehash._transitive_project_local_modules(module)

    assert "json" not in project_local
    assert module.__name__ in project_local, "denominator: the producer's own module IS included"


def test_hash_is_independent_of_module_traversal_order(tmp_path, monkeypatch):
    """The same modules in a different order give the same digest."""
    _write(tmp_path, "prod_order_helper", "H = 1\n")
    _write(tmp_path, "prod_order", "import prod_order_helper\n")
    module = _import(tmp_path, "prod_order")
    _import(tmp_path, "prod_order_helper")
    cls = _producer(module)

    forward = codehash._transitive_project_local_modules(module)
    assert len(forward) >= 2, "the fixture must actually exercise more than one module"
    reversed_map = dict(reversed(list(forward.items())))

    monkeypatch.setattr(codehash, "_transitive_project_local_modules", lambda m: forward)
    h_forward = codehash.compute_code_hash(cls)
    monkeypatch.setattr(codehash, "_transitive_project_local_modules", lambda m: reversed_map)
    h_reversed = codehash.compute_code_hash(cls)

    assert h_forward == h_reversed
