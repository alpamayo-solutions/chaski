"""Per-producer code hash for hash-triggered broker-window replay (design §10).

The service computes one SHA-256 digest per registered producer, covering:

1. the producer's own defining module source (the whole file, not just the
   class body);
2. the source of every module it transitively imports that is
   PROJECT-LOCAL — a customer-mounted module, a shared calculation helper
   under the same flat customer-modules namespace, or a shipped producer
   base such as the ``dataops`` image's ``dataops.producers.machine_state``
   — with every installed dependency (anything resolving under a
   ``site-packages``/``dist-packages`` directory, or the standard library)
   EXCLUDED. This SDK is an installed dependency in a deployment, so the
   ``chaski.dataops`` framework itself is outside the hash: upgrading the
   SDK does not replay every producer on every node, and a producer's
   hash says "MY calculation changed", not "something underneath me did";
3. a canonical (sorted-key JSON) dump of the current values of whatever
   env vars the producer declares via ``Producer.config_keys``.

Two failure modes this is written to avoid (both explicitly called out in
the design):

* Silently excluding an imported local module (e.g. a shared pure-calc
  helper a customer producer imports) would mean a real calculation change
  never triggers replay.
* Accidentally including a site-packages dependency, or hashing a
  dict/set in non-canonical order, would mean an UNCHANGED producer
  computes a DIFFERENT hash on every restart — replaying the whole window
  forever instead of exactly once per real change.

The traversal is static and side-effect-free: it walks ``import``
statements via :mod:`ast` and resolves each target against ``sys.modules``
ONLY — it never triggers a new import. By the time this runs (service
startup, after producer discovery), every module a registered producer can
legitimately reference is already imported, so this is not a limitation in
practice.
"""

from __future__ import annotations

import ast
import hashlib
import json
import logging
import sys
import sysconfig
from pathlib import Path
from typing import Any

from decouple import config as decouple_config

log = logging.getLogger("chaski.dataops.codehash")

_STDLIB_MODULE_NAMES = frozenset(getattr(sys, "stdlib_module_names", ()))


def _stdlib_roots() -> list[Path]:
    roots: list[Path] = []
    for key in ("stdlib", "platstdlib"):
        try:
            path = sysconfig.get_path(key)
        except Exception as exc:  # pragma: no cover - defensive, platform-dependent
            log.debug("sysconfig.get_path(%r) unavailable on this platform: %s", key, exc)
            continue
        if path:
            roots.append(Path(path).resolve())
    return roots


_STDLIB_ROOTS = _stdlib_roots()


# ─── project-local classification ──────────────────────────────────────────


def _is_project_local(module: Any) -> bool:
    """True iff ``module`` is code this deployment owns — never an
    installed dependency (site-packages/dist-packages) and never the
    standard library.

    This is deliberately NOT scoped to a fixed root like "the customer-
    modules dir" — a shipped image's own producer modules
    (``dataops.producers.*``, an editable install of that project) are
    equally project-local, and live outside site-packages the same way a
    customer-mounted ``.py`` file does (verified: an editable/local install
    of a package resolves its ``__file__`` OUTSIDE site-packages, while
    every PyPI/wheel dependency — including ``colca-data-contracts`` and
    this SDK, once installed into a deployment — resolves INSIDE it).
    """
    name = getattr(module, "__name__", None)
    if not name:
        return False
    top_level = name.split(".", 1)[0]
    if top_level in _STDLIB_MODULE_NAMES:
        return False

    file = getattr(module, "__file__", None)
    if not file:
        return False  # namespace/built-in/frozen module — nothing to hash

    path = Path(file).resolve()
    if "site-packages" in path.parts or "dist-packages" in path.parts:
        return False
    for stdlib_root in _STDLIB_ROOTS:
        try:
            path.relative_to(stdlib_root)
            return False
        except ValueError:
            continue
    return True


def _read_source(module: Any) -> str | None:
    """The module's CURRENT on-disk source, read fresh every call (never
    via :mod:`linecache`, which caches by mtime/size and can serve stale
    content) — the whole point of this module is to detect a source change
    the moment it happens, including within a single test process."""
    file = getattr(module, "__file__", None)
    if not file:
        return None
    try:
        return Path(file).read_text(encoding="utf-8")
    except OSError:
        log.warning("Could not read source of %s at %s — excluded from the hash", module.__name__, file)
        return None


# ─── static import-graph walk (sys.modules only, never triggers an import) ─


def _resolve_from_base(module: Any, node: ast.ImportFrom) -> str:
    """The dotted module name a ``from X import Y`` statement's ``X`` half
    resolves to, honoring relative imports (``node.level > 0``) against the
    importING module's own ``__package__``."""
    if node.level == 0:
        return node.module or ""
    package = getattr(module, "__package__", "") or ""
    parts = package.split(".") if package else []
    trim = node.level - 1
    if trim:
        parts = parts[:-trim] if trim <= len(parts) else []
    base = ".".join(p for p in parts if p)
    if node.module:
        return f"{base}.{node.module}" if base else node.module
    return base


def _imported_module_names(tree: ast.AST, module: Any) -> set[str]:
    """Every dotted name an ``import``/``from ... import ...`` statement in
    ``tree`` could plausibly refer to. Over-approximates on purpose (e.g. a
    ``from pkg import func`` also yields ``pkg.func`` as a candidate, which
    is a name rather than a module) — harmless, since resolution below only
    keeps candidates that are ACTUALLY present in ``sys.modules``."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            base = _resolve_from_base(module, node)
            if base:
                names.add(base)
            for alias in node.names:
                names.add(f"{base}.{alias.name}" if base else alias.name)
    return names


def _transitive_project_local_modules(root_module: Any) -> dict[str, Any]:
    """BFS from ``root_module`` over import statements, staying entirely
    inside ``sys.modules`` (never imports anything new), collecting every
    project-local module reached — ``root_module`` itself included when it
    qualifies (it always does for a real producer module)."""
    seen: dict[str, Any] = {}
    stack = [root_module]
    while stack:
        module = stack.pop()
        name = getattr(module, "__name__", None)
        if not name or name in seen:
            continue
        if not _is_project_local(module):
            continue
        seen[name] = module

        source = _read_source(module)
        if source is None:
            continue
        try:
            tree = ast.parse(source)
        except SyntaxError:
            log.warning(
                "Could not parse %s for import discovery — its own "
                "source is still hashed, but its imports are not walked",
                name,
            )
            continue

        for candidate in _imported_module_names(tree, module):
            resolved = sys.modules.get(candidate)
            if resolved is not None and resolved.__name__ not in seen:
                stack.append(resolved)
    return seen


# ─── config fingerprint ─────────────────────────────────────────────────────


def _config_fingerprint(producer_cls: type) -> str:
    """Canonical (sorted-key JSON) dump of the current values of every env
    var ``producer_cls.config_keys`` names. Read as plain strings (no cast)
    — the hash only needs to detect "this changed", not interpret it."""
    keys = sorted(set(getattr(producer_cls, "config_keys", ()) or ()))
    values = {key: decouple_config(key, default="") for key in keys}
    return json.dumps(values, sort_keys=True)


# ─── public API ─────────────────────────────────────────────────────────────


def _update(hasher, text: str) -> None:
    """Length-prefixed feed — never a bare separator — so no concatenation
    of (name, source) pairs can collide with a different split of the same
    bytes."""
    data = text.encode("utf-8")
    hasher.update(len(data).to_bytes(8, "big"))
    hasher.update(data)


def compute_code_hash(producer_cls: type) -> str:
    """SHA-256 hex digest for one producer class (design §10).

    Deterministic across processes given unchanged inputs: module names and
    config keys are both sorted before hashing, so dict/set iteration order
    can never perturb the result.
    """
    module = sys.modules.get(producer_cls.__module__)
    if module is None:
        raise RuntimeError(
            f"{producer_cls.__name__}'s module {producer_cls.__module__!r} is not "
            "in sys.modules — compute_code_hash must run after the producer is imported"
        )

    project_local = _transitive_project_local_modules(module)

    hasher = hashlib.sha256()
    for name in sorted(project_local):
        source = _read_source(project_local[name]) or ""
        _update(hasher, name)
        _update(hasher, source)

    _update(hasher, _config_fingerprint(producer_cls))

    return hasher.hexdigest()
