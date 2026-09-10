"""Per-producer code hash for hash-triggered replay.

The service computes one SHA-256 digest per producer, covering:

1. the source of the producer's module;
2. the source of every project-local module it imports, directly or not.
   Installed packages and the standard library are excluded, chaski
   included, so upgrading the SDK does not replay every producer;
3. the current values of the environment variables in
   ``Producer.config_keys``, as sorted-key JSON.

Missing a local module would hide a real calculation change; including an
installed package, or hashing in unstable order, would replay on every restart.

The traversal walks ``import`` statements with :mod:`ast` and resolves them
against ``sys.modules`` only, so it never imports anything. By the time it
runs, every module a producer uses is already imported.
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
    """True if ``module`` is code this deployment owns: not under
    site-packages or dist-packages, and not the standard library.

    Editable installs count as local because their files live outside
    site-packages, like a mounted ``.py`` file does.
    """
    name = getattr(module, "__name__", None)
    if not name:
        return False
    top_level = name.split(".", 1)[0]
    if top_level in _STDLIB_MODULE_NAMES:
        return False

    file = getattr(module, "__file__", None)
    if not file:
        return False  # namespace, built-in or frozen module: nothing to hash

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
    """The module's current source, read from disk on every call; linecache
    could serve a stale copy."""
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
    """Every dotted name an import statement in ``tree`` could refer to. It
    over-approximates (``from pkg import func`` also yields ``pkg.func``);
    only names present in ``sys.modules`` are kept later."""
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
    """Every project-local module reachable from ``root_module`` through
    imports, ``root_module`` included, without importing anything new."""
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
    """Sorted-key JSON of the current values of ``producer_cls.config_keys``,
    read as plain strings."""
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
    """SHA-256 hex digest for one producer class.

    Module names and config keys are sorted first, so the digest is the same
    in every process for unchanged inputs.
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
