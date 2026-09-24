"""Optional execution barriers for bounded application-clock windows.

The authority grants a window by setting ClockDefinition.stop_at. A worker
processes that boundary only after its declared upstream workers committed it.
Progress uses the existing service metadata; no second clock or OS adjustment.
"""

from __future__ import annotations

import json
import math
import os
import threading
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from colca_data_contracts.root import topic_prefix

if TYPE_CHECKING:
    from .service import Service


def dependencies_from_env() -> list[str] | None:
    """Deployment seam: absent disables coordination, [] is a source worker."""
    value = os.environ.get("FACTORY_STEP_DEPENDENCIES")
    if not value:
        return None
    result = json.loads(value)
    if not isinstance(result, list) or any(not isinstance(item, str) or not item for item in result):
        raise ValueError("FACTORY_STEP_DEPENDENCIES must be a JSON list of exact service topics")
    return result


class StepGate:
    """A durable completion position, advanced only after work has succeeded.

    The callback may run again after a crash between its commit and this gate's
    commit. Business effects must therefore be idempotent, like a stream consumer.
    Dependencies are exact topics: duplicate names on different nodes are safe.
    """

    def __init__(self, service: Service, dependencies: list[str], path: Path):
        for topic in dependencies:
            if topic.startswith("./") and len(topic) > 2 and not any(c in topic for c in "+#"):
                continue
            parts = topic.split("/")
            if (
                len(parts) < 5
                or parts[2] != "_ServiceDetails"
                or any(not p for p in parts)
                or any(c in topic for c in "+#")
            ):
                raise ValueError("step dependencies must be exact _ServiceDetails topics")
        self.service, self.dependencies, self.path = service, frozenset(dependencies), path
        self._lock = threading.RLock()
        self._records: dict[str, dict] = {}
        self._barriers: dict[str, dict] = {}
        self._record_topics: dict[str, str] = {}
        self.completed_at: float | None = None
        self.run_id: str | None = None
        if path.exists():
            value = json.loads(path.read_text())
            self.completed_at, self.run_id = float(value["completed_at"]), value["run_id"]
            if not math.isfinite(self.completed_at) or not isinstance(self.run_id, str) or not self.run_id:
                raise ValueError("invalid saved clock progress")

    def topics(self) -> set[str]:
        # Placement can change a local service's topic. Select local aliases by
        # the service name in its retained record, not by an invented mount.
        details = {
            f"{topic_prefix()}_ServiceDetails/{self.service.node_id}/#" if topic.startswith("./") else topic
            for topic in self.dependencies
        }
        return details | {t.replace("/_ServiceDetails/", "/_ClockProgress/", 1) for t in details}

    def reconnect(self) -> None:
        with self._lock:
            self._records.clear()
            self._barriers.clear()
            self._record_topics.clear()

    def observe(self, message) -> None:
        topic, data = str(message.topic), message.payload
        if isinstance(data, (str, bytes)):
            data = json.loads(data) if data else None
        elif is_dataclass(data) and not isinstance(data, type):
            data = asdict(data)
        if "/_ClockProgress/" in topic:
            details_topic = topic.replace("/_ClockProgress/", "/_ServiceDetails/", 1)
            with self._lock:
                if isinstance(data, dict):
                    self._barriers[details_topic] = data
                else:
                    self._barriers.pop(details_topic, None)
            return
        key = topic
        if key not in self.dependencies:
            if not isinstance(data, dict) or not topic.startswith(
                f"{topic_prefix()}_ServiceDetails/{self.service.node_id}/"
            ):
                return
            key = "./" + str(data.get("name", ""))
            if key not in self.dependencies:
                return
        with self._lock:
            if isinstance(data, dict):
                self._records[key] = data
                self._record_topics[key] = topic
            else:
                self._records.pop(key, None)

    def records(self) -> dict[str, dict]:
        """Current upstream service records, copied for controller status views."""
        with self._lock:
            records = json.loads(json.dumps(self._records))
            for key, row in records.items():
                progress = (row.get("metadata") or {}).get("application_clock")
                if not isinstance(progress, dict):
                    continue
                barrier = self._barriers.get(self._record_topics.get(key, ""), {})
                done = barrier.get("processed_at")
                if barrier.get("run_id") != progress.get("run_id") or not isinstance(done, (int, float)):
                    progress["processed_at"] = None
                elif isinstance(progress.get("processed_at"), (int, float)):
                    progress["processed_at"] = min(done, progress["processed_at"])
            return records

    def boundary(self) -> float | None:
        clock = self.service.clock
        status, definition = clock.status(), clock.definition
        if not status.ready or definition is None or definition.stop_at is None:
            return None
        if self.run_id is not None and self.run_id != definition.run_id:
            raise ValueError("saved clock progress belongs to another run; use a fresh deployment")
        if status.factory_now is None or status.factory_now < definition.stop_at:
            return None
        return definition.stop_at

    def ready(self) -> float | None:
        target = self.boundary()
        if target is None:
            return None
        if self.completed_at is not None and self.completed_at >= target:
            # Restore liveness after a process restart without redoing effects.
            self.service.report_progress(self.completed_at)
            return None
        if not self.dependencies:
            return target
        clock = self.service.clock
        definition = clock.definition
        if definition is None:
            return None
        rows = self.records()
        remaining = set(self.dependencies)
        now = clock.real_now()
        for topic, data in rows.items():
            if topic not in remaining:
                continue
            progress = (data.get("metadata") or {}).get("application_clock") or {}
            observed, done = progress.get("observed_at"), progress.get("processed_at")
            if (
                data.get("is_active")
                and progress.get("ready")
                and progress.get("run_id") == definition.run_id
                and isinstance(observed, (int, float))
                and 0 <= now - observed <= 15
                and isinstance(done, (int, float))
                and done >= target
            ):
                remaining.remove(topic)
        return None if remaining else target

    def complete(self, timestamp: float) -> None:
        if self.boundary() != timestamp:
            raise ValueError("only the granted clock boundary can be completed")
        definition = self.service.clock.definition
        if definition is None:
            raise ValueError("clock definition disappeared before completion")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        with temporary.open("w") as output:
            json.dump({"run_id": definition.run_id, "completed_at": timestamp}, output)
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(self.path)
        self.completed_at, self.run_id = timestamp, definition.run_id
        self.service.report_progress(timestamp, force=True)
