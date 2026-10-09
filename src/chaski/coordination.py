"""Optional execution barriers for bounded application-clock windows.

The authority grants a window by setting ClockDefinition.stop_at. A worker
processes that boundary only after its declared upstream workers committed it.
Ordered ClockProgress records own completion and functional readiness.
ServiceDetails supplies registration and discovery only.
"""

from __future__ import annotations

import asyncio
import copy
import json
import math
import os
import threading
import time
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

    def __init__(
        self, service: Service, dependencies: list[str], path: Path, *, asynchronous=False, monotonic=time.monotonic
    ):
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
        self.asynchronous = asynchronous
        self._monotonic = monotonic
        self._health_received: dict[str, tuple[float, float]] = {}
        self._issued: dict[float, str] = {}
        self._lock = threading.RLock()
        self.changes = service.clock.changes
        self._records: dict[str, dict] = {}
        self._barriers: dict[str, dict] = {}
        self._record_topics: dict[str, str] = {}
        self._ignored_topics: set[str] = set()
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
            self._health_received.clear()
            self._barriers.clear()
            self._record_topics.clear()
            self._ignored_topics.clear()
            self.changes.notify()

    def observe(self, message) -> None:
        topic, data = str(message.topic), message.payload
        if isinstance(data, (str, bytes)):
            data = json.loads(data) if data else None
        elif is_dataclass(data) and not isinstance(data, type):
            data = asdict(data)
        if "/_ClockProgress/" in topic:
            details_topic = topic.replace("/_ClockProgress/", "/_ServiceDetails/", 1)
            with self._lock:
                relevant = details_topic in self.dependencies or details_topic in self._record_topics.values()
                # Wildcard local aliases can receive a marker before identity.
                if not relevant and (
                    details_topic in self._ignored_topics
                    or not any(dep.startswith("./") for dep in self.dependencies)
                    or not details_topic.startswith(f"{topic_prefix()}_ServiceDetails/{self.service.node_id}/")
                ):
                    return
                previous = self._health_received.get(details_topic)
                observed = data.get("observed_at") if isinstance(data, dict) else None
                if (
                    not getattr(message, "retain", False)
                    and isinstance(observed, (int, float))
                    and not isinstance(observed, bool)
                    and math.isfinite(observed)
                    and (previous is None or observed != previous[1])
                ):
                    self._health_received[details_topic] = (self._monotonic(), observed)
                if self._barriers.get(details_topic) == data:
                    if relevant and self._health_received.get(details_topic) != previous:
                        self.changes.notify()
                    return
                if isinstance(data, dict):
                    self._barriers[details_topic] = dict(data)
                else:
                    self._barriers.pop(details_topic, None)
                    self._health_received.pop(details_topic, None)
                if relevant:
                    self.changes.notify()
            return
        key: str | None = topic
        if key not in self.dependencies:
            with self._lock:
                key = next((key for key, value in self._record_topics.items() if value == topic), None)
                if key is None:
                    if not isinstance(data, dict) or not topic.startswith(
                        f"{topic_prefix()}_ServiceDetails/{self.service.node_id}/"
                    ):
                        return
                    key = "./" + str(data.get("name", ""))
                    if key not in self.dependencies:
                        self._ignored_topics.add(topic)
                        self._barriers.pop(topic, None)
                        return
        with self._lock:
            if isinstance(data, dict):
                self._records[key] = data
                self._record_topics[key] = topic
            else:
                self._records.pop(key, None)
                self._health_received.pop(topic, None)
                self._record_topics.pop(key, None)
                self._barriers.pop(topic, None)
            self.changes.notify()

    def records(self, *, progress_only=False) -> dict[str, dict]:
        """Return discovery with its separately received control progress."""
        with self._lock:
            records = (
                {
                    key: {field: row.get(field) for field in ("id", "name", "is_active")}
                    for key, row in self._records.items()
                }
                if progress_only
                else copy.deepcopy(self._records)
            )
            for key, row in records.items():
                topic = self._record_topics.get(key, "")
                progress = copy.deepcopy(self._barriers.get(topic, {}))
                received = self._health_received.get(topic)
                progress["age_s"] = self._monotonic() - received[0] if received else None
                row["clock_progress"] = progress
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

    def _progress(self) -> dict[str, dict]:
        """Copy only dependency health and committed positions for readiness.

        Service records can include large catalogues. A wakeup must not decode
        and copy that metadata merely to compare a few progress fields.
        """
        with self._lock:
            result = {}
            for key, row in self._records.items():
                topic = self._record_topics.get(key, "")
                barrier = self._barriers.get(topic, {})
                received = self._health_received.get(topic)
                result[key] = {
                    "active": row.get("is_active"),
                    "ready": barrier.get("ready"),
                    "run_id": barrier.get("run_id"),
                    "age_s": self._monotonic() - received[0] if received else None,
                    "processed_at": barrier.get("processed_at"),
                }
            return result

    def ready(self) -> float | None:
        target = self.boundary()
        rows = self._progress()
        if self.asynchronous and self.dependencies:
            definition = self.service.clock.definition
            status = self.service.clock.status()
            progress = [rows.get(dep, {}) for dep in self.dependencies]
            if (
                definition
                and status.ready
                and all(
                    p.get("run_id") == definition.run_id and isinstance(p.get("processed_at"), (int, float))
                    for p in progress
                )
            ):
                target = min(definition.stop_at or status.factory_now, *(p["processed_at"] for p in progress))
                if status.factory_now < target:
                    return None
        if target is None:
            return None
        if self.completed_at is not None and self.completed_at >= target:
            # Restore liveness after a process restart without redoing effects.
            self.service.report_progress(self.completed_at)
            return None
        clock = self.service.clock
        definition = clock.definition
        if definition is None:
            return None
        if not self.dependencies:
            self._issued[target] = definition.run_id
            return target
        remaining = set(self.dependencies)
        for topic, data in rows.items():
            if topic not in remaining:
                continue
            age, done = data.get("age_s"), data.get("processed_at")
            if (
                data.get("active")
                and data.get("ready")
                and data.get("run_id") == definition.run_id
                and isinstance(age, (int, float))
                and 0 <= age <= 15
                and isinstance(done, (int, float))
                and done >= target
            ):
                remaining.remove(topic)
        if remaining:
            return None
        self._issued[target] = definition.run_id
        return target

    def wait_delay(self, maximum: float | None = None) -> float | None:
        """Next scheduled boundary, otherwise only an incoming event can help."""
        definition = self.service.clock.definition
        delay = None
        if definition is not None and definition.stop_at is not None:
            delay = self.service.clock.delay_until(definition.stop_at)
            # At the boundary we need an upstream commit or a new grant.
            if delay == 0:
                delay = None
        return (
            min(delay, maximum) if delay is not None and maximum is not None else (maximum if delay is None else delay)
        )

    async def wait_ready(self) -> float:
        """Wait for a grant and upstream commits; cancellation stops the wait."""
        while True:
            version = self.changes.version
            target = await asyncio.to_thread(self.ready)
            if target is not None:
                return target
            await self.changes.wait_async(version, self.wait_delay())

    def complete(self, timestamp: float) -> None:
        definition = self.service.clock.definition
        valid_async = (
            self.asynchronous
            and definition is not None
            and self._issued.get(timestamp) == definition.run_id
            and timestamp <= self.service.clock.now()
        )
        if self.boundary() != timestamp and not valid_async:
            raise ValueError("only an issued clock boundary can be completed")
        if self.completed_at is not None and timestamp < self.completed_at:
            raise ValueError("completion cannot move backwards")
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
        self._issued = {t: run for t, run in self._issued.items() if t > timestamp}
        self.service.report_progress(timestamp, force=True)
