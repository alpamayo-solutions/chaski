"""Runtime observations supplied to a deployment's telemetry exporter."""

from typing import Any


class ServiceTelemetry:
    """Nonblocking hooks; the SDK does not persist runtime observations."""

    def service_health(self, healthy: bool) -> None: ...

    def clock_progress(self, status: dict[str, Any]) -> None: ...
