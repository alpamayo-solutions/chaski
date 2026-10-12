"""Runtime observations supplied to a deployment's telemetry exporter."""


class ServiceTelemetry:
    """Nonblocking hooks; the SDK does not persist runtime observations."""

    def service_health(self, healthy: bool) -> None: ...
