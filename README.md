# chaski

A Python SDK for [Colca](https://github.com/alpamayo-solutions/colca).

- `Service` publishes data to a node: through its local door inside the
  deployment, or with an enrolled key from anywhere.
- `ConnectorService` polls a machine or another system through a driver and
  publishes what it reads.
- `DataOpsService` computes signals and annotations from a node's streams.
- `Node` runs a node inside your process and hands out services on it.

The name comes from the chasquis, the runners who carried messages along the
roads of the Inca empire.

## Install

chaski needs Python 3.11 or newer. The data contracts it builds on live in the
Colca repository:

```bash
pip install "colca-data-contracts @ git+https://github.com/alpamayo-solutions/colca#subdirectory=contracts"
pip install "chaski @ git+https://github.com/alpamayo-solutions/chaski"
```

Add the `dataops` extra for `DataOpsService`. `Node` needs the `colcad` binary:
install the `chaski[node]` wheel for your platform, put `colcad` on `PATH`, or
point `COLCAD_BINARY` at it.

## Publish data

```python
import chaski

# Inside the deployment's own network the local door needs no credential.
with chaski.Service("erp-bridge", mount="site1/erp") as svc:
    svc.publish("orders/open", 42)
```

Without `node=`, a service looks for the node's local door at `colca:80` and
`colca:1883`, the name a Compose deployment gives the node.

From outside the deployment, a service proves who it is with its own key:

```python
with chaski.Service("erp-bridge", mount="site1/erp", node="https://node.example.com") as svc:
    svc.publish("orders/open", 42)
```

The key is created on first use under `~/.colca/services/erp-bridge/`
(`COLCA_STATE_DIR` moves it). Until an operator enrolls it at the node,
`start()` raises `chaski.NotEnrolled`; the message and `svc.enroll_hint()` say
what to enroll. `svc.wait_enrolled(timeout)` waits instead of raising.

Every path a service publishes becomes a tag in its catalogue. Once the node
binds a tag to a signal, the values appear in the tree.

## Read from the node

```python
with chaski.Service("shift-report", mount="site1/reports") as svc:
    for entry in svc.kv("line1", contract="_Signal"):
        print(entry.path, entry.payload["name"])

    stream = svc.stream("metrics", cursor="shift-report")
    for record in stream:          # from where the cursor stands
        handle(record.payload)
        stream.ack(record)         # the cursor moves only when you say so
```

## Poll a source

A driver implements four `async` methods: `connect()`, `discover()` (the tags
the source offers), `read(targets)` (one poll of the bound tags) and `close()`.
`ConnectorService` does the rest: catalogue, polling on a fixed cadence,
publishing on change, a heartbeat, buffering through a broker outage and
reconnecting with backoff.

```python
from chaski import ConnectorService, Driver

class MyDriver(Driver):
    ...

ConnectorService("oven-connector", mount="site1/ovens", driver=MyDriver()).run()
```

## Compute from streams

```python
from chaski.dataops import DataOpsService, Producer, SignalOutput, SignalRangeInput, on_metric

class Doubled(Producer):
    name = "doubled"
    system_element_name = "oven"

    temperature = SignalRangeInput("temperature", window="10m")
    doubled = SignalOutput("doubled", "float", "temperature times two")

    @on_metric("temperature")
    async def recompute(self, metric) -> None:
        self.doubled.publish(metric.value * 2, metric.timestamp)
        self.advance_watermark(metric.timestamp)

DataOpsService("dataops", mount="site1").add(Doubled).run()
```

Producers can also run on a schedule (`@every("30s")`, `@cron("0 6 * * 1-5")`)
and read windows of buffered values (`self.temperature.fetch(start, end)`).
When a producer's code changes, the service replays the window its inputs
cover.

## Run a node

```python
with chaski.Node("line-1", parent="https://hub.example.com") as node:
    print(node.enroll_hint())      # what the parent's operator does, once
    svc = node.service("press-bridge")
    svc.publish("press3/temp", 71.5)
```

An embedded node keeps its data under `~/.colca/nodes/line-1/`, buffers while
the parent is unreachable, and pins the parent's key on first contact.
`node.status()` reports `stopped`, `starting`, `awaiting_enrollment`,
`enrolled`, `offline` or `crashed`.

## Topic root

chaski uses the same topic root as every Colca process: `colca`, or whatever
`COLCA_TOPIC_ROOT` names.

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md). chaski is
licensed under the [Functional Source License, Version 1.1, ALv2 Future
License](LICENSE.md): use it for anything except a product or service that
competes with it; every release becomes Apache 2.0 two years after it is
published.
