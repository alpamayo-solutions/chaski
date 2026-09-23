# chaski

[![ci](https://github.com/alpamayo-solutions/chaski/actions/workflows/ci.yml/badge.svg)](https://github.com/alpamayo-solutions/chaski/actions/workflows/ci.yml)
[![codeql](https://github.com/alpamayo-solutions/chaski/actions/workflows/codeql.yml/badge.svg)](https://github.com/alpamayo-solutions/chaski/actions/workflows/codeql.yml)
[![License: FSL-1.1-ALv2](https://img.shields.io/badge/license-FSL--1.1--ALv2-blue)](LICENSE.md)

<!-- --8<-- [start:site] the Colca documentation site shows everything down to the end marker -->
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

chaski needs Python 3.11 or newer and runs against Colca 0.3 up to 0.6:

```bash
pip install chaski
```

Add the `dataops` extra for `DataOpsService`, and the `node` extra for `Node`,
which brings the `colcad` binary for your platform: `pip install "chaski[node]"`.
Without it, `Node` looks for `colcad` on `PATH` or at `COLCAD_BINARY`.

Each [release](https://github.com/alpamayo-solutions/chaski/releases) also
carries the wheel and sdist, and the latest code installs straight from git:

```bash
pip install "colca-data-contracts @ git+https://github.com/alpamayo-solutions/colca#subdirectory=contracts"
pip install "chaski @ git+https://github.com/alpamayo-solutions/chaski"
```

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

`@on_constant`/`@on_signal` fire on an operator/catalog `_Constant` write or a
`_Signal` binding appearing or disappearing — an integration taking over a
field, or releasing it — neither of which is a `_Metric`, so `@on_metric`
never sees them:

```python
from chaski.dataops import Producer, SignalOutput, on_constant, on_signal

class Ownership(Producer):
    name = "ownership"
    system_element_name = "oven"

    active_recipe = SignalOutput("activeRecipe", "string", "who set it and to what")

    async def on_ready(self) -> None:
        ...  # compute and publish once at startup; outputs are already bound here

    @on_constant("oven/operator/activeRecipeId")
    async def on_operator_write(self, constant) -> None:
        ...  # `constant` is None when the record was retired

    @on_signal("oven/temperature")
    async def on_integration_binding(self, signal) -> None:
        ...  # `signal` is None once the binding is released
```

`path_or_pattern` is a node-local path, or an MQTT filter over one (`+`/`#`);
delivery is retained, so subscribing also delivers whatever is already set.
`on_ready()` runs once per producer, after every output is bound and before
any trigger — `@every`/`@cron`/`@on_metric`/`@on_constant`/`@on_signal` —
can fire, for startup work that needs to publish.

`@on_command` executes a command sent to an exact node-local path and answers
it with an `_Ack` beside it (`_Ack/<node>/<path>`). A person may command but
not write data, so this is how a value a producer owns gets set from a UI:

```python
from chaski.dataops import Command, CommandRejected, Producer, on_command

class Selection(Producer):
    name = "selection"
    system_element_name = "oven"

    @on_command("oven/operator/setRecipe")            # _CmdParam unless contract= says otherwise
    async def set_recipe(self, command: Command) -> str:
        recipe = command.params.get("recipeId")
        if recipe not in self.recipes:
            raise CommandRejected(422, f"unknown recipe {recipe!r}")
        ...  # write the value, set by command.actor_id
        return f"recipe {recipe} set"             # the 200 ack's message
```

The command is read from the node's `commands` stream through a durable
cursor of the service's own, so `command.actor_id`/`actor_label` are the
node's attestation, not the sender's claim; its MQTT topic only wakes the
drain. An expired command (`expires_at`, unix ms) is answered `498` without
running the handler, `CommandRejected` answers its own code, any other
exception `500`. A page of commands is acked after it was handled, so a
restart can deliver a command twice: handlers must be idempotent.

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
<!-- --8<-- [end:site] -->

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md). chaski is
licensed under the [Functional Source License, Version 1.1, ALv2 Future
License](LICENSE.md): use it for anything except a product or service that
competes with it; every release becomes Apache 2.0 two years after it is
published.
