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

## Write records and send commands

Everything a service writes goes over its MQTT session, at QoS 1 with the
node's PUBACK awaited; HTTP is for reading.

```python
with chaski.Service("line-guard") as svc:
    svc.send(f"colca/v1/_Finding/{svc.node_id}/line1/speed", json.dumps(finding), retain=True)
    svc.retract(f"colca/v1/_Finding/{svc.node_id}/line1/speed")   # the tombstone

    ack = svc.command("_CmdConfigure", "constant/upsert", {"constants": [...]}, lifetime=30)
    if ack["result_code"] != 200:
        raise RuntimeError(ack["message"])
```

`command()` subscribes to the command's `_Ack`, sends it with a
`correlation_id`, and returns the outcome, or raises `TimeoutError`. A
process with its own franzmq session uses `chaski.CommandSender` for the
same.

`lifetime` has no default; the sender decides. `lifetime=None` sends a
command without `expires_at`: it never expires and is delivered whenever its
node is reachable, for as long as the node's retention keeps it. A number of
seconds sends an `expires_at`; once it passed, the executor answers `498`
without running it. Give commands that move a physical machine a short
lifetime.

### Commanding a node below, and waiting separately

A hub service commands a service on an edge by naming the edge node and the
path as the hub sees it, the edge's mount first. The hub stores the command
and hands it to the edge when the edge is connected, also after an outage of
days, and the edge's answer comes back up:

```python
sent = svc.send_command(
    "_CmdParam",
    f"{edge_mount}/fleet/apply",          # the path at the hub
    {"command": {"proposal": proposal}},
    node=edge_node_id,                     # the node that executes it
    lifetime=None,                         # wait for the edge, however long
    progress=True,                         # 202 acks: queued, forwarded
)
# send_command returns once the hub stored it. Waiting is separate, and a
# wait that times out leaves the command queued.
try:
    outcome = sent.wait(timeout=30)
except TimeoutError:
    ...  # still queued; the outcome arrives later
```

`sent.correlation_id` names the command. A sender that must learn an outcome
that can come days later reads the `_Ack` records at `sent.ack_topic` from its
node's `commands` stream with a cursor of its own, instead of holding a wait
open. `chaski.is_progress(ack)` tells a `202` progress ack (`stage` `queued`
or `forwarded`) from the outcome. The node answers a command it will never
deliver: `403` when its sender lost the grant before it was forwarded, `410`
when the edge was retired or retention dropped it.

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

A consumer that keeps up with a stream reads when woken: ring a
`chaski.Doorbell` from the MQTT subscription to the topics the stream reads and
on every reconnect, and `stream.follow(bell)` drains after each ring. Pass the
same topics as `svc.stream(..., topics=[...])`. A filtered stream whose topics
stay silent also drains every `chaski.doorbell.IDLE_DRAIN_S` (5 minutes), so
its cursor moves past the records it skips instead of holding the node's
retention (`follow(bell, idle_drain_s=...)`). A consumer that stops
reading anyway shows as the node's `cursor_lag` finding (colca 0.19+), which
`svc.cursor_lag` follows and a `DataOpsService` health door fails on.

For current state, `svc.retained_view` keeps a snapshot of some contracts and
follows their changes on the durable streams. Every view names the paths it
reads:

```python
view = svc.retained_view(
    contracts=["_SystemElement", "_Signal"],
    streams=["entities"],
    cursor="plant",
    scope=chaski.ViewScope(["line1/press3", "line2"], depth=2),
)
for entry in view.read():          # local, no network I/O once loaded
    print(entry.path, entry.payload["name"])
```

A prefix is a path in whole segments without the node id: `line1/press3`
holds itself and everything below it, not `line1/press30`. `depth` keeps
entries at most that many segments below a prefix. The snapshot reads `/kv`
per prefix, and the drain fetches with the view's contracts and matching topic
filters, so records outside the scope are never applied and never count as
unread on the view's cursor. Every record on its streams wakes the view, also
one of another contract: the drain then acks the offset it scanned to, so the
cursor follows the head and never holds back the node's retention. Drains that
find nothing of the view's start at most a second apart. A scope whose prefixes, contracts and depth need
more than the node's 1000 topic filters is refused when the view is created.
There is no empty scope: `chaski.ViewScope.whole_node()` reads every path and
has to be asked for.

#### Read after write

A screen that re-reads when a record's topic fires on MQTT can reach the view
before the view applied that record. Capture the stream heads when the read
arrives and wait until the view applied through them:

```python
heads = view.heads()               # {"entities": 4711}: one tail read, moves nothing
if not view.wait_caught_up(heads, timeout=2):
    log.info("answering from position %s", view.position("entities"))
rows = view.read()                 # holds every record up to the heads
```

`view.position(stream)` is the last offset the view applied and acknowledged
(local, no network I/O; the stream may be omitted for a single-stream view).
`view.wait_caught_up(heads, timeout)` takes a `{stream: offset}` mapping, one
offset for a single-stream view, or nothing to capture `view.heads()` itself.
It waits on the view's own drain, which runs on stream-change hints, and
returns `False` when the timeout passed first or the view was closed.

A stream that `svc.consume` (or `follow`/`drain`) works through offers the
same: `stream.position` is the last offset its cursor passed in this process,
moved by every acknowledgement and set to the head when a drain finds the
cursor already there, and `stream.wait_caught_up(stream.head(), timeout=2)`
waits for the consumer to get there.

Run one process per service name on a node. A second process with the same
name connects with the same MQTT client id; the broker hands the one session
back and forth, and each process's unsubscribes remove the other's
subscriptions, so consumers stop being woken. Both processes log an error on
every takeover, report `unhealthy` in their `_ServiceDetails`, and fail their
health door; `svc.identity_conflict` says why until no takeover has followed
for 60 s. chaski does not pick which process stops.

### Push-driven consumer rules

- Subscribe before hydrating state or draining history. On reconnect, rebuild
  invalid caches and resume durable streams from their saved cursors.
- Use `RetainedView` for live current state and `Stream` for durable history.
  MQTT messages and watch hints wake consumers; they do not prove that every
  record was processed. Do not use retained state as an event history.
- Read only the contracts and paths you need: give every retained view the
  narrowest `ViewScope` it can use. Hydrate on startup, reconnect or
  explicit invalidation, then maintain the view from pushed changes. Avoid a
  separate `/kv` lookup per message or a recurring refresh timer.
- Capture the wakeup generation before draining. Continue through filtered
  pages until the stream boundary, including short pages. Never acknowledge
  retention gaps as successful processing.
- Commit effects or a durable processing inbox before acknowledging input.
  Make replay idempotent, bound queues and expose backpressure. Preserve
  timestamped samples even when their values do not change.
- Coalesce wakeups into bounded batches. Limit how often a batch starts without
  sleeping between pages that still contain admitted work. A continuous stream
  must not require reaching an empty queue before progress can be committed.
- Retry failed operations with backoff and honor `Retry-After`
  (`chaski.Backoff`: `delay(error)` after each failure, `reset()` after a
  success). Do not add periodic reads to hide a broken subscription. Expose
  connection health, last completed drain and queue/processing lag separately.
- Publish state/configuration when it changes; batch high-rate measurements.
  Command acceptance, execution and projected visibility are different events.
  An MQTT acknowledgement does not establish execution completion.
- Use real monotonic time for transport deadlines, retries and pacing; use
  factory/event time for simulated behavior. PLC acquisition, scheduled work,
  alarm deadlines, batching, health checks and display clocks are legitimate
  timers. Periodic reads to discover new Colca business records are not.
- Test reconnect, lost acknowledgements, duplicate delivery, continuous input,
  cancellation, retention gaps and restart with unchanged producer code.

See [consumer failure and producer recovery](#consumer-failure-and-producer-recovery)
for the state and acknowledgement contract, and [CONTRIBUTING](CONTRIBUTING.md#set-up)
for broker-backed tests.

## Poll a source

A driver implements four `async` methods: `connect()`, `discover()` (the tags
the source offers), `read(targets)` (one poll of the bound tags) and `close()`.
`ConnectorService` does the rest: catalogue, polling on a fixed cadence,
publishing on change, a heartbeat, buffering through a broker outage and
reconnecting with backoff.

A sample the node refuses for good (its schema, its topic rules, a missing
grant, another producer's signal) does not hold up the samples behind it. The
connector records it in its `rejected_input` `_Finding`, counts it
(`refused_samples_total`, `Telemetry.sample_refused`) and drops it. A NaN or
infinite reading, or a non-finite source timestamp, is refused the same way
before it is buffered, since JSON cannot carry it. Each signal is logged once per spell of refusals. Transport
failures and refusals the node did not decide are kept and retried.

```python
from chaski import ConnectorService, Driver

class MyDriver(Driver):
    ...

ConnectorService("oven-connector", mount="site1/ovens", driver=MyDriver()).run()
```

`run()` serves the connector with `/is_healthy` on port 8888. A driver whose
client needs the running event loop at construction (pymodbus's
`AsyncModbusTcpClient`, for one) builds the connector in a factory instead:

```python
from chaski import run_connector

run_connector(lambda: ConnectorService("press-connector", mount="site1/press", driver=ModbusDriver()))
```

### Write a signal

The standard way to set a signal is a `_CmdParam` command at the signal's own
path with `{"command": {"value": ...}}`. The connector that holds the signal's
binding executes it; no custom verb is needed. It announces a route for every
bound signal, writes through its driver's `write(target, value, command)`, reads
the tag back, and only then answers. A driver that does not implement `write`
answers `501` `unsupported`.

```python
class MyDriver(Driver):
    async def write(self, target, value, command):
        await self.client.write(target.handle, value)   # raise if the source refuses
```

`command` is the write being executed (`chaski.Command`):
`command.sender` is the sender the node attested, `command.on_behalf_of` the
person it says it acts for, `command.operation_id` its idempotency key,
`command.expires_at` its deadline (unix ms) and `command.params` every field
the sender put beside `value`.

A sender needs a `cmd` grant with the `param` class on the signal's path:

```python
sent = svc.write_signal(
    "line1/press3/setpoint", 12.5, lifetime=10,    # short: it moves a machine
    operation_id=request_id,                       # the same id when retrying this write
    on_behalf_of={"id": user.sub, "label": user.username},
)
try:
    answer = sent.wait(timeout=15)
except TimeoutError:
    answer = None   # result unknown: the write may still happen; read the signal's current value
```

The answer is the result, not an acknowledgement of receipt:

| `result_code` | `result.outcome` | Meaning |
|---|---|---|
| 200 | `applied` | written and read back; `result.value` is the value read |
| 409 | `failed` | written, but the value read back differs (`result.value`, `result.requested`) |
| 502 | `failed` | the source refused the write |
| 503 | `failed` | the source is not connected, or the tag is not available there |
| 504 | `unknown` | written, but reading it back failed |
| 501 | `unsupported` | the driver cannot write |
| 422 | `refused` | the tag is read-only, the command carries no value, or a malformed `operation_id`/`on_behalf_of` |
| 409 | `refused` | the `operation_id` was already used for a different write |
| 403 | `refused` | a person sent a write on behalf of someone else |
| 498 | | the command expired before it ran; nothing was written |

The `_Ack` repeats `operation_id` and `on_behalf_of` when the write carried
them.

**Once per operation.** A write sent with an `operation_id` is recorded on
disk under the attested sender and that id before the driver runs. A repeat
(an HTTP request retried by a browser, a resend after a timeout) gets a new
`correlation_id` but the same `operation_id`; it is answered with the recorded
outcome and `"replayed": true`, and the source is not written again. A repeat
of a write that was started and never answered (the connector restarted
mid-write) is answered `504`. A repeat gets whatever the first execution
answered, a failure included; a new attempt after a failure takes a new
`operation_id`. A write that expired (`498`) was not executed and is not
recorded. The record is kept seven days. Every command
executor does this, `@on_command` handlers included.

**Who.** The node attests the sender: it authenticated the session and stamps
that identity on the stored command, and the executor reads it from there.
`on_behalf_of` is not attested by the node. It is the sender's claim, exactly
as trustworthy as the sender, so grant `cmd` only to services that
authenticate the people they act for, and show it as "anna via hmi-api". A
person cannot send a command on behalf of someone else (`403`).

**Taking over a write.** `ConnectorService.handle_signal_write(write)` runs
every write once the connector checked the binding and the tag; its default
is `await write.apply()`. Override it to check the sender, to write a value
derived from the command (a recipe of several values on one signal), or to
add to the answer with a `chaski.CommandResult` or a `chaski.CommandRejected`:

```python
from chaski import CommandRejected, CommandResult, ConnectorService

class PressConnector(ConnectorService):
    async def handle_signal_write(self, write):
        if write.command.sender.label != "hmi-api":
            raise CommandRejected(403, "refused: only the HMI writes this machine", {"outcome": "refused"})
        values = write.command.params.get("values")   # sent with write_signal(..., params={"values": ...})
        journal_id = self.journal.start(write.command)  # who, for whom, which operation
        try:
            answer = await write.apply(value=encode_recipe(values))
        except CommandRejected as rejected:
            raise CommandRejected(rejected.code, rejected.message, {**rejected.result, "journal": journal_id}) from None
        return CommandResult(answer.message, {**answer.result, "journal": journal_id})
```

A write for a signal on another node takes `node=` and the path as the
sender's node sees it, like any command, and waits there while that node is
unreachable, within its lifetime. The connector records a write on disk
before it starts: restarted between the write and its answer, it answers
`504` (outcome unknown) and does not write again.

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

An output can say what it is: `SignalOutput("availability", "float",
"share of planned time running", unit="%", semantic_type="availability")`.
The unit, semantic type (a semantic tag the node knows) and description travel
in the output's catalogue entry; the node applies them to the signal it binds
and follows later changes. An output keeps its tag id across restarts.

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
node's attestation, not the sender's claim. The stream's growth, watched for
the executor's command contracts and `_Ack`, wakes the drain, so its own
answers never stay unread on its cursor. An expired command (`expires_at`,
unix ms) is answered `498` without running the handler. A command without
`expires_at` never expires and may arrive long after it was sent, when this
node was cut off from the sender's: a handler whose effect must not happen
late checks `command.ts` itself or relies on its senders setting a lifetime.
A handler may return `CommandResult(message, result)` (from `chaski.dataops`)
instead of a string; the `_Ack` carries `result`.
`CommandRejected` answers its own code, any other exception `500`: that
answer is the command's durable rejection, and the command is not retried.

A handler runs only while the executor's broker link is up. While it is down
the executor waits, until the command's deadline at most; a command that
expires meanwhile is answered `498` without running. Inside a handler, a
write after the deadline or while the link is down is refused
(`chaski.NotSent`) instead of queued for after a reconnect, and the
command is answered `500`. A command the handler sends (`Service.command`)
expires, and is waited for, by the same deadline; with no time left it is not
sent and raises `NotSent`. `chaski.write_deadline()` returns the deadline
(unix seconds), `None` outside a handler.

A handler whose write got no PUBACK (`franzmq.errors.PublishTimeout`, also as
the cause of what it raised) is answered `504`, outcome unknown: the MQTT
client keeps the write queued and may deliver it after a reconnect, so `500`
("nothing changed") would be false. A sender that gets `504` reads the state
back before it relies on either outcome.

Each command runs once and gets one answer. The answer is recorded in the
service's buffer before it is published, and the command before its handler
runs. A failed `_Ack` publish is sent again once the broker link is back, with
jittered backoff of at most 5 s, and the cursor passes the command only after
the node confirmed its answer. The same answer can therefore arrive twice; it
is identical both times. After a restart a recorded answer is published again
unchanged, a command that was started but not answered is answered `504`, and a
command whose `_Ack` is already in the `commands` stream is skipped.

Every background task of a `DataOpsService` (ingest, command executor,
re-resolution, clock-driven ticks) runs supervised: an exception is logged at
once, reported in `handler_health` (degraded, then unhealthy) and the task is
restarted with backoff. The MQTT client reconnects at most 5 s (jittered) after
the broker is back.

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

### Optional application time

`Clock()` uses host time without requiring NTP. `Clock(source="mqtt")` uses
Colca's existing live `_TimeSync` beacon. Host/NTP and MQTT corrections are
alternatives, never added together. `Clock.from_env()` reads
`APPLICATION_TIME_SOURCE` (`local` by default), `FACTORY_CLOCK_TOPIC` (an exact
`_ClockDefinition` topic), and `TIME_SYNC_MAX_AGE_S` (120 real seconds).

Pass the clock to `Service`, `ConnectorService` or `DataOpsService`. A selected
factory definition supplies the epoch, rate, pause and stop boundary. Missing
or stale authority suspends application work; health, reconnects, credentials
and network deadlines continue in real time. No OS clock is changed. Source
readings can provide an optional timestamp with `Reading`; acquisition time
remains the connector default.

Periodic DataOps callbacks execute every due factory tick in order and persist
their progress. Inputs normally use application time; declare
`SignalRangeInput(..., time_domain="real")` for infrastructure heartbeat inputs.
Output timestamps stay in application time when a real-time heartbeat triggers
computation.

For coordinated simulation, pass `step_dependencies=[...]` to each service.
An empty list identifies a source worker; omitting the argument keeps normal
continuous operation. Dependencies are exact `_ServiceDetails` topics or local
service names such as `./temperature-source` (resolved from service records,
independent of placement). The deployment helper `dependencies_from_env()` in
`chaski.coordination` reads the optional JSON `FACTORY_STEP_DEPENDENCIES` list.

The controller grants a bounded window with `ClockDefinition.stop_at`. A worker
awaits `service.step.wait_ready()`, completes the work, commits its effects, then
calls `service.step.complete(boundary)`. Connectors wait for their sources,
acquire and publish the boundary sample, then acknowledge it; DataOps drains
upstream samples and due callbacks before acknowledging. Failed reads, publishes
or processing cannot acknowledge a completed window. The controller must wait
for **all explicitly required workers** before extending the boundary. This
makes the requested rate a ceiling, with sample resolution determined by the
window length. It does not make slow hardware run faster.

Completion positions survive restarts. Callbacks must be idempotent because a
crash between committing an effect and recording completion can replay it.
Missing, inactive, stale or wrong-run dependency progress holds the window.
`service.report_progress(processed_at)` is available for continuous workers;
report committed work, never the target clock. Its liveness heartbeat uses real
time and continues while a run is paused.

The execution gate and clock waits wake on MQTT changes rather than checking on
an interval. `clock.sleep_until()` schedules a real timer for the factory deadline
and reschedules it when time or configuration changes. Synchronous workers can
capture `step.changes.version`, check `step.ready()`, then call
`step.changes.wait(version, step.wait_delay(1.0))`. Capturing the version first
prevents a completion arriving between the check and wait from being lost. The
optional maximum timeout is for shutdown/health/retry housekeeping; it does not
pace normal work. After setting a thread's stop event, notify `clock.changes` to
wake it immediately. Async waits support normal task cancellation.


### Durable publication and build compatibility

`Service.publish()` persists samples that are waiting for a signal binding under
`state_dir/pending-samples.sqlite3`. Keep `state_dir` on a persistent volume.
Publication resumes on binding/restart, removes samples only after broker
acknowledgement, and raises `BufferError` at its per-path limit. Replay after a
lost reply can repeat a sample; consumers must remain idempotent. Signals
explicitly marked unpublished suppress their queued samples.

Scoped watches, queue telemetry and embedded-node lifecycle events require
Colca 0.20 or later. Upgrade the SDK and broker as a tested pair; the `node`
extra constrains the supported broker version range for embedded deployments.


### Consumer failure and producer recovery

`Stream.drain()` captures a finite head, processes and acknowledges complete pages,
and returns even if writers remain active. The final page may include newer
records. `Stream.follow()` subscribes before draining and coalesces notifications;
it does not poll. Pass a stop event for cancellation during a page. A partially
consumed page remains unacknowledged. With an external doorbell, ring it after
setting stop to wake an idle follower.

A failed handler is never acknowledged. This holds for `@on_metric`,
`@on_constant`, `@on_signal`, factory-time `@every`/`@cron` callbacks,
the replay after a producer's code changed, and `Service.consume()`:

- The cursor moves only past the records before the failed one, so a restart
  resumes at it. No replay watermark is recorded for failed processing.
- The same input is retried with bounded, jittered backoff (1 s doubling to
  30 s). An `@on_constant`/`@on_signal` retry ends early when a newer record at
  the same topic replaces the failed one. A wall-clock `@every`/`@cron` tick has
  no input; its next tick is the retry.
- Each failure is logged with its traceback and counted per handler in
  `svc.handler_health`. One failure makes the service `degraded`; five in a row
  (`HandlerHealth(unhealthy_after=...)`) make it `unhealthy`. The first success
  clears the count. The DataOps health door reports `handlers` and `failing`
  and answers 503 when unhealthy; `_ServiceDetails` turns unhealthy too,
  combined with what `svc.status()` was told.

To pass an input on purpose, raise `chaski.Reject(reason, detail=...)` from the
handler. The runner records the rejection durably, as the service's retained
`rejected_input` `_Finding` (PUBACK awaited; each rejection is also a record on
the `entities` stream), and acknowledges the input only after that. A rejection
that cannot be recorded counts as a failure. `svc.clear_rejections()` retires
the finding once the inputs were dealt with.

The replay after a code change works the same way. A rejected buffered record
is recorded and the replay goes on. Any other failure leaves the producer's
watermark and code hash unwritten; the service stays up, reports the `replay`
task degraded, then unhealthy, and retries the replay with backoff before
intake starts. A `@every`/`@cron` callback that raises `Reject` has its tick
recorded and passed.

```python
@on_metric("panel_edge")
async def on_edge(self, metric) -> None:
    if metric.value is None:
        raise chaski.Reject("edge without a value", detail={"ts": metric.timestamp})
    self.tracker.push(metric)          # may raise: retried, never skipped
```

A plain stream consumer gets the same through `svc.consume(stream, handler,
bell=bell, stop=stop)`. `@on_command` handlers keep their answer: a failure is
answered `500` (`504` when a write is unconfirmed) in the command's `_Ack`,
which the sender reads, and the command is not run again.

Handlers must be idempotent, because a retry runs the same input again.
Remove `strict=False` if you previously passed it.

An `on_metric` producer with in-memory state can set `state_version = 1` and
implement `snapshot_state()` and `restore_state(state)`. Snapshots must be JSON
values and must include all state needed to continue its metric handlers. Restore
must replace state without publishing. Chaski saves snapshots plus per-handler
input offsets before acknowledgement, restores matching code/version checkpoints
at startup, and skips already checkpointed redelivery. Missing or incompatible
checkpoints reconstruct from retained inputs; provision sufficient history for
that reconstruction. A retained input window cannot recover an arbitrarily old
open interval, so preserve the buffer volume.

Effects must still be idempotent: a crash after publication but before saving a
checkpoint can repeat them. Chaski does not promise a transaction spanning a
remote system and local SQLite. Chaski saves a checkpoint only after metric handlers.
A `@every`/`@cron`, `@on_constant` or `@on_command` callback that changes
checkpointed state saves it itself, after the change:

```python
from chaski.dataops import every, save_checkpoint

@every("5m")
async def flush(self) -> None:
    self.publish_pending()
    self.pending.clear()
    save_checkpoint(self)    # nothing for a producer without state_version
```

Existing producers with external recovery may keep their own implementation and
leave `state_version` unset. Do not enable both recovery owners for the same state.

The CI suite runs broker-backed reconnect and retained-view recovery tests as well
as finite-drain, cancellation, failed-handler, checkpoint and redelivery tests.
