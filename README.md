# messaging-lab

A learning project for exploring [Solace PubSub+](https://solace.com/) messaging concepts (contexts, sessions, guaranteed-delivery flows, topics and queues) through the official [`SolaceSystems.Solclient.Messaging`](https://www.nuget.org/packages/SolaceSystems.Solclient.Messaging) .NET SDK.

Rather than calling the native SDK directly everywhere, this repo builds a small, transport-agnostic messaging abstraction (serialize → publish, subscribe → deserialize → handle) and backs it with Solace-specific adapters, so the Solace-specific concerns (contexts, sessions, flows, acknowledgement, destination lifecycles) are isolated and can be studied/extended in one place.

## Requirements

- .NET SDK targeting `net10.0`
- Access to a Solace PubSub+ broker (a local Docker broker or a free [Solace Cloud](https://console.solace.cloud/) instance both work) with a message VPN, and a queue for anything you want to consume with `SolaceConcurrentSubscriber<T>` / `SolaceSequentialSubscriber<T>`

## Build

```
dotnet build messaging-lab.slnx
```

## Project layout

- `messaging-lab.solace.fw/` — the library.
  - `serialization/` — `IMessageSerializer<T>` / `IMessageDeserializer<T>`, implemented with `System.Text.Json` (`JsonMessageSerializer<T>`, `JsonMessageDeserializer<T>`).
  - `publish/` — `IMessageSender<T>` / `IMessagePublisher<T>`, implemented by `SolaceMessageSender<T>` (serializes and publishes to a fixed destination, retrying on a full publisher window up to a configurable timeout) and `SolacePublisher<T>` (resolves a topic from settings and delegates to a sender).
  - `subscribe/` — `IMessageHandler<T>` / `IMessageSubscriber`, implemented by `SolaceConcurrentSubscriber<T>` (channel + worker pool, optional ordering via a key selector) and `SolaceSequentialSubscriber<T>` (single-threaded baseline: deserializes, handles, and acks inline on the delivery callback). Both bind a client-acknowledged guaranteed-delivery flow to a queue.
  - `SolaceMessagingEnvironment`, `SolaceContext`, `SolaceSession` — thin lifecycle wrappers around the native `ContextFactory` / `IContext` / `ISession`.
- `messaging-lab.orders/` — the `OrderPlaced` message contract (`OrderId`, `Total`, a per-key `Sequence`, `PublishedAtUtc`) shared by the two console apps below.
- `messaging-lab.solace.loadgen/` — a console app that publishes `OrderPlaced` test messages to a topic via `SolacePublisher<T>`, round-robin across a configurable number of keys with a strictly increasing per-key `Sequence`.
- `messaging-lab.solace.subscriber/` — a console app that binds either `SolaceConcurrentSubscriber<T>` or `SolaceSequentialSubscriber<T>` (config-driven) and reports throughput, end-to-end latency (p50/p99), and per-key ordering violations, so the two subscriber types can be compared directly. See [Comparing the subscribers](#comparing-the-subscribers) below.

Both console apps log to the console and to a rolling daily file under `logs/` (see [Logging](#logging) below).

See `CLAUDE.md` for a deeper architectural walkthrough (interface/adapter split, concurrency model, disposal ownership).

## Example

```csharp
using messaging_lab.solace.fw;
using messaging_lab.solace.fw.publish;
using messaging_lab.solace.fw.serialization;
using messaging_lab.solace.fw.subscribe;
using SolaceSystems.Solclient.Messaging;

record OrderPlaced(string OrderId, decimal Total);

record OrderTopicSettings : IMessagePublisherSettings
{
    public string Topic => "orders/placed";
}

record OrderQueueSettings : IMessageSubscriberSettings
{
    public string Queue => "orders-queue";
}

class OrderHandler : IMessageHandler<OrderPlaced>
{
    public bool Handle(OrderPlaced message)
    {
        Console.WriteLine($"Order {message.OrderId}: {message.Total:C}");
        return true; // false (or a thrown exception) leaves the message unacked for redelivery
    }
}

using var context = new SolaceContext();
using var session = context.CreateSession(new SessionProperties
{
    Host = "tcp://localhost:55555",
    VPNName = "default",
    UserName = "default",
});
session.Connect();

using var publisher = new SolacePublisher<OrderPlaced>(
    session, new OrderTopicSettings(), new JsonMessageSerializer<OrderPlaced>(), MessageDeliveryMode.Persistent);
using var subscriber = new SolaceConcurrentSubscriber<OrderPlaced>(session, new OrderQueueSettings(), new JsonMessageDeserializer<OrderPlaced>(), new OrderHandler());

subscriber.Subscribe();
publisher.Publish(new OrderPlaced("1001", 42.50m));
```

> The queue only receives what's published to the topic if it has a matching topic subscription configured on the broker (e.g. `orders-queue` subscribed to `orders/placed`), and the publisher needs a guaranteed delivery mode (`Persistent`/`NonPersistent`, not the default `Direct`) for messages to be queued at all.

## Comparing the subscribers

`messaging-lab.solace.loadgen` and `messaging-lab.solace.subscriber` together give you a runnable harness for comparing `SolaceConcurrentSubscriber<T>` against `SolaceSequentialSubscriber<T>` under identical load, instead of just reasoning about them.

1. On the broker, make sure the subscriber's queue (`Subscriber:Queue` in its `appsettings.json`, default `CHANGED`) has a topic subscription to the topic the load generator publishes to (`LoadGen:Topic`, default `data-changed`) - otherwise published messages never reach the queue.
2. Publish a batch of test messages:
   ```
   dotnet run --project messaging-lab.solace.loadgen -- --LoadGen:Count 50000 --LoadGen:KeyCount 16
   ```
   Messages are spread round-robin across `KeyCount` keys (`OrderId`s), each carrying a strictly increasing per-key `Sequence` and a `PublishedAtUtc` timestamp.
3. Run the subscriber to consume them:
   ```
   dotnet run --project messaging-lab.solace.subscriber
   ```
   Its `appsettings.json` controls the comparison:
   - `UseConcurrentSubscriber` - `true` binds `SolaceConcurrentSubscriber<T>` (with a key selector, so same-key messages stay in order across worker lanes), `false` binds `SolaceSequentialSubscriber<T>`.
   - `Concurrency` - worker/lane count for the concurrent subscriber.
   - `SimulatedHandlerWorkMinMs` / `SimulatedHandlerWorkMaxMs` - a uniform-random per-message delay the handler blocks for, simulating real work (e.g. an external call). With trivial handler work there's nothing for concurrency to overlap, so both subscribers process messages at roughly the same rate - a non-zero delay here is what makes the comparison meaningful.
   - `MetricsReportIntervalSeconds` - how often it logs a snapshot: messages handled, elapsed time, msgs/sec, ordering violations (a per-key sequence check, catching any message handled out of delivery order), and p50/p99 end-to-end latency.

Run the same load through each `UseConcurrentSubscriber` setting to compare throughput directly.

Or automate the whole sweep with `scripts/run-benchmark.ps1`: it publishes a batch, drains it through the subscriber, and repeats across the sequential baseline plus a set of concurrency levels (`-Concurrencies 1,2,4,8` by default), polling the broker to detect when each configuration has fully drained before moving to the next. It writes a self-contained HTML report to `reports/`.

```
./scripts/run-benchmark.ps1 -Count 1000
```

### Latest benchmark results

[`reports/benchmark-20260905-170759.html`](reports/benchmark-20260905-170759.html) - 1000 messages across 16 keys per configuration, `SimulatedHandlerWorkMinMs`/`MaxMs` at 250/1000ms:

| Configuration | Elapsed | Throughput | Speedup | p50 | p99 |
|---|---|---|---|---|---|
| Sequential | 627.9s | 1.6/s | 1.00x | 317.5s | 624.2s |
| Concurrent, n=1 | 637.7s | 1.6/s | 1.00x | 326.5s | 634.5s |
| Concurrent, n=2 | 316.0s | 3.2/s | 2.00x | 161.9s | 314.9s |
| Concurrent, n=4 | 235.7s | 4.2/s | 2.62x | 93.2s | 232.4s |
| Concurrent, n=8 | 193.9s | 5.2/s | 3.25x | 88.3s | 190.5s |

All five runs handled all 1000 messages with **0 ordering violations** (5,000 messages total). `n=1` tracks the sequential baseline almost exactly, as expected. Scaling flattens past `n=4` (2.62x -> 3.25x for a doubling of lanes, versus 2.00x -> 2.62x from `n=2` to `n=4`) - with only 16 keys hashed across 8 lanes, that's consistent with an uneven key-to-lane distribution rather than a real ceiling in the subscriber itself.

The report also has an addendum: a separate, later run at `n=32` (this machine's logical CPU count) with `KeyCount` raised to 64 - 16 keys can't exercise 32 lanes, since a key selector never routes one key to more than one lane. That run hit **24.5 msgs/sec** (40.82s to drain, 0 violations) - roughly 15.3x the sequential baseline and 4.7x the `n=8` result above, though it isn't a controlled comparison against the sweep (different `KeyCount`, different time), which is why it's kept as a separate section in the report rather than a sixth row in the table.

### Scaling past CPU count

[`reports/benchmark-20260905-173219.html`](reports/benchmark-20260905-173219.html) - a follow-up sweep at `KeyCount=100`, comparing the sequential baseline against `n = 32` (this machine's logical CPU count), `n = 62`, and `n = 93`:

| Configuration | Elapsed | Throughput | Speedup | p50 | p99 |
|---|---|---|---|---|---|
| Sequential | 630.0s | 1.6/s | 1.00x | 310.9s | 625.5s |
| Concurrent, n=32 | 50.5s | 19.8/s | 12.38x | 22.6s | 48.0s |
| Concurrent, n=62 | 30.3s | 33.1/s | 20.69x | 16.1s | 30.4s |
| Concurrent, n=93 | 20.8s | 48.1/s | 30.06x | 11.3s | 21.5s |

All four runs handled all 1000 messages with **0 ordering violations** (4,000 messages total). Scaling stays close to linear well past the CPU count - `n=32` -> `n=62` (1.94x more lanes) is 1.67x more throughput (~86% of ideal), and `n=62` -> `n=93` (1.5x more lanes) is 1.45x more throughput (~84% of ideal) - unlike the flattening seen between `n=4` and `n=8` in the sweep above.

This run also fixed a real confound: `OrderHandler`'s simulated work uses `Thread.Sleep`, a blocking call, so each lane occupies a real .NET ThreadPool worker thread for the full delay. The ThreadPool's default minimum is `Environment.ProcessorCount`, growing beyond that only via a throttled injection algorithm (roughly one new thread per ~0.5-1s under sustained starvation) - so `n=62`/`n=93` wouldn't have run at their full requested concurrency without help. `messaging-lab.solace.subscriber`'s `Program.cs` now calls `ThreadPool.SetMinThreads` sized to the configured `Concurrency` before constructing the subscriber, so a lane count actually gets that much real parallelism from the start.

On whether the specific lane counts (`numberOfCPUs`, `(numberOfCPUs-1)*2`, `(numberOfCPUs-1)*3`) are a meaningful ladder: not particularly. That "reserve one core, scale by a multiple" convention comes from tuning CPU-bound thread pools, and this handler is I/O-shaped (its `Thread.Sleep` stands in for an external call), where lanes aren't pinned to cores and there's no compute to saturate - so there's no physical reason CPU count should be a ceiling here. A more defensible ladder would be tied to something that actually constrains this system: the flow's `WindowSize` (split across lanes, so `laneCapacity = WindowSize / concurrency` floors at 1 once `concurrency` exceeds it), `KeyCount` (a lane with no assigned keys does nothing, which is what broke the earlier `n=32`/`KeyCount=16` addendum run before it was corrected), or a real downstream capacity limit if one were being modeled.

## Logging

Both `messaging-lab.solace.loadgen` and `messaging-lab.solace.subscriber` log to the console and to a rolling daily file (via Serilog) next to the built executable - `logs/loadgen-YYYYMMDD.log` and `logs/subscriber-YYYYMMDD.log` respectively, retaining the last 14 days - so a run's output survives after the process exits and can be reviewed for anomalies later.

`SolaceConcurrentSubscriber<T>` and `SolaceSequentialSubscriber<T>` (in `messaging-lab.solace.fw`) each take an optional `ILogger<T>`. When supplied, a message that fails to deserialize or whose handler throws logs a warning with the exception and queue name before being left unacked for redelivery, instead of failing silently.
