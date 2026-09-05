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

## Porting `SolaceConcurrentSubscriber<T>` to .NET Framework 4.8

This repo targets `net10.0`, but nothing in `SolaceConcurrentSubscriber<T>` (`messaging-lab.solace.fw/subscribe/SolaceConcurrentSubscriber.cs`) is actually tied to modern .NET - the class body would port to `net48` essentially unchanged. Every real adaptation happens in the `.csproj`, not the class:

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <PropertyGroup>
        <TargetFramework>net48</TargetFramework>
        <RootNamespace>messaging_lab.solace.fw</RootNamespace>
        <LangVersion>12.0</LangVersion>
        <ImplicitUsings>enable</ImplicitUsings>
        <Nullable>enable</Nullable>
    </PropertyGroup>

    <ItemGroup>
      <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" Version="10.0.11" />
      <PackageReference Include="SolaceSystems.Solclient.Messaging" Version="10.30.0" />
      <PackageReference Include="System.Threading.Channels" Version="9.0.0" />
    </ItemGroup>

</Project>
```

- **`<LangVersion>` must be set explicitly.** The .NET SDK defaults `LangVersion` by target framework, and for anything below `net5.0` that default is C# 7.3 - too old for the file-scoped `namespace messaging_lab.solace.fw.subscribe;`, the null-forgiving `_keySelector!`/`_lanes!` operators, and the `?? []` collection-expression default this class already uses. Bumping `LangVersion` to 12 (or `latest`) is a compiler-only setting, independent of the target framework, and unlocks all of it on `net48` too. `ImplicitUsings` and `Nullable` are likewise pure compile-time features (annotations and warnings, not IL), so both keep working on `net48` without change.
- **`System.Threading.Channels` needs an explicit `PackageReference`.** It ships in the BCL from `net5.0` on, but the NuGet package multi-targets down to `net461`, so it's still a drop-in for `net48` - same `Channel.CreateBounded`, same `BoundedChannelOptions`, same `ChannelReader<T>.ReadAllAsync()`. Its `net461` target transitively pulls in `Microsoft.Bcl.AsyncInterfaces` (for `IAsyncEnumerable<T>`, which is what makes the `await foreach (... in _ingress.Reader.ReadAllAsync())` loops in `RunRouterAsync`/`RunLaneWorkerAsync`/`RunUnorderedWorkerAsync` compile) and `System.Threading.Tasks.Extensions` (for `ValueTask`, used internally by the channel APIs) - neither needs to be referenced directly.
- **`SolaceSystems.Solclient.Messaging` and `Microsoft.Extensions.Logging.Abstractions`** already multi-target `net48`/`net461` alongside modern TFMs, so both `PackageReference`s carry over as-is - the native Solace SDK's original support surface was .NET Framework, so if anything this direction is the well-trodden one.
- **Elsewhere in the library, not in this file:** any `record` types (e.g. the `OrderPlaced`/`*Settings` records from the [Example](#example) above) use `init`-only setters, which the compiler backs with `System.Runtime.CompilerServices.IsExternalInit` - present in `net5.0`+ but missing from `net48`'s mscorlib. A five-line polyfill (a private, empty `IsExternalInit` class in that same namespace) is the standard fix and is only needed once per assembly.

With those project-level pieces in place, `SolaceConcurrentSubscriber.cs` itself needs zero logic changes: the bounded ingress `Channel<IMessage>`, the router/lane split for keyed ordering, the per-message try/catch around deserialize+handle+ack, and `Dispose`'s drain-then-`Task.WaitAll` shutdown all behave identically on `net48`. The one runtime nuance worth carrying over from [Scaling past CPU count](#scaling-past-cpu-count) is that `net48`'s `ThreadPool` has the same throttled thread-injection behavior as `net10.0` - a lane count run with blocking handler work still wants the same `ThreadPool.SetMinThreads` warm-up before constructing the subscriber.

### Without `System.Threading.Channels`: a `BlockingCollection<T>` version

If `System.Threading.Channels` isn't an option at all (an internal package-approval policy, a wish to avoid any async infrastructure on `net48`), the whole class can be rebuilt on `System.Collections.Concurrent.BlockingCollection<T>` instead. It's been in mscorlib since .NET Framework 4.0, so this version needs **no extra NuGet package** for the queueing itself - only `Microsoft.Extensions.Logging.Abstractions` and `SolaceSystems.Solclient.Messaging` from the original `PackageReference` list. `LangVersion` still needs the same explicit bump described above (tuple deconstruction in `foreach`, `using var`, the null-forgiving operator, and file-scoped namespaces are unchanged), but nothing here needs `IAsyncEnumerable<T>` or `ValueTask`, because nothing here is `async` - `BlockingCollection<T>.GetConsumingEnumerable()` blocks the calling thread instead of yielding, so the router and workers become plain synchronous methods run on dedicated, long-running tasks rather than `async` ones pulled from the pool:

```csharp
using System.Collections.Concurrent;
using System.Text;
using messaging_lab.solace.fw.serialization;
using Microsoft.Extensions.Logging;
using SolaceSystems.Solclient.Messaging;

namespace messaging_lab.solace.fw.subscribe;

/// <summary>
/// Binds a guaranteed-delivery flow to the queue named in <see cref="IMessageSubscriberSettings"/>,
/// deserializes each delivered message from JSON, and dispatches it to an <see cref="IMessageHandler{T}"/>.
/// The flow uses client acknowledgement: a message is only acked when the handler returns true,
/// so a false result (or a thrown exception) leaves it eligible for redelivery.
/// <p>
/// The Solace context drives message delivery from a single thread, so <c>OnMessageReceived</c>
/// only hands each message off to a <see cref="BlockingCollection{T}"/> and returns immediately; the
/// actual deserialize/handle/ack work happens on dedicated background threads (long-running tasks, not
/// pooled ones, since each spends its life blocked in <c>GetConsumingEnumerable()</c> rather than yielding).
/// </p>
/// <p>
/// Without an <see cref="IMessageKeySelector{T}"/>, <paramref name="concurrency"/> workers all pull from
/// one shared collection, so two messages may be handled concurrently and complete in either order - fine for
/// independent messages, wrong if two messages describe the same record. Supplying a key selector routes
/// same-key messages to the same one of <paramref name="concurrency"/> lanes, each drained in order by a
/// single worker, so same-key messages are always handled in delivery order while different keys still run
/// in parallel across lanes. <see cref="BlockingCollection{T}"/>'s default backing store is a
/// <see cref="ConcurrentQueue{T}"/>, so both the shared collection and each lane stay FIFO.
/// </p>
/// </summary>
public sealed class SolaceConcurrentSubscriber<T> : IMessageSubscriber, IDisposable
{
    readonly IQueue _queue;
    readonly IFlow _flow;
    readonly IMessageDeserializer<T> _deserializer;
    readonly IMessageHandler<T> _handler;
    readonly IMessageKeySelector<T>? _keySelector;
    readonly ILogger<SolaceConcurrentSubscriber<T>>? _logger;
    readonly BlockingCollection<IMessage> _ingress;
    readonly BlockingCollection<(IMessage Message, T Payload)>[]? _lanes;
    readonly Task[] _workers;
    readonly Task? _router;
    bool _disposed;

    public SolaceConcurrentSubscriber(
        SolaceSession session,
        IMessageSubscriberSettings settings,
        IMessageDeserializer<T> deserializer,
        IMessageHandler<T> handler,
        IMessageKeySelector<T>? keySelector = null,
        int concurrency = 4,
        ILogger<SolaceConcurrentSubscriber<T>>? logger = null)
    {
        _deserializer = deserializer;
        _handler = handler;
        _keySelector = keySelector;
        _logger = logger;

        _queue = ContextFactory.Instance.CreateQueue(settings.Queue);
        var flowProperties = new FlowProperties
        {
            AckMode = MessageAckMode.ClientAck,
            FlowStartState = false,
        };

        _flow = session.Native.CreateFlow(flowProperties, _queue, null, OnMessageReceived, (_, _) => { });

        _ingress = new BlockingCollection<IMessage>(boundedCapacity: flowProperties.WindowSize);

        if (_keySelector is null)
        {
            _router = null;
            _lanes = null;
            _workers = Enumerable.Range(0, concurrency)
                .Select(_ => Task.Factory.StartNew(RunUnorderedWorker, TaskCreationOptions.LongRunning))
                .ToArray();
        }
        else
        {
            var laneCapacity = Math.Max(1, flowProperties.WindowSize / concurrency);
            _lanes =
            [
                .. Enumerable.Range(0, concurrency)
                    .Select(_ => new BlockingCollection<(IMessage, T)>(boundedCapacity: laneCapacity))
            ];

            _router = Task.Factory.StartNew(RunRouter, TaskCreationOptions.LongRunning);
            _workers = _lanes
                .Select(lane => Task.Factory.StartNew(() => RunLaneWorker(lane), TaskCreationOptions.LongRunning))
                .ToArray();
        }
    }

    public IFlow Native => _flow;

    public void Subscribe()
    {
        var returnCode = _flow.Start();
        if (returnCode != ReturnCode.SOLCLIENT_OK)
        {
            throw new InvalidOperationException($"Failed to start flow for queue '{_flow.GetEndpoint().Name}': {returnCode}");
        }
    }

    public void Unsubscribe()
    {
        var returnCode = _flow.Stop();
        if (returnCode != ReturnCode.SOLCLIENT_OK)
        {
            throw new InvalidOperationException($"Failed to stop flow for queue '{_flow.GetEndpoint().Name}': {returnCode}");
        }
    }

    // Blocks the Solace context thread when the ingress collection is at capacity - same
    // backpressure the channel-based version gets from FullMode.Wait.
    void OnMessageReceived(object? sender, MessageEventArgs args) => _ingress.Add(args.Message);

    // Deserializes and keys each message (in delivery order) and hands it to the lane its key maps to.
    void RunRouter()
    {
        foreach (var message in _ingress.GetConsumingEnumerable())
        {
            try
            {
                var json = Encoding.UTF8.GetString(message.BinaryAttachment ?? Array.Empty<byte>());
                var payload = _deserializer.Deserialize(json);
                var lane = _lanes![unchecked((uint)_keySelector!.GetKey(payload).GetHashCode()) % (uint)_lanes.Length];
                lane.Add((message, payload));
            }
            catch (Exception ex)
            {
                // Malformed message or key extraction failure; leave unacked for redelivery.
                _logger?.LogWarning(ex, "Failed to deserialize or key a message on queue '{Queue}'; leaving unacked for redelivery.", ((IEndpoint)_queue).Name);
                message.Dispose();
            }
        }

        foreach (var lane in _lanes!)
        {
            lane.CompleteAdding();
        }
    }

    void RunLaneWorker(BlockingCollection<(IMessage Message, T Payload)> lane)
    {
        foreach (var (message, payload) in lane.GetConsumingEnumerable())
        {
            try
            {
                using (message)
                {
                    if (_handler.Handle(payload))
                    {
                        _flow.Ack(message.ADMessageId);
                    }
                }
            }
            catch (Exception ex)
            {
                // This message faulted; keep draining the rest of the lane in order.
                _logger?.LogWarning(ex, "Handler faulted for a message on queue '{Queue}'; leaving unacked for redelivery.", ((IEndpoint)_queue).Name);
            }
        }
    }

    void RunUnorderedWorker()
    {
        foreach (var message in _ingress.GetConsumingEnumerable())
        {
            try
            {
                using (message)
                {
                    var json = Encoding.UTF8.GetString(message.BinaryAttachment ?? Array.Empty<byte>());
                    var payload = _deserializer.Deserialize(json);

                    if (_handler.Handle(payload))
                    {
                        _flow.Ack(message.ADMessageId);
                    }
                }
            }
            catch (Exception ex)
            {
                // This message faulted; keep draining the rest of the collection.
                _logger?.LogWarning(ex, "Failed to deserialize or handle a message on queue '{Queue}'; leaving unacked for redelivery.", ((IEndpoint)_queue).Name);
            }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _ingress.CompleteAdding();
        _router?.Wait();
        Task.WaitAll(_workers);

        _ingress.Dispose();
        if (_lanes is not null)
        {
            foreach (var lane in _lanes) lane.Dispose();
        }

        _flow.Dispose();
        _queue.Dispose();
    }
}
```

The shape is identical to the original - same constructor signature, same router/lane split for keyed ordering, same per-message try/catch, same disposal sequence - only the primitive changed. Two behavioral differences worth calling out:

- **`TaskCreationOptions.LongRunning` matters here in a way it didn't for the `Channel`-based version.** An `async` worker awaiting a channel reader doesn't occupy a thread while it's waiting for the next message - it yields back to the pool. A `BlockingCollection<T>` worker's `GetConsumingEnumerable()` physically blocks its thread until a message arrives, so running `concurrency` (or `concurrency` lane) workers via plain `Task.Run` would tie up that many `ThreadPool` threads indefinitely, competing with the pool's own throttled growth (see [Scaling past CPU count](#scaling-past-cpu-count)). `LongRunning` tells the scheduler to give each worker its own dedicated thread instead of a pool thread.
- **`OnMessageReceived` blocks the Solace context thread identically to the `Channel` version** (`_ingress.Add` blocks at capacity just like `WriteAsync(...).GetAwaiter().GetResult()` did) - no change in backpressure behavior, just a different API to get there.

## Logging

Both `messaging-lab.solace.loadgen` and `messaging-lab.solace.subscriber` log to the console and to a rolling daily file (via Serilog) next to the built executable - `logs/loadgen-YYYYMMDD.log` and `logs/subscriber-YYYYMMDD.log` respectively, retaining the last 14 days - so a run's output survives after the process exits and can be reviewed for anomalies later.

`SolaceConcurrentSubscriber<T>` and `SolaceSequentialSubscriber<T>` (in `messaging-lab.solace.fw`) each take an optional `ILogger<T>`. When supplied, a message that fails to deserialize or whose handler throws logs a warning with the exception and queue name before being left unacked for redelivery, instead of failing silently.
