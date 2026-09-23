using System.Text;
using System.Threading.Channels;
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
/// only hands each message off to a channel and returns immediately; the actual deserialize/handle/ack
/// work happens on background worker tasks.
/// </p>
/// <p>
/// Without an <see cref="IMessageKeySelector{T}"/>, <paramref name="concurrency"/> workers all pull from
/// one shared channel, so two messages may be handled concurrently and complete in either order - fine for
/// independent messages, wrong if two messages describe the same record. Supplying a key selector routes
/// same-key messages to the same one of <paramref name="concurrency"/> lanes, each drained in order by a
/// single worker, so same-key messages are always handled in delivery order while different keys still run
/// in parallel across lanes.
/// </p>
/// <p>
/// See <see cref="SolaceSequentialSubscriber{T}"/> for a single-threaded alternative that deserializes,
/// handles, and acks each message inline on the delivery callback - no channels, no worker tasks.
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
    readonly Channel<(IMessage Message, int Generation)> _ingress;
    readonly Channel<(IMessage Message, T Payload, int Generation)>[]? _lanes;
    readonly Task[] _workers;
    readonly Task? _router;
    bool _disposed;

    // Bumped once per FlowInactive (the flow losing every partition it had - see OnFlowEvent). Each
    // message is tagged with the generation in effect when it was received; a lane/unordered worker
    // drops (never calls the handler on) anything tagged older than the *current* generation instead
    // of handling it, since FlowInactive means whatever's already buffered belongs to a partition
    // this flow no longer owns - it's either already been resent to the new owner or about to be, so
    // handling it here too would just be a duplicate. Comparing generations at dequeue time (rather
    // than pausing and later resuming on FlowActive) matters because a plain "are we active right
    // now" check would wrongly let stale, pre-inactive messages through once FlowActive fires again,
    // if they're still sitting in a channel behind messages that arrived after reactivation.
    int _generation;

    public SolaceConcurrentSubscriber(
        SolaceSession session,
        IMessageSubscriberSettings settings,
        IMessageDeserializer<T> deserializer,
        IMessageHandler<T> handler,
        IMessageKeySelector<T>? keySelector = null,
        int concurrency = 4,
        int? windowSize = null,
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
            ActiveFlowInd = true,
        };
        if (windowSize is int ws)
        {
            flowProperties.WindowSize = ws;
        }

        _flow = session.Native.CreateFlow(flowProperties, _queue, null, OnMessageReceived, OnFlowEvent);

        _ingress = Channel.CreateBounded<(IMessage, int)>(new BoundedChannelOptions(flowProperties.WindowSize)
        {
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });

        if (_keySelector is null)
        {
            _router = null;
            _lanes = null;
            _workers = Enumerable.Range(0, concurrency).Select(_ => Task.Run(RunUnorderedWorkerAsync)).ToArray();
        }
        else
        {
            var laneCapacity = Math.Max(1, flowProperties.WindowSize / concurrency);
            _lanes =
            [
                .. Enumerable.Range(0, concurrency)
                    .Select(_ => Channel.CreateBounded<(IMessage, T, int)>(new BoundedChannelOptions(laneCapacity)
                    {
                        SingleWriter = true,
                        SingleReader = true,
                        FullMode = BoundedChannelFullMode.Wait,
                    }))
            ];

            _router = Task.Run(RunRouterAsync);
            _workers = _lanes.Select(lane => Task.Run(() => RunLaneWorkerAsync(lane))).ToArray();
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

    void OnMessageReceived(object? sender, MessageEventArgs args)
    {
        var generation = Volatile.Read(ref _generation);
        _ingress.Writer.WriteAsync((args.Message, generation)).AsTask().GetAwaiter().GetResult();
    }

    // FlowInactive means the broker has taken every partition away from this flow (not just some -
    // see the field comment on _generation); bumping the generation here is what makes every message
    // already buffered at that moment get dropped instead of handled, wherever it currently sits.
    void OnFlowEvent(object? sender, FlowEventArgs args)
    {
        switch (args.Event)
        {
            case FlowEvent.FlowActive:
                _logger?.LogInformation(
                    "Flow for queue '{Queue}' is active (holds at least one partition).", ((IEndpoint)_queue).Name);
                break;
            case FlowEvent.FlowInactive:
                var generation = Interlocked.Increment(ref _generation);
                _logger?.LogWarning(
                    "Flow for queue '{Queue}' is inactive (holds no partitions); generation bumped to {Generation} - anything already buffered will be dropped instead of handled.",
                    ((IEndpoint)_queue).Name, generation);
                break;
        }
    }

    void LogDropped(int messageGeneration) =>
        _logger?.LogInformation(
            "Dropped a buffered message on queue '{Queue}' (generation {MessageGeneration} predates current {CurrentGeneration}) - it was received before the flow's most recent FlowInactive, so it's likely already been (or is about to be) redelivered to whichever consumer this partition was reassigned to.",
            ((IEndpoint)_queue).Name, messageGeneration, Volatile.Read(ref _generation));

    // Deserializes and keys each message (in delivery order) and hands it to the lane its key maps to.
    async Task RunRouterAsync()
    {
        await foreach (var (message, generation) in _ingress.Reader.ReadAllAsync())
        {
            if (generation != Volatile.Read(ref _generation))
            {
                // Buffered before the most recent FlowInactive; drop rather than risk handling it
                // again on top of whichever consumer it's since been (or is about to be) resent to.
                LogDropped(generation);
                message.Dispose();
                continue;
            }

            try
            {
                var json = Encoding.UTF8.GetString(message.BinaryAttachment ?? []);
                var payload = _deserializer.Deserialize(json);
                var lane = _lanes![unchecked((uint)_keySelector!.GetKey(payload).GetHashCode()) % (uint)_lanes.Length];
                await lane.Writer.WriteAsync((message, payload, generation));
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
            lane.Writer.TryComplete();
        }
    }

    async Task RunLaneWorkerAsync(Channel<(IMessage Message, T Payload, int Generation)> lane)
    {
        await foreach (var (message, payload, generation) in lane.Reader.ReadAllAsync())
        {
            try
            {
                if (generation != Volatile.Read(ref _generation))
                {
                    LogDropped(generation);
                    message.Dispose();
                    continue;
                }

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

    async Task RunUnorderedWorkerAsync()
    {
        await foreach (var (message, generation) in _ingress.Reader.ReadAllAsync())
        {
            try
            {
                if (generation != Volatile.Read(ref _generation))
                {
                    LogDropped(generation);
                    message.Dispose();
                    continue;
                }

                var json = Encoding.UTF8.GetString(message.BinaryAttachment ?? []);
                var payload = _deserializer.Deserialize(json);

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
                // This message faulted; keep draining the rest of the channel.
                _logger?.LogWarning(ex, "Failed to deserialize or handle a message on queue '{Queue}'; leaving unacked for redelivery.", ((IEndpoint)_queue).Name);
            }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _ingress.Writer.TryComplete();
        _router?.Wait();
        Task.WaitAll(_workers);
        _flow.Dispose();
        _queue.Dispose();
    }
}
