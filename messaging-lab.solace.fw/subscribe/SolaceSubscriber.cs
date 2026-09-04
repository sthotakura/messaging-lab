using System.Text;
using System.Threading.Channels;
using messaging_lab.solace.fw.serialization;
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
/// </summary>
public sealed class SolaceSubscriber<T> : IMessageSubscriber, IDisposable
{
    readonly IQueue _queue;
    readonly IFlow _flow;
    readonly IMessageDeserializer<T> _deserializer;
    readonly IMessageHandler<T> _handler;
    readonly IMessageKeySelector<T>? _keySelector;
    readonly Channel<IMessage> _ingress;
    readonly Channel<(IMessage Message, T Payload)>[]? _lanes;
    readonly Task[] _workers;
    readonly Task? _router;
    bool _disposed;

    public SolaceSubscriber(
        SolaceSession session,
        IMessageSubscriberSettings settings,
        IMessageDeserializer<T> deserializer,
        IMessageHandler<T> handler,
        IMessageKeySelector<T>? keySelector = null,
        int concurrency = 4)
    {
        _deserializer = deserializer;
        _handler = handler;
        _keySelector = keySelector;

        _queue = ContextFactory.Instance.CreateQueue(settings.Queue);
        var flowProperties = new FlowProperties
        {
            AckMode = MessageAckMode.ClientAck,
            FlowStartState = false,
        };

        _flow = session.Native.CreateFlow(flowProperties, _queue, null, OnMessageReceived, null);

        _ingress = Channel.CreateBounded<IMessage>(new BoundedChannelOptions(flowProperties.WindowSize)
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
            _lanes = Enumerable.Range(0, concurrency)
                .Select(_ => Channel.CreateBounded<(IMessage, T)>(new BoundedChannelOptions(laneCapacity)
                {
                    SingleWriter = true,
                    SingleReader = true,
                    FullMode = BoundedChannelFullMode.Wait,
                }))
                .ToArray();

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

    void OnMessageReceived(object? sender, MessageEventArgs args) =>
        _ingress.Writer.WriteAsync(args.Message).AsTask().GetAwaiter().GetResult();

    // Deserializes and keys each message (in delivery order) and hands it to the lane its key maps to.
    async Task RunRouterAsync()
    {
        await foreach (var message in _ingress.Reader.ReadAllAsync())
        {
            try
            {
                var json = Encoding.UTF8.GetString(message.BinaryAttachment ?? []);
                var payload = _deserializer.Deserialize(json);
                var lane = _lanes![unchecked((uint)_keySelector!.GetKey(payload).GetHashCode()) % (uint)_lanes.Length];
                await lane.Writer.WriteAsync((message, payload));
            }
            catch
            {
                // Malformed message or key extraction failure; leave unacked for redelivery.
                message.Dispose();
            }
        }

        foreach (var lane in _lanes!)
        {
            lane.Writer.TryComplete();
        }
    }

    async Task RunLaneWorkerAsync(Channel<(IMessage Message, T Payload)> lane)
    {
        await foreach (var (message, payload) in lane.Reader.ReadAllAsync())
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
            catch
            {
                // This message faulted; keep draining the rest of the lane in order.
            }
        }
    }

    async Task RunUnorderedWorkerAsync()
    {
        await foreach (var message in _ingress.Reader.ReadAllAsync())
        {
            try
            {
                using (message)
                {
                    var json = Encoding.UTF8.GetString(message.BinaryAttachment ?? []);
                    var payload = _deserializer.Deserialize(json);

                    if (_handler.Handle(payload))
                    {
                        _flow.Ack(message.ADMessageId);
                    }
                }
            }
            catch
            {
                // This message faulted; keep draining the rest of the channel.
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
