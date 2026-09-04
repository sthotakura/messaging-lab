using System.Text;
using System.Threading.Channels;
using messaging_lab.solace.fw.serialization;
using SolaceSystems.Solclient.Messaging;

namespace messaging_lab.solace.fw.subscribe;

/// <summary>
/// Binds a guaranteed-delivery flow to the queue named in <see cref="IMessageSubscriberSettings"/>,
/// deserializes each delivered message from JSON, and dispatches it to an <see cref="IMessageHandler{T}"/>.
/// The flow uses client acknowledgement: a message is only acked when the handler returns true,
/// so a false result leaves it eligible for redelivery.
/// <p>
/// The Solace context drives message delivery from a single thread, so <c>OnMessageReceived</c>
/// only hands each message off to a bounded channel and returns immediately; a pool of worker
/// tasks drains the channel concurrently, which is what actually runs the deserializer and handler.
/// </p>
/// </summary>
public sealed class SolaceSubscriber<T> : IMessageSubscriber, IDisposable
{
    readonly IQueue _queue;
    readonly IFlow _flow;
    readonly IMessageDeserializer<T> _deserializer;
    readonly IMessageHandler<T> _handler;
    readonly Channel<IMessage> _channel;
    readonly Task[] _workers;
    bool _disposed;

    public SolaceSubscriber(
        SolaceSession session,
        IMessageSubscriberSettings settings,
        IMessageDeserializer<T> deserializer,
        IMessageHandler<T> handler,
        int concurrency = 4)
    {
        _deserializer = deserializer;
        _handler = handler;

        _queue = ContextFactory.Instance.CreateQueue(settings.Queue);
        var flowProperties = new FlowProperties
        {
            AckMode = MessageAckMode.ClientAck,
            FlowStartState = false,
        };

        _flow = session.Native.CreateFlow(flowProperties, _queue, null, OnMessageReceived, null);

        _channel = Channel.CreateBounded<IMessage>(new BoundedChannelOptions(flowProperties.WindowSize)
        {
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });

        _workers = [.. Enumerable.Range(0, concurrency).Select(_ => Task.Run(RunWorkerAsync))];
    }

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
        _channel.Writer.WriteAsync(args.Message).AsTask().GetAwaiter().GetResult();

    async Task RunWorkerAsync()
    {
        while (!_channel.Reader.Completion.IsCompleted)
        {
            try
            {
                await ProcessAsync();
            }
            catch
            {
                // A single message faulted this iteration; restart and keep draining the channel.
            }
        }
    }

    async Task ProcessAsync()
    {
        await foreach (var message in _channel.Reader.ReadAllAsync())
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
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _channel.Writer.TryComplete();
        Task.WaitAll(_workers);
        _flow.Dispose();
        _queue.Dispose();
    }
}
