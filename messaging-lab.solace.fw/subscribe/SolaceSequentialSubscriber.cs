using System.Text;
using messaging_lab.solace.fw.serialization;
using Microsoft.Extensions.Logging;
using SolaceSystems.Solclient.Messaging;

namespace messaging_lab.solace.fw.subscribe;

/// <summary>
/// Binds a guaranteed-delivery flow to the queue named in <see cref="IMessageSubscriberSettings"/> and,
/// on the flow's own delivery callback, deserializes each message from JSON, calls the
/// <see cref="IMessageHandler{T}"/> synchronously, and acks when it returns true - one message at a time,
/// in delivery order, with no channels or worker tasks.
/// <p>
/// This intentionally does the deserialize/handle/ack work directly on the Solace context's delivery
/// thread, which the native SDK otherwise recommends against blocking. It exists as a simple baseline
/// to compare against <see cref="SolaceConcurrentSubscriber{T}"/>; prefer that type for real workloads.
/// </p>
/// </summary>
public sealed class SolaceSequentialSubscriber<T> : IMessageSubscriber, IDisposable
{
    readonly IQueue _queue;
    readonly IFlow _flow;
    readonly IMessageDeserializer<T> _deserializer;
    readonly IMessageHandler<T> _handler;
    readonly ILogger<SolaceSequentialSubscriber<T>>? _logger;
    bool _disposed;

    public SolaceSequentialSubscriber(
        SolaceSession session,
        IMessageSubscriberSettings settings,
        IMessageDeserializer<T> deserializer,
        IMessageHandler<T> handler,
        ILogger<SolaceSequentialSubscriber<T>>? logger = null)
    {
        _deserializer = deserializer;
        _handler = handler;
        _logger = logger;

        _queue = ContextFactory.Instance.CreateQueue(settings.Queue);
        var flowProperties = new FlowProperties
        {
            AckMode = MessageAckMode.ClientAck,
            FlowStartState = false,
        };

        _flow = session.Native.CreateFlow(flowProperties, _queue, null, OnMessageReceived, (_, _) => { });
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
        using var message = args.Message;

        try
        {
            var json = Encoding.UTF8.GetString(message.BinaryAttachment ?? []);
            var payload = _deserializer.Deserialize(json);

            if (_handler.Handle(payload))
            {
                _flow.Ack(message.ADMessageId);
            }
        }
        catch (Exception ex)
        {
            // Malformed message or handler failure; leave unacked for redelivery.
            _logger?.LogWarning(ex, "Failed to deserialize or handle a message on queue '{Queue}'; leaving unacked for redelivery.", ((IEndpoint)_queue).Name);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _flow.Dispose();
        _queue.Dispose();
    }
}
