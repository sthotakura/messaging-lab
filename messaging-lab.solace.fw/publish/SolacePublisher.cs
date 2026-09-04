using messaging_lab.solace.fw.serialization;
using SolaceSystems.Solclient.Messaging;

namespace messaging_lab.solace.fw.publish;

/// <summary>
/// High-level publisher facade: resolves the destination topic from settings and delegates
/// the actual serialize-and-send work to an <see cref="IMessageSender{T}"/>.
/// </summary>
public sealed class SolacePublisher<T>(IMessageSender<T> sender) : IMessagePublisher<T>, IDisposable
{
    bool _disposed;

    public SolacePublisher(
        SolaceSession session,
        IMessagePublisherSettings settings,
        IMessageSerializer<T> serializer,
        MessageDeliveryMode deliveryMode = MessageDeliveryMode.Direct)
        : this(new SolaceMessageSender<T>(session, ContextFactory.Instance.CreateTopic(settings.Topic), serializer, deliveryMode))
    {
    }

    public void Publish(T message) => sender.Send(message);

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        (sender as IDisposable)?.Dispose();
    }
}
