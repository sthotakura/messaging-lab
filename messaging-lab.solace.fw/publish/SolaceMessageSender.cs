using System.Text;
using messaging_lab.solace.fw.serialization;
using SolaceSystems.Solclient.Messaging;

namespace messaging_lab.solace.fw.publish;

/// <summary>
/// Transport-level sender: serializes a message to JSON and publishes it as the binary
/// attachment of a Solace message to a fixed destination over a session.
/// </summary>
public sealed class SolaceMessageSender<T>(
    SolaceSession session,
    IDestination destination,
    IMessageSerializer<T> serializer,
    MessageDeliveryMode deliveryMode = MessageDeliveryMode.Direct)
    : IMessageSender<T>, IDisposable
{
    bool _disposed;

    public void Send(T message)
    {
        var json = serializer.Serialize(message);

        using var solaceMessage = ContextFactory.Instance.CreateMessage();
        solaceMessage.Destination = destination;
        solaceMessage.DeliveryMode = deliveryMode;
        solaceMessage.BinaryAttachment = Encoding.UTF8.GetBytes(json);

        var returnCode = session.Send(solaceMessage);
        if (returnCode != ReturnCode.SOLCLIENT_OK)
        {
            throw new InvalidOperationException($"Failed to send message to '{destination.Name}': {returnCode}");
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        destination.Dispose();
    }
}
