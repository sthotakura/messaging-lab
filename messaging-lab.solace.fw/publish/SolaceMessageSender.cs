using System.Diagnostics;
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
    MessageDeliveryMode deliveryMode = MessageDeliveryMode.Direct,
    TimeSpan? sendTimeout = null)
    : IMessageSender<T>, IDisposable
{
    static readonly TimeSpan DefaultSendTimeout = TimeSpan.FromSeconds(10);

    readonly TimeSpan _sendTimeout = sendTimeout ?? DefaultSendTimeout;
    bool _disposed;

    public void Send(T message)
    {
        var json = serializer.Serialize(message);

        using var solaceMessage = ContextFactory.Instance.CreateMessage();
        solaceMessage.Destination = destination;
        solaceMessage.DeliveryMode = deliveryMode;
        solaceMessage.BinaryAttachment = Encoding.UTF8.GetBytes(json);

        // A full publisher window returns WOULD_BLOCK rather than blocking; retry until it
        // drains, bounded by _sendTimeout so a stalled session/broker can't hang the caller forever.
        var stopwatch = Stopwatch.StartNew();
        ReturnCode returnCode;
        while ((returnCode = session.Send(solaceMessage)) == ReturnCode.SOLCLIENT_WOULD_BLOCK)
        {
            if (stopwatch.Elapsed >= _sendTimeout)
            {
                throw new TimeoutException(
                    $"Timed out after {_sendTimeout} waiting for the publisher window to drain for '{destination.Name}'.");
            }

            Thread.Sleep(1);
        }

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
