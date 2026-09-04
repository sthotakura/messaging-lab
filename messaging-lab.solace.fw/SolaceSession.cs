using SolaceSystems.Solclient.Messaging;

namespace messaging_lab.solace.fw;

/// <summary>
/// Thin lifecycle wrapper around an <see cref="ISession"/>, re-exposing received messages
/// and session events as ordinary .NET events so handlers can be attached after creation.
/// </summary>
public sealed class SolaceSession : IDisposable
{
    readonly ISession _session;
    bool _disposed;

    internal SolaceSession(ISession session)
    {
        _session = session;
    }

    public ISession Native => _session;

    public event EventHandler<MessageEventArgs>? MessageReceived;
    public event EventHandler<SessionEventArgs>? SessionEventOccurred;

    internal void RaiseMessageReceived(object sender, MessageEventArgs args) =>
        MessageReceived?.Invoke(sender, args);

    internal void RaiseSessionEvent(object sender, SessionEventArgs args) =>
        SessionEventOccurred?.Invoke(sender, args);

    public ReturnCode Connect() => _session.Connect();

    public ReturnCode Disconnect() => _session.Disconnect();

    public ReturnCode Send(IMessage message) => _session.Send(message);

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _session.Dispose();
    }
}
