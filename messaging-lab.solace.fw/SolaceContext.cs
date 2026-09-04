using SolaceSystems.Solclient.Messaging;

namespace messaging_lab.solace.fw;

/// <summary>
/// Thin lifecycle wrapper around an <see cref="IContext"/>. Ensures the process-wide
/// Solace API is initialized and acts as a factory for <see cref="SolaceSession"/> instances.
/// </summary>
public sealed class SolaceContext : IDisposable
{
    readonly IContext _context;
    bool _disposed;

    public SolaceContext(ContextProperties? properties = null, ContextFactoryProperties? factoryProperties = null)
    {
        SolaceMessagingEnvironment.EnsureInitialized(factoryProperties);
        _context = ContextFactory.Instance.CreateContext(
            properties ?? new ContextProperties(),
            (sender, args) => ContextEventOccurred?.Invoke(sender, args));
    }

    public IContext Native => _context;

    public event EventHandler<ContextEventArgs>? ContextEventOccurred;

    public SolaceSession CreateSession(SessionProperties properties)
    {
        SolaceSession? wrapper = null;

        var session = _context.CreateSession(
            properties,
            (sender, args) => wrapper!.RaiseMessageReceived(sender!, args),
            (sender, args) => wrapper!.RaiseSessionEvent(sender!, args));

        wrapper = new SolaceSession(session);
        return wrapper;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _context.Dispose();
    }
}
