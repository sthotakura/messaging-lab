using SolaceSystems.Solclient.Messaging;

namespace messaging_lab.solace.fw;

/// <summary>
/// Guards the process-wide ContextFactory Init/Cleanup calls required by the native Solace client.
/// </summary>
public static class SolaceMessagingEnvironment
{
    static readonly object Lock = new();
    static bool _initialized;

    public static void EnsureInitialized(ContextFactoryProperties? properties = null)
    {
        if (_initialized) return;

        lock (Lock)
        {
            if (_initialized) return;

            ContextFactory.Instance.Init(properties ?? new ContextFactoryProperties());
            _initialized = true;
        }
    }

    public static void Cleanup()
    {
        lock (Lock)
        {
            if (!_initialized) return;

            ContextFactory.Instance.Cleanup();
            _initialized = false;
        }
    }
}
