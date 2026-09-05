using messaging_lab.solace.fw;
using messaging_lab.solace.fw.subscribe;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace messaging_lab.solace.subscriber;

/// <summary>
/// Starts the configured <see cref="IMessageSubscriber"/> for the lifetime of the host
/// (the <see cref="SolaceSession"/> is already connected by the time this runs - see its
/// registration in Program.cs) and unsubscribes/disconnects on shutdown.
/// </summary>
public sealed class SubscriberHostedService(
    SolaceSession session,
    IMessageSubscriber subscriber,
    ILogger<SubscriberHostedService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        subscriber.Subscribe();
        logger.LogInformation("Subscriber started.");

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
        finally
        {
            subscriber.Unsubscribe();
            session.Disconnect();
            logger.LogInformation("Subscriber stopped.");
        }
    }
}
