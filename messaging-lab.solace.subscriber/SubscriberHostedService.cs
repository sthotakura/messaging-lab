using messaging_lab.solace.fw;
using messaging_lab.solace.fw.subscribe;
using messaging_lab.solace.subscriber.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace messaging_lab.solace.subscriber;

/// <summary>
/// Starts the configured <see cref="IMessageSubscriber"/> for the lifetime of the host
/// (the <see cref="SolaceSession"/> is already connected by the time this runs - see its
/// registration in Program.cs) and unsubscribes/disconnects on shutdown.
/// </summary>
public sealed class SubscriberHostedService(
    SolaceSession session,
    IMessageSubscriber subscriber,
    IOptions<SubscriberOptions> options,
    ILogger<SubscriberHostedService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        subscriber.Subscribe();
        var subscriberOptions = options.Value;
        logger.LogInformation(
            "Subscriber started (instance {InstanceId}, queue '{Queue}', {Kind}).",
            subscriberOptions.InstanceId ?? "-",
            subscriberOptions.Queue,
            subscriberOptions.UseConcurrentSubscriber ? $"concurrent, {subscriberOptions.Concurrency} lanes" : "sequential");

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
