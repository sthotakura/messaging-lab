using messaging_lab.solace.fw;
using messaging_lab.solace.fw.subscribe;
using messaging_lab.solace.subscriber.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SolaceSystems.Solclient.Messaging;

namespace messaging_lab.solace.subscriber;

/// <summary>
/// Testing-only chaos knob, active only when <see cref="SubscriberOptions.SimulateBlipAfterSeconds"/>
/// is set: deliberately disconnects and reconnects this instance's Solace session mid-run, simulating
/// a network blip or crash-and-restart on an otherwise-healthy, still-running process.
/// <p>
/// This exists because Solace's rebalance never displaces an already-active partition owner just
/// because more consumers join - a real session drop is the only way to reach
/// <see cref="FlowEvent.FlowInactive"/> on an instance that genuinely held a partition, and this is
/// the only way to reproduce that from a single benchmark process without an OS-level network control
/// or a disruptive queue partition-count change (which disconnects every bound client).
/// </p>
/// </summary>
public sealed class NetworkBlipSimulatorService(
    SolaceSession session,
    IMessageSubscriber subscriber,
    IOptions<SubscriberOptions> options,
    ILogger<NetworkBlipSimulatorService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = options.Value;
        if (opts.SimulateBlipAfterSeconds is not int after) return;

        try
        {
            await Task.Delay(TimeSpan.FromSeconds(after), stoppingToken);

            logger.LogWarning("Simulating a network blip: disconnecting the session for {DurationS}s.", opts.SimulateBlipDurationSeconds);
            session.Disconnect();

            await Task.Delay(TimeSpan.FromSeconds(opts.SimulateBlipDurationSeconds), stoppingToken);

            logger.LogWarning("Simulated blip over: reconnecting.");
            var connectResult = session.Connect();
            if (connectResult != ReturnCode.SOLCLIENT_OK)
            {
                logger.LogError("Failed to reconnect after simulated blip: {ReturnCode}", connectResult);
                return;
            }

            try
            {
                // The flow was already Start()-ed once; this asserts it again in case the SDK needs an
                // explicit nudge to rebind on the reconnected session rather than doing so on its own.
                subscriber.Subscribe();
                logger.LogInformation("Re-subscribed after simulated blip.");
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Re-Subscribe() after simulated blip threw - checking whether the flow rebinds without it.");
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown raced the simulated blip; nothing to do.
        }
    }
}
