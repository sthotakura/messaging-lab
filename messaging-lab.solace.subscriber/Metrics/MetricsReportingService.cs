using messaging_lab.solace.subscriber.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace messaging_lab.solace.subscriber.Metrics;

/// <summary>
/// Logs a throughput/ordering/latency snapshot on a fixed interval, plus a final one on shutdown,
/// so SolaceConcurrentSubscriber and SolaceSequentialSubscriber runs can be compared directly.
/// </summary>
public sealed class MetricsReportingService(
    ThroughputTracker metrics,
    IOptions<SubscriberOptions> options,
    ILogger<MetricsReportingService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = TimeSpan.FromSeconds(options.Value.MetricsReportIntervalSeconds);

        try
        {
            while (true)
            {
                await Task.Delay(interval, stoppingToken);
                Report();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
        finally
        {
            Report();
        }
    }

    void Report()
    {
        var snapshot = metrics.Snapshot();
        logger.LogInformation(
            "Handled {Count} messages in {Elapsed:g} ({Rate:F1} msgs/sec) - ordering violations: {Violations}, latency p50={P50:F1}ms p99={P99:F1}ms",
            snapshot.Count,
            snapshot.Elapsed,
            snapshot.MessagesPerSecond,
            snapshot.OrderingViolations,
            snapshot.P50LatencyMs,
            snapshot.P99LatencyMs);
    }
}
