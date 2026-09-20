using messaging_lab.orders;
using messaging_lab.solace.fw.subscribe;
using messaging_lab.solace.subscriber.Configuration;
using messaging_lab.solace.subscriber.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace messaging_lab.solace.subscriber.Orders;

public sealed class OrderHandler(
    ILogger<OrderHandler> logger,
    ThroughputTracker metrics,
    OrderingValidator orderingValidator,
    IOptions<SubscriberOptions> options) : IMessageHandler<OrderPlaced>
{
    public bool Handle(OrderPlaced message)
    {
        SimulateWork();

        var latency = DateTime.UtcNow - message.PublishedAtUtc;

        if (!orderingValidator.RecordAndCheck(message.OrderId, message.Sequence))
        {
            metrics.RecordOrderingViolation();
            logger.LogWarning(
                "Out-of-order message for {OrderId}: sequence {Sequence} handled after a later one", message.OrderId, message.Sequence);
        }

        // This process's own OrderingValidator above only ever sees the subset of keys/sequences
        // *this* process received, so it can't detect reordering across multiple subscriber
        // processes on a non-partitioned queue (see README > Comparing multiple subscribers on a
        // non-partitioned queue). This line lets an external tool reconstruct the true global
        // handling order by merging every instance's log by timestamp and checking it there instead.
        logger.LogInformation("HANDLED OrderId={OrderId:l} Sequence={Sequence}", message.OrderId, message.Sequence);

        metrics.RecordHandled(latency);
        return true;
    }

    void SimulateWork()
    {
        var max = options.Value.SimulatedHandlerWorkMaxMs;
        if (max <= 0) return;

        var min = Math.Min(options.Value.SimulatedHandlerWorkMinMs, max);
        var delayMs = min == max ? max : Random.Shared.Next(min, max + 1);
        if (delayMs > 0) Thread.Sleep(delayMs);
    }
}
