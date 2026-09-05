namespace messaging_lab.solace.subscriber.Configuration;

public sealed class SubscriberOptions
{
    public required string Queue { get; init; }

    /// <summary>
    /// True to bind <see cref="messaging_lab.solace.fw.subscribe.SolaceConcurrentSubscriber{T}"/>,
    /// false to bind <see cref="messaging_lab.solace.fw.subscribe.SolaceSequentialSubscriber{T}"/>.
    /// </summary>
    public bool UseConcurrentSubscriber { get; init; } = true;

    /// <summary>Worker/lane count passed to <see cref="messaging_lab.solace.fw.subscribe.SolaceConcurrentSubscriber{T}"/>; ignored otherwise.</summary>
    public int Concurrency { get; init; } = 4;

    /// <summary>How often <see cref="Metrics.MetricsReportingService"/> logs a throughput/ordering/latency snapshot.</summary>
    public int MetricsReportIntervalSeconds { get; init; } = 5;

    /// <summary>
    /// Simulated per-message work (e.g. an external call) that <see cref="Orders.OrderHandler"/> blocks for
    /// before returning - a uniform random duration between this and <see cref="SimulatedHandlerWorkMaxMs"/>.
    /// Zero (the default) disables it. Without some non-trivial handler cost, the concurrent and sequential
    /// subscribers process trivial work at roughly the same rate, since there's nothing for concurrency to
    /// overlap - this is what makes a throughput comparison between them meaningful.
    /// </summary>
    public int SimulatedHandlerWorkMinMs { get; init; } = 0;

    /// <summary>See <see cref="SimulatedHandlerWorkMinMs"/>.</summary>
    public int SimulatedHandlerWorkMaxMs { get; init; } = 0;
}
