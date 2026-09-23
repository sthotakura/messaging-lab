namespace messaging_lab.solace.subscriber.Configuration;

public sealed class SubscriberOptions
{
    public required string Queue { get; init; }

    /// <summary>
    /// Optional identifier for this process, used to keep multiple concurrently-running subscriber
    /// instances (bound to the same partitioned queue) from colliding on one log file and to
    /// attribute metrics/log lines to a specific instance. Unset (null) for a single-instance run.
    /// </summary>
    public string? InstanceId { get; init; }

    /// <summary>
    /// True to bind <see cref="messaging_lab.solace.fw.subscribe.SolaceConcurrentSubscriber{T}"/>,
    /// false to bind <see cref="messaging_lab.solace.fw.subscribe.SolaceSequentialSubscriber{T}"/>.
    /// </summary>
    public bool UseConcurrentSubscriber { get; init; } = true;

    /// <summary>Worker/lane count passed to <see cref="messaging_lab.solace.fw.subscribe.SolaceConcurrentSubscriber{T}"/>; ignored otherwise.</summary>
    public int Concurrency { get; init; } = 4;

    /// <summary>
    /// Flow control window (max messages the broker will have delivered-but-unacked to this flow at
    /// once) passed to <see cref="messaging_lab.solace.fw.subscribe.SolaceConcurrentSubscriber{T}"/>;
    /// ignored otherwise. Null (the default) leaves the SDK's own default (255) in place. Valid range
    /// is 1-255. Shrinking this bounds how many messages can be caught in a disturbed partition handoff
    /// at once, at the cost of how far the broker can read ahead of this flow's actual processing rate.
    /// </summary>
    public int? WindowSize { get; init; }

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

    /// <summary>
    /// If set, this instance deliberately disconnects its own Solace session this many seconds after
    /// starting, simulating a network blip or crash on an otherwise-healthy, still-running process.
    /// This exists because Solace's own rebalance never displaces an already-active partition owner
    /// just because more consumers join (see run-benchmark-mid-drain-rebalance.ps1's notes) - a real
    /// session drop is the only way to reach <c>FlowEvent.FlowInactive</c> on an instance that was
    /// genuinely active, so this is what <see cref="NetworkBlipSimulatorService"/> uses to reproduce
    /// that from a single process. Null (the default) disables it.
    /// </summary>
    public int? SimulateBlipAfterSeconds { get; init; }

    /// <summary>How long the simulated blip in <see cref="SimulateBlipAfterSeconds"/> lasts before reconnecting.</summary>
    public int SimulateBlipDurationSeconds { get; init; } = 5;
}
