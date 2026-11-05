namespace messaging_lab.solace.fanout.Configuration;

public sealed class FanoutOptions
{
    public required string Topic { get; init; }

    /// <summary>Target publish rate. A fixed rate keeps the time-to-full predictable from the queue's quota.</summary>
    public int RatePerSecond { get; init; } = 500;

    public int DurationSeconds { get; init; } = 60;

    /// <summary>Padding added to each message so a small spool quota fills in seconds rather than minutes.</summary>
    public int PayloadBytes { get; init; } = 1024;

    /// <summary>How long to wait for outstanding ACKs/rejections after the last send before disconnecting.</summary>
    public int SettleTimeoutSeconds { get; init; } = 15;

    /// <summary>
    /// If set, per-second counters, the rejected sequence numbers, and a run summary are written here
    /// for scripts/run-benchmark-fanout-spool-quota.ps1 to analyse.
    /// </summary>
    public string? OutputDirectory { get; init; }
}
