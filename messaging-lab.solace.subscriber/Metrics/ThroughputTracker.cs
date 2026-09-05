using System.Collections.Concurrent;
using System.Diagnostics;

namespace messaging_lab.solace.subscriber.Metrics;

/// <summary>
/// Thread-safe counters for comparing SolaceConcurrentSubscriber/SolaceSequentialSubscriber:
/// messages handled, ordering violations, and per-message end-to-end latency (publish to handle).
/// </summary>
public sealed class ThroughputTracker
{
    long _count;
    long _orderingViolations;
    long _startTimestamp;
    long _lastTimestamp;
    readonly ConcurrentQueue<double> _latenciesMs = new();

    public void RecordHandled(TimeSpan latency)
    {
        var now = Stopwatch.GetTimestamp();
        Interlocked.CompareExchange(ref _startTimestamp, now, 0);
        Interlocked.Exchange(ref _lastTimestamp, now);
        Interlocked.Increment(ref _count);
        _latenciesMs.Enqueue(latency.TotalMilliseconds);
    }

    public void RecordOrderingViolation() => Interlocked.Increment(ref _orderingViolations);

    public ThroughputSnapshot Snapshot()
    {
        var count = Interlocked.Read(ref _count);
        var violations = Interlocked.Read(ref _orderingViolations);
        var start = Interlocked.Read(ref _startTimestamp);
        var last = Interlocked.Read(ref _lastTimestamp);
        var elapsed = start == 0 ? TimeSpan.Zero : Stopwatch.GetElapsedTime(start, last);

        var latencies = _latenciesMs.ToArray();
        Array.Sort(latencies);

        return new ThroughputSnapshot(count, violations, elapsed, Percentile(latencies, 0.50), Percentile(latencies, 0.99));
    }

    static double Percentile(double[] sortedValues, double percentile)
    {
        if (sortedValues.Length == 0) return 0;
        var index = (int)Math.Clamp(Math.Round(percentile * (sortedValues.Length - 1)), 0, sortedValues.Length - 1);
        return sortedValues[index];
    }
}

public readonly record struct ThroughputSnapshot(
    long Count,
    long OrderingViolations,
    TimeSpan Elapsed,
    double P50LatencyMs,
    double P99LatencyMs)
{
    public double MessagesPerSecond => Elapsed.TotalSeconds > 0 ? Count / Elapsed.TotalSeconds : 0;
}
