using System.Collections.Concurrent;

namespace messaging_lab.solace.subscriber.Metrics;

/// <summary>Distinguishes an exact re-handle of an already-seen sequence from a genuine reorder.</summary>
public enum OrderingOutcome
{
    InOrder,

    /// <summary>Same sequence handled twice for this key - e.g. a partition handoff redelivered a message this
    /// process (or another one, on a non-partitioned queue) had already handled but not yet acked in time.</summary>
    Duplicate,

    /// <summary>A sequence lower than the highest already seen for this key, but not an exact repeat.</summary>
    OutOfOrder,
}

/// <summary>
/// Tracks the highest sequence number handled per key and classifies any message whose sequence
/// doesn't strictly increase relative to the last one seen for that key.
/// </summary>
public sealed class OrderingValidator
{
    readonly ConcurrentDictionary<string, long> _lastSeenSequence = new();

    public OrderingOutcome RecordAndCheck(string key, long sequence)
    {
        var outcome = OrderingOutcome.InOrder;

        _lastSeenSequence.AddOrUpdate(
            key,
            _ => sequence,
            (_, last) =>
            {
                if (sequence == last) outcome = OrderingOutcome.Duplicate;
                else if (sequence < last) outcome = OrderingOutcome.OutOfOrder;
                return Math.Max(last, sequence);
            });

        return outcome;
    }
}
