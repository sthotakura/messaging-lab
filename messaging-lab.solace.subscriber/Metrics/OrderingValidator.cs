using System.Collections.Concurrent;

namespace messaging_lab.solace.subscriber.Metrics;

/// <summary>
/// Tracks the highest sequence number handled per key and flags any message whose sequence
/// doesn't strictly increase relative to the last one seen for that key - i.e. it was handled
/// out of delivery order.
/// </summary>
public sealed class OrderingValidator
{
    readonly ConcurrentDictionary<string, long> _lastSeenSequence = new();

    public bool RecordAndCheck(string key, long sequence)
    {
        var inOrder = true;

        _lastSeenSequence.AddOrUpdate(
            key,
            _ => sequence,
            (_, last) =>
            {
                if (sequence <= last) inOrder = false;
                return Math.Max(last, sequence);
            });

        return inOrder;
    }
}
