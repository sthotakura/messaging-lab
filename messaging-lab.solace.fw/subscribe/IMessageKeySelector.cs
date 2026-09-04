namespace messaging_lab.solace.fw.subscribe;

/// <summary>
/// Derives an ordering key from a deserialized message. Messages that share a key are
/// guaranteed to be handled in delivery order relative to each other; messages with
/// different keys may be handled concurrently and in any relative order.
/// </summary>
public interface IMessageKeySelector<in T>
{
    string GetKey(T message);
}
