namespace messaging_lab.orders;

/// <summary>
/// Shared test message published by messaging-lab.solace.loadgen and consumed by
/// messaging-lab.solace.subscriber. <paramref name="Sequence"/> increases monotonically per
/// <paramref name="OrderId"/>, letting a subscriber detect out-of-order delivery for that key.
/// <paramref name="PublishedAtUtc"/> lets a subscriber measure end-to-end latency.
/// </summary>
public record OrderPlaced(string OrderId, decimal Total, long Sequence, DateTime PublishedAtUtc);
