namespace messaging_lab.solace.fanout;

/// <summary>
/// Wire-compatible with messaging_lab.orders.OrderPlaced (the subscriber ignores the extra
/// <paramref name="Padding"/> field), so the existing subscriber can consume these unchanged.
/// </summary>
public sealed record FanoutOrder(string OrderId, decimal Total, long Sequence, DateTime PublishedAtUtc, string Padding);
