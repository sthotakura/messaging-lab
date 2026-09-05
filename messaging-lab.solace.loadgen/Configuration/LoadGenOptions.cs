namespace messaging_lab.solace.loadgen.Configuration;

public sealed class LoadGenOptions
{
    public required string Topic { get; init; }

    /// <summary>Total messages to publish, spread round-robin across <see cref="KeyCount"/> keys.</summary>
    public int Count { get; init; } = 1000;

    /// <summary>Distinct OrderId keys to publish under; each key's messages carry a strictly increasing sequence.</summary>
    public int KeyCount { get; init; } = 8;

    public MessageDeliveryModeOption DeliveryMode { get; init; } = MessageDeliveryModeOption.Persistent;
}

public enum MessageDeliveryModeOption
{
    Persistent,
    NonPersistent,
}
