namespace messaging_lab.solace.loadgen.Configuration;

public sealed class SolaceOptions
{
    public required string Host { get; init; }

    public required string VPNName { get; init; }

    public required string UserName { get; init; }

    public string? Password { get; init; }
}
