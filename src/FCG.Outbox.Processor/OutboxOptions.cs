namespace FCG.Outbox.Processor;

public sealed class OutboxOptions
{
    public const string SectionName = "Outbox";

    public string ConnectionString { get; init; } = string.Empty;
    public string QueueUrl { get; init; } = string.Empty;
    public string Region { get; init; } = "us-east-1";
    public int BatchSize { get; init; } = 20;
    public int PollIntervalSeconds { get; init; } = 5;
    public int RetryDelayMinutes { get; init; } = 15;
    public int SuccessfulRetentionDays { get; init; } = 7;
}
