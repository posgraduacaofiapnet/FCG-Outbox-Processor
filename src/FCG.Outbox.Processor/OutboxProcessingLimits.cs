namespace FCG.Outbox.Processor;

public static class OutboxProcessingLimits
{
    public const int MaximumAttempts = 10;

    public static bool IsExhausted(int attempts) => attempts >= MaximumAttempts;
}
