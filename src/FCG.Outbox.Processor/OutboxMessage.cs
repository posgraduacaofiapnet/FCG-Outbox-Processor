using System.Text.Json;

namespace FCG.Outbox.Processor;

public sealed record OutboxMessage(
    Guid Id,
    string EventType,
    DateTimeOffset CreatedAt,
    string Payload,
    int Attempts);

public sealed record OutboxEnvelope(
    Guid Id,
    string EventType,
    DateTimeOffset CreatedAt,
    JsonElement Payload);
