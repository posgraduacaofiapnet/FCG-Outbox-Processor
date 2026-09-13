using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Options;

namespace FCG.Outbox.Processor;

public interface IOutboxPublisher
{
    Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken);
}

public sealed class SqsOutboxPublisher(
    IAmazonSQS sqs,
    IOptions<OutboxOptions> options,
    ILogger<SqsOutboxPublisher> logger) : IOutboxPublisher
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly OutboxOptions _options = options.Value;

    public async Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken)
    {
        using var payload = JsonDocument.Parse(message.Payload);
        var envelope = new OutboxEnvelope(
            message.Id,
            message.EventType,
            message.CreatedAt,
            payload.RootElement.Clone());

        logger.LogInformation(
            "Publishing outbox event {EventId} of type {EventType} on attempt {Attempt}",
            message.Id,
            message.EventType,
            message.Attempts);

        var response = await sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl = _options.QueueUrl,
            MessageBody = JsonSerializer.Serialize(envelope, JsonOptions)
        }, cancellationToken);

        logger.LogInformation(
            "Published outbox event {EventId} of type {EventType}; SQS message {MessageId}",
            message.Id,
            message.EventType,
            response.MessageId);
    }
}
