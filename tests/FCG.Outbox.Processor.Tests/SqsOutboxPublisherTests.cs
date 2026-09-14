using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace FCG.Outbox.Processor.Tests;

public sealed class SqsOutboxPublisherTests
{
    [Fact]
    public async Task PublishAsync_SendsCompleteEnvelopeToConfiguredQueue()
    {
        var sqs = new Mock<IAmazonSQS>();
        SendMessageRequest? captured = null;
        sqs.Setup(x => x.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()))
            .Callback<SendMessageRequest, CancellationToken>((request, _) => captured = request)
            .ReturnsAsync(new SendMessageResponse { MessageId = "sqs-id" });
        var options = Options.Create(new OutboxOptions { QueueUrl = "https://example.test/queue" });
        var sut = new SqsOutboxPublisher(sqs.Object, options, NullLogger<SqsOutboxPublisher>.Instance);
        var message = new OutboxMessage(Guid.NewGuid(), "UserCreated", DateTimeOffset.UtcNow, "{\"email\":\"ada@example.com\"}", 1);

        await sut.PublishAsync(message, CancellationToken.None);

        Assert.NotNull(captured);
        Assert.Equal(options.Value.QueueUrl, captured!.QueueUrl);
        using var json = JsonDocument.Parse(captured.MessageBody);
        Assert.Equal(message.Id, json.RootElement.GetProperty("id").GetGuid());
        Assert.Equal(message.EventType, json.RootElement.GetProperty("eventType").GetString());
        Assert.Equal("ada@example.com", json.RootElement.GetProperty("payload").GetProperty("email").GetString());
    }
}
