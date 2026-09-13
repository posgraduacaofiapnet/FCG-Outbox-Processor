using FCG.Outbox.Processor;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace FCG.Outbox.Processor.Tests;

public sealed class OutboxDispatcherWorkerTests
{
    [Theory]
    [InlineData(9, false)]
    [InlineData(10, true)]
    [InlineData(11, true)]
    public void IsExhausted_UsesTenAttemptsAsTheLimit(int attempts, bool expected)
    {
        Assert.Equal(expected, OutboxProcessingLimits.IsExhausted(attempts));
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenPublishSucceeds_MarksMessageSuccessful()
    {
        var message = CreateMessage(attempts: 1);
        var repository = new FakeRepository(message);
        var publisher = new FakePublisher();
        var worker = CreateWorker(repository, publisher);

        await worker.ProcessBatchAsync(CancellationToken.None);

        Assert.Equal(new[] { message.Id }, repository.SuccessfulIds);
        Assert.Empty(repository.FailedIds);
        Assert.Equal(new[] { message.Id }, publisher.PublishedIds);
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenTenthAttemptFails_RecordsFailureWithoutSuccess()
    {
        var message = CreateMessage(OutboxProcessingLimits.MaximumAttempts);
        var repository = new FakeRepository(message);
        var publisher = new FakePublisher { Exception = new InvalidOperationException("SQS unavailable") };
        var worker = CreateWorker(repository, publisher);

        await worker.ProcessBatchAsync(CancellationToken.None);

        Assert.Empty(repository.SuccessfulIds);
        Assert.Equal(new[] { message.Id }, repository.FailedIds);
        Assert.Equal(new[] { message.Id }, publisher.PublishedIds);
    }

    private static OutboxDispatcherWorker CreateWorker(
        IOutboxRepository repository,
        IOutboxPublisher publisher) => new(
        repository,
        publisher,
        Options.Create(new OutboxOptions
        {
            ConnectionString = "Server=unused",
            QueueUrl = "https://example.invalid/queue",
            RetryDelayMinutes = 15
        }),
        NullLogger<OutboxDispatcherWorker>.Instance);

    private static OutboxMessage CreateMessage(int attempts) => new(
        Guid.NewGuid(),
        "UserCreated",
        DateTimeOffset.UtcNow,
        "{}",
        attempts);

    private sealed class FakeRepository : IOutboxRepository
    {
        private readonly OutboxMessage[] _messages;

        public FakeRepository(params OutboxMessage[] messages)
        {
            _messages = messages;
        }

        public List<Guid> SuccessfulIds { get; } = [];
        public List<Guid> FailedIds { get; } = [];

        public Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<OutboxMessage>>(_messages);

        public Task MarkSuccessfulAsync(Guid id, CancellationToken cancellationToken)
        {
            SuccessfulIds.Add(id);
            return Task.CompletedTask;
        }

        public Task RecordFailureAsync(Guid id, CancellationToken cancellationToken)
        {
            FailedIds.Add(id);
            return Task.CompletedTask;
        }

        public Task<int> DeleteProcessedAsync(CancellationToken cancellationToken) => Task.FromResult(0);
    }

    private sealed class FakePublisher : IOutboxPublisher
    {
        public Exception? Exception { get; init; }
        public List<Guid> PublishedIds { get; } = [];

        public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken)
        {
            PublishedIds.Add(message.Id);
            var exception = Exception;
            return exception is null ? Task.CompletedTask : Task.FromException(exception);
        }
    }
}
