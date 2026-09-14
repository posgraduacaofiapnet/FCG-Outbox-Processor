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

    [Fact]
    public async Task ProcessBatchAsync_WhenRetryablePublishFails_RecordsFailure()
    {
        var message = CreateMessage(attempts: 2);
        var repository = new FakeRepository(message);
        var publisher = new FakePublisher { Exception = new InvalidOperationException("SQS unavailable") };

        await CreateWorker(repository, publisher).ProcessBatchAsync(CancellationToken.None);

        Assert.Equal(new[] { message.Id }, repository.FailedIds);
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenClaimFails_ReturnsWithoutPublishing()
    {
        var repository = new FakeRepository { ClaimException = new InvalidOperationException("SQL unavailable") };
        var publisher = new FakePublisher();

        await CreateWorker(repository, publisher).ProcessBatchAsync(CancellationToken.None);

        Assert.Empty(publisher.PublishedIds);
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenFailureCannotBeRecorded_ContinuesCleanup()
    {
        var repository = new FakeRepository(CreateMessage(2))
        {
            RecordFailureException = new InvalidOperationException("SQL unavailable")
        };
        var publisher = new FakePublisher { Exception = new InvalidOperationException("SQS unavailable") };

        await CreateWorker(repository, publisher).ProcessBatchAsync(CancellationToken.None);

        Assert.True(repository.CleanupCalled);
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenCleanupDeletesRows_Completes()
    {
        var repository = new FakeRepository { DeletedCount = 2 };

        await CreateWorker(repository, new FakePublisher()).ProcessBatchAsync(CancellationToken.None);

        Assert.True(repository.CleanupCalled);
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenCleanupFails_DoesNotFailBatch()
    {
        var repository = new FakeRepository { CleanupException = new InvalidOperationException("SQL unavailable") };

        var action = () => CreateWorker(repository, new FakePublisher())
            .ProcessBatchAsync(CancellationToken.None);

        await action();
    }

    [Fact]
    public async Task ProcessBatchAsync_WhenCancelledDuringClaim_PropagatesCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var repository = new FakeRepository { ClaimException = new OperationCanceledException(cancellation.Token) };

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            CreateWorker(repository, new FakePublisher()).ProcessBatchAsync(cancellation.Token));
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
        public Exception? ClaimException { get; init; }
        public Exception? RecordFailureException { get; init; }
        public Exception? CleanupException { get; init; }
        public int DeletedCount { get; init; }
        public bool CleanupCalled { get; private set; }

        public Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(CancellationToken cancellationToken) =>
            ClaimException is null
                ? Task.FromResult<IReadOnlyList<OutboxMessage>>(_messages)
                : Task.FromException<IReadOnlyList<OutboxMessage>>(ClaimException);

        public Task MarkSuccessfulAsync(Guid id, CancellationToken cancellationToken)
        {
            SuccessfulIds.Add(id);
            return Task.CompletedTask;
        }

        public Task RecordFailureAsync(Guid id, CancellationToken cancellationToken)
        {
            FailedIds.Add(id);
            return RecordFailureException is null
                ? Task.CompletedTask
                : Task.FromException(RecordFailureException);
        }

        public Task<int> DeleteProcessedAsync(CancellationToken cancellationToken)
        {
            CleanupCalled = true;
            return CleanupException is null
                ? Task.FromResult(DeletedCount)
                : Task.FromException<int>(CleanupException);
        }
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
