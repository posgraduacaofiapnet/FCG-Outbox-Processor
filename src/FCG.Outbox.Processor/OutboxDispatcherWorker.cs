using Microsoft.Extensions.Options;

namespace FCG.Outbox.Processor;

public sealed class OutboxDispatcherWorker(
    IOutboxRepository repository,
    IOutboxPublisher publisher,
    IOptions<OutboxOptions> options,
    ILogger<OutboxDispatcherWorker> logger) : BackgroundService
{
    private readonly OutboxOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "Outbox processor started with batch size {BatchSize} and poll interval {PollIntervalSeconds}s",
            _options.BatchSize,
            _options.PollIntervalSeconds);

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_options.PollIntervalSeconds));
        do
        {
            await ProcessBatchAsync(stoppingToken);
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    internal async Task ProcessBatchAsync(CancellationToken cancellationToken)
    {
        IReadOnlyList<OutboxMessage> messages;
        try
        {
            messages = await repository.ClaimBatchAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Failed to claim an outbox batch");
            return;
        }

        foreach (var message in messages)
        {
            try
            {
                await publisher.PublishAsync(message, cancellationToken);
                await repository.MarkSuccessfulAsync(message.Id, cancellationToken);
                OutboxMetrics.Published.Inc();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                OutboxMetrics.Failed.Inc();
                var exhausted = OutboxProcessingLimits.IsExhausted(message.Attempts);

                if (exhausted)
                {
                    OutboxMetrics.Exhausted.Inc();
                    logger.LogCritical(
                        exception,
                        "Outbox event {EventId} of type {EventType} exhausted after {AttemptCount} attempts and requires manual intervention",
                        message.Id,
                        message.EventType,
                        message.Attempts);
                }
                else
                {
                    logger.LogWarning(
                        exception,
                        "Failed to publish outbox event {EventId} of type {EventType} on attempt {Attempt}; retry scheduled in {RetryDelayMinutes} minutes",
                        message.Id,
                        message.EventType,
                        message.Attempts,
                        _options.RetryDelayMinutes);
                }

                try
                {
                    await repository.RecordFailureAsync(message.Id, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception schedulingException)
                {
                    logger.LogError(
                        schedulingException,
                        "Failed to update retry state for outbox event {EventId}; its claim lease remains active",
                        message.Id);
                }
            }
        }

        try
        {
            var deleted = await repository.DeleteProcessedAsync(cancellationToken);
            if (deleted > 0)
            {
                logger.LogInformation("Deleted {DeletedCount} processed outbox events after retention", deleted);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            logger.LogWarning(exception, "Failed to clean processed outbox events");
        }
    }
}
