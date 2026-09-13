using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace FCG.Outbox.Processor;

public interface IOutboxRepository
{
    Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(CancellationToken cancellationToken);
    Task MarkSuccessfulAsync(Guid id, CancellationToken cancellationToken);
    Task RecordFailureAsync(Guid id, CancellationToken cancellationToken);
    Task<int> DeleteProcessedAsync(CancellationToken cancellationToken);
}

public sealed class SqlOutboxRepository(IOptions<OutboxOptions> options) : IOutboxRepository
{
    private readonly OutboxOptions _options = options.Value;

    public async Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(CancellationToken cancellationToken)
    {
        // Keep the attempt limit as a SQL literal so the query can use the filtered pending index.
        var sql = $"""
            ;WITH Candidates AS
            (
                SELECT TOP (@BatchSize)
                    Id,
                    EventType,
                    IsSuccessful,
                    CreatedAt,
                    Payload,
                    NextAttemptAt,
                    Attempts
                FROM dbo.OutboxMessages WITH (UPDLOCK, READPAST, ROWLOCK)
                WHERE IsSuccessful = 0
                  AND Attempts < {OutboxProcessingLimits.MaximumAttempts}
                  AND (NextAttemptAt IS NULL OR NextAttemptAt <= @Now)
                ORDER BY NextAttemptAt, CreatedAt, Id
            )
            UPDATE Candidates
               SET Attempts = Attempts + 1,
                   NextAttemptAt = @LeaseUntil
            OUTPUT
                inserted.Id,
                inserted.EventType,
                inserted.CreatedAt,
                inserted.Payload,
                inserted.Attempts;
            """;

        var now = DateTimeOffset.UtcNow;
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@BatchSize", SqlDbType.Int).Value = _options.BatchSize;
        command.Parameters.Add("@Now", SqlDbType.DateTimeOffset).Value = now;
        command.Parameters.Add("@LeaseUntil", SqlDbType.DateTimeOffset).Value = now.AddMinutes(_options.RetryDelayMinutes);

        var messages = new List<OutboxMessage>(_options.BatchSize);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            messages.Add(new OutboxMessage(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetFieldValue<DateTimeOffset>(2),
                reader.GetString(3),
                reader.GetInt32(4)));
        }

        return messages;
    }

    public Task MarkSuccessfulAsync(Guid id, CancellationToken cancellationToken) =>
        ExecuteAsync(
            """
            UPDATE dbo.OutboxMessages
               SET IsSuccessful = 1,
                   NextAttemptAt = NULL
             WHERE Id = @Id
               AND IsSuccessful = 0;
            """,
            id,
            cancellationToken);

    public Task RecordFailureAsync(Guid id, CancellationToken cancellationToken) =>
        ExecuteAsync(
            $"""
            UPDATE dbo.OutboxMessages
               SET IsSuccessful = 0,
                   NextAttemptAt = CASE
                       WHEN Attempts >= {OutboxProcessingLimits.MaximumAttempts} THEN NULL
                       ELSE DATEADD(MINUTE, @RetryDelayMinutes, SYSUTCDATETIME())
                   END
             WHERE Id = @Id
               AND IsSuccessful = 0;
            """,
            id,
            cancellationToken,
            includeRetryDelay: true);

    public async Task<int> DeleteProcessedAsync(CancellationToken cancellationToken)
    {
        const string sql = """
            DELETE FROM dbo.OutboxMessages
             WHERE Id IN
             (
                 SELECT TOP (500) Id
                   FROM dbo.OutboxMessages WITH (READPAST)
                  WHERE IsSuccessful = 1
                    AND CreatedAt < @Cutoff
                  ORDER BY CreatedAt, Id
             );
            """;

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@Cutoff", SqlDbType.DateTimeOffset).Value =
            DateTimeOffset.UtcNow.AddDays(-_options.SuccessfulRetentionDays);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task ExecuteAsync(
        string sql,
        Guid id,
        CancellationToken cancellationToken,
        bool includeRetryDelay = false)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = id;
        if (includeRetryDelay)
        {
            command.Parameters.Add("@RetryDelayMinutes", SqlDbType.Int).Value = _options.RetryDelayMinutes;
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(_options.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }
}
