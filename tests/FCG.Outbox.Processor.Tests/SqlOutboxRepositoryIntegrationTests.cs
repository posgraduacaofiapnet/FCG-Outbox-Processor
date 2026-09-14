using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Xunit;

namespace FCG.Outbox.Processor.Tests;

public sealed class SqlOutboxRepositoryIntegrationTests
{
    [SkippableFact]
    public async Task Repository_ClaimsCompletesRetriesAndDeletesMessages()
    {
        var connectionString = Environment.GetEnvironmentVariable("FCG_TEST_SQL_CONNECTION");
        Skip.If(string.IsNullOrWhiteSpace(connectionString), "FCG_TEST_SQL_CONNECTION is not configured.");
        var successfulId = Guid.NewGuid();
        var exhaustedId = Guid.NewGuid();
        await InsertAsync(connectionString!, successfulId, attempts: 0, DateTimeOffset.UtcNow.AddDays(-10));
        await InsertAsync(connectionString!, exhaustedId, attempts: 9, DateTimeOffset.UtcNow);
        var options = Options.Create(new OutboxOptions
        {
            ConnectionString = connectionString!,
            QueueUrl = "https://example.test/queue",
            BatchSize = 10,
            RetryDelayMinutes = 15,
            SuccessfulRetentionDays = 7
        });
        var sut = new SqlOutboxRepository(options);

        try
        {
            var claimed = await sut.ClaimBatchAsync(CancellationToken.None);
            Assert.Contains(claimed, message => message.Id == successfulId && message.Attempts == 1);
            Assert.Contains(claimed, message => message.Id == exhaustedId && message.Attempts == 10);

            await sut.MarkSuccessfulAsync(successfulId, CancellationToken.None);
            await sut.RecordFailureAsync(exhaustedId, CancellationToken.None);
            var deleted = await sut.DeleteProcessedAsync(CancellationToken.None);

            Assert.True(deleted >= 1);
            var state = await ReadStateAsync(connectionString!, exhaustedId);
            Assert.Equal((false, 10, null), state);
        }
        finally
        {
            await DeleteAsync(connectionString!, successfulId, exhaustedId);
        }
    }

    [SkippableFact]
    public async Task HealthCheck_WithAvailableSqlServer_ReturnsHealthy()
    {
        var connectionString = Environment.GetEnvironmentVariable("FCG_TEST_SQL_CONNECTION");
        Skip.If(string.IsNullOrWhiteSpace(connectionString), "FCG_TEST_SQL_CONNECTION is not configured.");
        var sut = new SqlServerHealthCheck(Options.Create(new OutboxOptions { ConnectionString = connectionString! }));

        var result = await sut.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    private static async Task InsertAsync(string connectionString, Guid id, int attempts, DateTimeOffset createdAt)
    {
        const string sql = """
            INSERT INTO dbo.OutboxMessages
                (Id, EventType, IsSuccessful, CreatedAt, Payload, NextAttemptAt, Attempts)
            VALUES
                (@Id, N'UserCreated', 0, @CreatedAt, N'{"userId":"00000000-0000-0000-0000-000000000001"}', NULL, @Attempts);
            """;
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@Id", id);
        command.Parameters.AddWithValue("@CreatedAt", createdAt);
        command.Parameters.AddWithValue("@Attempts", attempts);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<(bool IsSuccessful, int Attempts, DateTimeOffset? NextAttemptAt)> ReadStateAsync(
        string connectionString,
        Guid id)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            "SELECT IsSuccessful, Attempts, NextAttemptAt FROM dbo.OutboxMessages WHERE Id = @Id", connection);
        command.Parameters.AddWithValue("@Id", id);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return (reader.GetBoolean(0), reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTimeOffset>(2));
    }

    private static async Task DeleteAsync(string connectionString, params Guid[] ids)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        foreach (var id in ids)
        {
            await using var command = new SqlCommand("DELETE FROM dbo.OutboxMessages WHERE Id = @Id", connection);
            command.Parameters.AddWithValue("@Id", id);
            await command.ExecuteNonQueryAsync();
        }
    }
}
