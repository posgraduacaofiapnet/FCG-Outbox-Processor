using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Xunit;

namespace FCG.Outbox.Processor.Tests;

public sealed class SqlServerHealthCheckTests
{
    [Fact]
    public async Task CheckHealthAsync_WhenConnectionFails_ReturnsUnhealthy()
    {
        var sut = new SqlServerHealthCheck(Options.Create(new OutboxOptions
        {
            ConnectionString = "Server=127.0.0.1,1;Database=missing;User Id=sa;Password=InvalidPassword1!;TrustServerCertificate=True;Connect Timeout=1"
        }));

        var result = await sut.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }
}
