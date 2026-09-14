using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace FCG.Outbox.Processor.Tests;

public sealed class ProgramTests : IClassFixture<OutboxApiFactory>
{
    private readonly OutboxApiFactory _factory;

    public ProgramTests(OutboxApiFactory factory) => _factory = factory;

    [Fact]
    public async Task LiveHealthEndpoint_ReturnsSuccess()
    {
        using var client = _factory.CreateClient();

        var response = await client.GetAsync("/health/live");

        response.EnsureSuccessStatusCode();
    }

    [Fact]
    public async Task MetricsEndpoint_ReturnsPrometheusPayload()
    {
        using var client = _factory.CreateClient();

        var response = await client.GetAsync("/metrics");

        response.EnsureSuccessStatusCode();
        Assert.Contains("# HELP", await response.Content.ReadAsStringAsync());
    }
}

public sealed class OutboxApiFactory : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureAppConfiguration((_, configuration) => configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                ["Outbox:ConnectionString"] = "Server=unused",
                ["Outbox:QueueUrl"] = "https://example.test/queue",
                ["Outbox:ServiceUrl"] = "http://localhost.localstack.cloud:4566",
                ["Outbox:BatchSize"] = "10",
                ["Outbox:PollIntervalSeconds"] = "1",
                ["Outbox:RetryDelayMinutes"] = "15",
                ["Outbox:SuccessfulRetentionDays"] = "7"
            }));

        builder.ConfigureServices(services => services.RemoveAll<IHostedService>());
    }
}
