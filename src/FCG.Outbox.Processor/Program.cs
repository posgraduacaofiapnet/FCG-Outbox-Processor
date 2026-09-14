using Amazon;
using Amazon.SQS;
using FCG.Outbox.Processor;
using Prometheus;
using Serilog;
using Serilog.Formatting.Compact;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((context, configuration) => configuration
    .ReadFrom.Configuration(context.Configuration)
    .Enrich.FromLogContext()
    .Enrich.WithProperty("Service", "FCG.Outbox.Processor")
    .WriteTo.Console(new RenderedCompactJsonFormatter()));

builder.Services
    .AddOptions<OutboxOptions>()
    .Bind(builder.Configuration.GetSection(OutboxOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConnectionString), "Outbox:ConnectionString is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.QueueUrl), "Outbox:QueueUrl is required.")
    .Validate(options => options.BatchSize is > 0 and <= 100, "Outbox:BatchSize must be between 1 and 100.")
    .Validate(options => options.PollIntervalSeconds > 0, "Outbox:PollIntervalSeconds must be positive.")
    .Validate(options => options.RetryDelayMinutes > 0, "Outbox:RetryDelayMinutes must be positive.")
    .Validate(options => options.SuccessfulRetentionDays > 0, "Outbox:SuccessfulRetentionDays must be positive.")
    .ValidateOnStart();

var region = builder.Configuration[$"{OutboxOptions.SectionName}:Region"] ?? "us-east-1";
var serviceUrl = builder.Configuration[$"{OutboxOptions.SectionName}:ServiceUrl"];
builder.Services.AddSingleton<IAmazonSQS>(_ => string.IsNullOrWhiteSpace(serviceUrl)
    ? new AmazonSQSClient(RegionEndpoint.GetBySystemName(region))
    : new AmazonSQSClient(new AmazonSQSConfig
    {
        ServiceURL = serviceUrl,
        AuthenticationRegion = region
    }));
builder.Services.AddSingleton<IOutboxRepository, SqlOutboxRepository>();
builder.Services.AddSingleton<IOutboxPublisher, SqsOutboxPublisher>();
builder.Services.AddHostedService<OutboxDispatcherWorker>();
builder.Services.AddHealthChecks().AddCheck<SqlServerHealthCheck>("sqlserver");

var app = builder.Build();
app.UseHttpMetrics();
app.MapHealthChecks("/health/live", new() { Predicate = _ => false });
app.MapHealthChecks("/health/ready");
app.MapMetrics();
app.Run();

public partial class Program;
