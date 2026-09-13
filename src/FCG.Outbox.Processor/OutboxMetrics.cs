using Prometheus;

namespace FCG.Outbox.Processor;

public static class OutboxMetrics
{
    public static readonly Counter Published = Metrics.CreateCounter(
        "fcg_outbox_published_total",
        "Total number of outbox events published to SQS.");

    public static readonly Counter Failed = Metrics.CreateCounter(
        "fcg_outbox_publish_failed_total",
        "Total number of outbox event publication attempts that failed.");

    public static readonly Counter Exhausted = Metrics.CreateCounter(
        "fcg_outbox_exhausted_total",
        "Total number of outbox events that exhausted the maximum publication attempts.");
}
