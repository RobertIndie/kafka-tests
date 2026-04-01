namespace ksn_disconnect;

internal static class SummaryFormatting
{
    public static Dictionary<string, object?> BuildIntermediateSummaryFields(
        AppOptions options,
        MetricsCollector.RunSummary current,
        MetricsCollector.RunSummary? previous,
        DateTimeOffset now,
        DateTimeOffset? previousAt)
    {
        var targetProducerRate = options.ProducerTrafficEnabled(now) ? options.GetCurrentRate(now) : 0d;
        var targetProducerRatePerProducer = options.ProducerTrafficEnabled(now) ? options.GetCurrentRatePerProducer(now) : 0d;

        var intervalSec = previousAt is null
            ? Math.Max(1, options.StatsIntervalSec)
            : Math.Max(0.001d, (now - previousAt.Value).TotalSeconds);

        var producedAttemptRate = ComputeRate(current.ProducedAttempted, previous?.ProducedAttempted, intervalSec);
        var producedSuccessRate = ComputeRate(current.ProducedSucceeded, previous?.ProducedSucceeded, intervalSec);
        var producedFailureRate = ComputeRate(current.ProducedFailed, previous?.ProducedFailed, intervalSec);
        var consumedRate = ComputeRate(current.Consumed, previous?.Consumed, intervalSec);

        return new Dictionary<string, object?>
        {
            ["severity"] = current.Severity,
            ["targetProducerRate"] = Math.Round(targetProducerRate, 2),
            ["targetProducerRatePerProducer"] = Math.Round(targetProducerRatePerProducer, 2),
            ["observedProduceAttemptRate"] = Math.Round(producedAttemptRate, 2),
            ["observedProduceSuccessRate"] = Math.Round(producedSuccessRate, 2),
            ["observedProduceFailureRate"] = Math.Round(producedFailureRate, 2),
            ["observedConsumeRate"] = Math.Round(consumedRate, 2),
            ["producedSucceeded"] = current.ProducedSucceeded,
            ["producedFailed"] = current.ProducedFailed,
            ["consumed"] = current.Consumed,
            ["commitFailed"] = current.CommitFailed,
            ["producerRecreated"] = current.ProducerRecreated,
            ["consumerRecreated"] = current.ConsumerRecreated,
            ["rebalances"] = current.Rebalances,
        };
    }

    private static double ComputeRate(long current, long? previous, double intervalSec)
    {
        var baseline = previous ?? 0L;
        return Math.Max(0d, current - baseline) / intervalSec;
    }
}
