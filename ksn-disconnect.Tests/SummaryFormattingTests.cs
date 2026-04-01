using Xunit;

namespace ksn_disconnect.Tests;

public sealed class SummaryFormattingTests
{
    [Fact]
    public void BuildIntermediateSummaryFields_IncludesProducerTargetAndObservedRates()
    {
        var now = new DateTimeOffset(2026, 4, 1, 8, 0, 10, TimeSpan.Zero);
        var previousAt = now.AddSeconds(-5);
        var options = new AppOptions
        {
            Scenario = ScenarioKind.Mixed,
            ProduceRate = 10_000,
            BurstRate = 20_000,
            BurstIntervalSec = 15,
            ProducerCount = 2,
            StatsIntervalSec = 5,
            IdleWindowSec = 0,
            ProcessStartUtc = new DateTimeOffset(2026, 4, 1, 8, 0, 0, TimeSpan.Zero),
        };

        var previous = CreateSummary(producedAttempted: 100, producedSucceeded: 90, producedFailed: 10, consumed: 50);
        var current = CreateSummary(producedAttempted: 50_100, producedSucceeded: 50_090, producedFailed: 10, consumed: 10_050);

        var fields = SummaryFormatting.BuildIntermediateSummaryFields(options, current, previous, now, previousAt);

        Assert.Equal(10_000d, fields["targetProducerRate"]);
        Assert.Equal(5_000d, fields["targetProducerRatePerProducer"]);
        Assert.Equal(10_000d, fields["observedProduceAttemptRate"]);
        Assert.Equal(10_000d, fields["observedProduceSuccessRate"]);
        Assert.Equal(0d, fields["observedProduceFailureRate"]);
        Assert.Equal(2_000d, fields["observedConsumeRate"]);
    }

    private static MetricsCollector.RunSummary CreateSummary(
        long producedAttempted,
        long producedSucceeded,
        long producedFailed,
        long consumed)
    {
        return new MetricsCollector.RunSummary(
            RunId: "test-run",
            Severity: "Expected",
            ProducedAttempted: producedAttempted,
            ProducedSucceeded: producedSucceeded,
            ProducedFailed: producedFailed,
            Consumed: consumed,
            CommitSucceeded: 0,
            CommitFailed: 0,
            ProducerRecreated: 0,
            ConsumerRecreated: 0,
            Rebalances: 0,
            FatalErrors: 0,
            StatsEvents: 0,
            ErrorCounts: new Dictionary<string, long>(),
            ErrorSamples: Array.Empty<MetricsCollector.ErrorSample>(),
            SuspiciousSignals: Array.Empty<string>());
    }
}
