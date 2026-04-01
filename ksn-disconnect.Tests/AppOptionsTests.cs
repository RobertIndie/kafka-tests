using Xunit;

namespace ksn_disconnect.Tests;

public sealed class AppOptionsTests
{
    [Fact]
    public void GetCurrentRatePerProducer_SplitsTotalRateAcrossProducers()
    {
        var now = new DateTimeOffset(2026, 4, 1, 8, 0, 5, TimeSpan.Zero);
        var options = new AppOptions
        {
            Scenario = ScenarioKind.Mixed,
            ProduceRate = 10_000,
            BurstRate = 20_000,
            BurstIntervalSec = 15,
            ProducerCount = 2,
            IdleWindowSec = 0,
            ProcessStartUtc = new DateTimeOffset(2026, 4, 1, 8, 0, 0, TimeSpan.Zero),
        };

        Assert.Equal(10_000, options.GetCurrentRate(now));
        Assert.Equal(5_000, options.GetCurrentRatePerProducer(now));
    }
}
