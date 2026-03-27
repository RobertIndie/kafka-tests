using System.Collections.Concurrent;
using System.Text.Json;

namespace ksn_disconnect;

internal sealed class MetricsCollector
{
    private readonly string _runId;
    private readonly int _errorSampleLimit;
    private readonly ConcurrentDictionary<string, long> _errorCounts = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentQueue<ErrorSample> _errorSamples = new();

    private long _producedAttempted;
    private long _producedSucceeded;
    private long _producedFailed;
    private long _consumed;
    private long _commitSucceeded;
    private long _commitFailed;
    private long _producerRecreated;
    private long _consumerRecreated;
    private long _rebalances;
    private long _fatalErrors;
    private long _statsEvents;

    public MetricsCollector(string runId, int errorSampleLimit)
    {
        _runId = runId;
        _errorSampleLimit = errorSampleLimit;
    }

    public void RecordProduceAttempt() => Interlocked.Increment(ref _producedAttempted);
    public void RecordProduceSuccess() => Interlocked.Increment(ref _producedSucceeded);
    public void RecordProduceFailure() => Interlocked.Increment(ref _producedFailed);
    public void RecordConsumed() => Interlocked.Increment(ref _consumed);
    public void RecordCommitSuccess() => Interlocked.Increment(ref _commitSucceeded);
    public void RecordCommitFailure() => Interlocked.Increment(ref _commitFailed);
    public void RecordProducerRecreated() => Interlocked.Increment(ref _producerRecreated);
    public void RecordConsumerRecreated() => Interlocked.Increment(ref _consumerRecreated);
    public void RecordRebalance() => Interlocked.Increment(ref _rebalances);
    public void RecordStatsEvent() => Interlocked.Increment(ref _statsEvents);

    public void RecordError(string source, string code, bool isFatal, bool isBrokerError, string reason)
    {
        _errorCounts.AddOrUpdate(code, 1, (_, current) => current + 1);
        if (_errorSamples.Count < _errorSampleLimit)
        {
            _errorSamples.Enqueue(new ErrorSample(DateTimeOffset.UtcNow, source, code, isFatal, isBrokerError, reason));
        }

        if (isFatal)
        {
            Interlocked.Increment(ref _fatalErrors);
        }
    }

    public RunSummary Snapshot(AppOptions options)
    {
        var errorCounts = _errorCounts.OrderByDescending(item => item.Value)
            .ToDictionary(item => item.Key, item => item.Value, StringComparer.OrdinalIgnoreCase);
        var samples = _errorSamples.ToArray();

        var suspicious = new List<string>();
        if (errorCounts.TryGetValue("Local_Transport", out var localTransport) && localTransport > 3)
        {
            suspicious.Add($"Local_Transport count={localTransport}");
        }

        if (errorCounts.TryGetValue("Local_MsgTimedOut", out var messageTimeouts) && messageTimeouts > 0)
        {
            suspicious.Add($"Local_MsgTimedOut count={messageTimeouts}");
        }

        if (Volatile.Read(ref _rebalances) > Math.Max(2, options.ConsumerCount * 3) && !options.UsesRebalance)
        {
            suspicious.Add($"Unexpected rebalance count={Volatile.Read(ref _rebalances)}");
        }

        var severity = Volatile.Read(ref _fatalErrors) > 0
            ? "Critical"
            : suspicious.Count > 0
                ? "Suspicious"
                : "Expected";

        return new RunSummary(
            _runId,
            severity,
            Volatile.Read(ref _producedAttempted),
            Volatile.Read(ref _producedSucceeded),
            Volatile.Read(ref _producedFailed),
            Volatile.Read(ref _consumed),
            Volatile.Read(ref _commitSucceeded),
            Volatile.Read(ref _commitFailed),
            Volatile.Read(ref _producerRecreated),
            Volatile.Read(ref _consumerRecreated),
            Volatile.Read(ref _rebalances),
            Volatile.Read(ref _fatalErrors),
            Volatile.Read(ref _statsEvents),
            errorCounts,
            samples,
            suspicious);
    }

    public sealed record ErrorSample(
        DateTimeOffset Ts,
        string Source,
        string Code,
        bool IsFatal,
        bool IsBrokerError,
        string Reason);

    public sealed record RunSummary(
        string RunId,
        string Severity,
        long ProducedAttempted,
        long ProducedSucceeded,
        long ProducedFailed,
        long Consumed,
        long CommitSucceeded,
        long CommitFailed,
        long ProducerRecreated,
        long ConsumerRecreated,
        long Rebalances,
        long FatalErrors,
        long StatsEvents,
        IReadOnlyDictionary<string, long> ErrorCounts,
        IReadOnlyList<ErrorSample> ErrorSamples,
        IReadOnlyList<string> SuspiciousSignals)
    {
        public string ToJson() => JsonSerializer.Serialize(this, new JsonSerializerOptions { WriteIndented = true });
    }
}
