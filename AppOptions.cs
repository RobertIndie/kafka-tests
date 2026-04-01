using System.Globalization;
using Confluent.Kafka;

namespace ksn_disconnect;

internal enum ScenarioKind
{
    Steady,
    Burst,
    Recycle,
    Rebalance,
    Mixed,
}

internal enum CommitMode
{
    Auto,
    Manual,
}

internal enum AppLogLevel
{
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

internal sealed class AppOptions
{
    public string RunId { get; init; } = $"run-{DateTimeOffset.UtcNow:yyyyMMddHHmmss}";
    public bool ShowHelp { get; init; }
    public string BootstrapServers { get; init; } = string.Empty;
    public string Topic { get; init; } = string.Empty;
    public string GroupId { get; init; } = "ksn-disconnect";
    public int PartitionsHint { get; init; } = 1;
    public string ClientIdPrefix { get; init; } = "ksn-disconnect";
    public SecurityProtocol? SecurityProtocol { get; init; }
    public SaslMechanism? SaslMechanism { get; init; }
    public string? SaslUsername { get; init; }
    public string? SaslPassword { get; init; }
    public string? SslCaLocation { get; init; }
    public int ProducerCount { get; init; } = 1;
    public double ProduceRate { get; init; } = 2;
    public double BurstRate { get; init; } = 30;
    public int BurstIntervalSec { get; init; } = 20;
    public int MessageBytes { get; init; } = 512;
    public int LingerMs { get; init; } = 20;
    public int BatchSize { get; init; } = 131072;
    public Acks Acks { get; init; } = Acks.All;
    public bool EnableIdempotence { get; init; } = true;
    public CompressionType? CompressionType { get; init; }
    public int ConsumerCount { get; init; } = 1;
    public int HandlerMinLatencyMs { get; init; } = 10;
    public int HandlerMaxLatencyMs { get; init; } = 50;
    public CommitMode CommitMode { get; init; } = CommitMode.Manual;
    public AutoOffsetReset AutoOffsetReset { get; init; } = AutoOffsetReset.Earliest;
    public int ProducerRecreateIntervalSec { get; init; }
    public int ConsumerRecreateIntervalSec { get; init; }
    public int IdleWindowSec { get; init; } = 0;
    public int RunDurationSec { get; init; } = 300;
    public int MetadataMaxAgeMs { get; init; } = 180_000;
    public int SocketTimeoutMs { get; init; } = 60_000;
    public int RequestTimeoutMs { get; init; } = 30_000;
    public long ConnectionsMaxIdleMs { get; init; } = 540_000;
    public ScenarioKind Scenario { get; init; } = ScenarioKind.Mixed;
    public AppLogLevel LogLevel { get; init; } = AppLogLevel.Info;
    public int StatsIntervalSec { get; init; } = 30;
    public bool OutputJson { get; init; }
    public int ErrorSampleLimit { get; init; } = 20;

    public bool UsesBurst => Scenario is ScenarioKind.Burst or ScenarioKind.Mixed;
    public bool UsesProducerRecycle => Scenario is ScenarioKind.Recycle or ScenarioKind.Mixed;
    public bool UsesConsumerRecycle => Scenario is ScenarioKind.Recycle or ScenarioKind.Rebalance or ScenarioKind.Mixed;
    public bool UsesIdleWindow => Scenario == ScenarioKind.Mixed && IdleWindowSec > 0;
    public bool UsesRebalance => Scenario is ScenarioKind.Rebalance or ScenarioKind.Mixed;

    public bool ProducerTrafficEnabled(DateTimeOffset now)
    {
        if (!UsesIdleWindow)
        {
            return true;
        }

        var start = ProcessStartUtc ?? now;
        var elapsed = now - start;
        var window = (int)(elapsed.TotalSeconds / Math.Max(1, IdleWindowSec));
        return window % 2 == 0;
    }

    public double GetCurrentRate(DateTimeOffset now)
    {
        if (!UsesBurst || BurstIntervalSec <= 0)
        {
            return ProduceRate;
        }

        var start = ProcessStartUtc ?? now;
        var elapsedSec = (long)(now - start).TotalSeconds;
        var withinCycle = elapsedSec % BurstIntervalSec;
        return withinCycle == 0 ? Math.Max(BurstRate, ProduceRate) : ProduceRate;
    }

    public double GetCurrentRatePerProducer(DateTimeOffset now)
    {
        var producerCount = Math.Max(1, ProducerCount);
        return GetCurrentRate(now) / producerCount;
    }

    private DateTimeOffset? _processStartUtc;
    public DateTimeOffset? ProcessStartUtc
    {
        get => _processStartUtc;
        set => _processStartUtc ??= value;
    }

    public static AppOptions Parse(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < args.Length; i++)
        {
            var current = args[i];
            if (!current.StartsWith("--", StringComparison.Ordinal))
            {
                continue;
            }

            var key = current[2..];
            if (key is "help" or "h")
            {
                values[key] = "true";
                continue;
            }

            if (i + 1 >= args.Length || args[i + 1].StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException($"Missing value for --{key}");
            }

            values[key] = args[++i];
        }

        var showHelp = values.ContainsKey("help") || values.ContainsKey("h");
        var options = new AppOptions
        {
            ShowHelp = showHelp,
            BootstrapServers = Get(values, "bootstrap-servers", string.Empty),
            Topic = Get(values, "topic", string.Empty),
            GroupId = Get(values, "group-id", "ksn-disconnect"),
            PartitionsHint = Get(values, "partitions-hint", 1),
            ClientIdPrefix = Get(values, "client-id-prefix", "ksn-disconnect"),
            SecurityProtocol = GetEnumNullable<SecurityProtocol>(values, "security-protocol"),
            SaslMechanism = GetEnumNullable<SaslMechanism>(values, "sasl-mechanism"),
            SaslUsername = GetNullable(values, "sasl-username"),
            SaslPassword = GetNullable(values, "sasl-password"),
            SslCaLocation = GetNullable(values, "ssl-ca-location"),
            ProducerCount = Get(values, "producer-count", 1),
            ProduceRate = Get(values, "produce-rate", 2d),
            BurstRate = Get(values, "burst-rate", 30d),
            BurstIntervalSec = Get(values, "burst-interval-sec", 20),
            MessageBytes = Get(values, "message-bytes", 512),
            LingerMs = Get(values, "linger-ms", 20),
            BatchSize = Get(values, "batch-size", 131072),
            Acks = GetEnum(values, "acks", Acks.All),
            EnableIdempotence = Get(values, "enable-idempotence", true),
            CompressionType = GetEnumNullable<CompressionType>(values, "compression-type"),
            ConsumerCount = Get(values, "consumer-count", 1),
            HandlerMinLatencyMs = Get(values, "handler-min-latency-ms", 10),
            HandlerMaxLatencyMs = Get(values, "handler-max-latency-ms", 50),
            CommitMode = GetEnum(values, "commit-mode", CommitMode.Manual),
            AutoOffsetReset = GetEnum(values, "auto-offset-reset", AutoOffsetReset.Earliest),
            ProducerRecreateIntervalSec = Get(values, "producer-recreate-interval-sec", 0),
            ConsumerRecreateIntervalSec = Get(values, "consumer-recreate-interval-sec", 0),
            IdleWindowSec = Get(values, "idle-window-sec", 0),
            RunDurationSec = Get(values, "run-duration-sec", 300),
            MetadataMaxAgeMs = Get(values, "metadata-max-age-ms", 180000),
            SocketTimeoutMs = Get(values, "socket-timeout-ms", 60000),
            RequestTimeoutMs = Get(values, "request-timeout-ms", 30000),
            ConnectionsMaxIdleMs = Get(values, "connections-max-idle-ms", 540000L),
            Scenario = GetEnum(values, "scenario", ScenarioKind.Mixed),
            LogLevel = GetEnum(values, "log-level", AppLogLevel.Info),
            StatsIntervalSec = Get(values, "stats-interval-sec", 30),
            OutputJson = Get(values, "output-json", false),
            ErrorSampleLimit = Get(values, "error-sample-limit", 20),
        };

        if (!options.ShowHelp)
        {
            if (string.IsNullOrWhiteSpace(options.BootstrapServers))
            {
                throw new ArgumentException("--bootstrap-servers is required.");
            }

            if (string.IsNullOrWhiteSpace(options.Topic))
            {
                throw new ArgumentException("--topic is required.");
            }
        }

        if (options.HandlerMaxLatencyMs < options.HandlerMinLatencyMs)
        {
            throw new ArgumentException("--handler-max-latency-ms must be >= --handler-min-latency-ms.");
        }

        return options;
    }

    public static string GetHelpText() =>
        """
        Kafka .NET 断连复现工具

        Required:
          --bootstrap-servers <host:port,...>
          --topic <topic>

        Common:
          --group-id <group-id>
          --scenario steady|burst|recycle|rebalance|mixed
          --producer-count <n>
          --consumer-count <n>
          --produce-rate <messages/sec>
          --burst-rate <messages/sec>
          --burst-interval-sec <seconds>
          --message-bytes <bytes>
          --run-duration-sec <seconds>
          --producer-recreate-interval-sec <seconds>
          --consumer-recreate-interval-sec <seconds>
          --idle-window-sec <seconds>
          --commit-mode auto|manual
          --auto-offset-reset earliest|latest
          --output-json true|false

        Security:
          --security-protocol <Plaintext|Ssl|SaslPlaintext|SaslSsl>
          --sasl-mechanism <Plain|ScramSha256|ScramSha512>
          --sasl-username <value>
          --sasl-password <value>
          --ssl-ca-location <path>

        Example:
          dotnet run -- \
            --bootstrap-servers localhost:9092 \
            --topic test.disconnect \
            --group-id test.disconnect.group \
            --scenario mixed \
            --producer-count 2 \
            --consumer-count 3 \
            --produce-rate 5 \
            --burst-rate 80 \
            --burst-interval-sec 15 \
            --producer-recreate-interval-sec 20 \
            --consumer-recreate-interval-sec 25 \
            --idle-window-sec 30
        """;

    private static string? GetNullable(IReadOnlyDictionary<string, string> values, string key) =>
        values.TryGetValue(key, out var value) ? value : null;

    private static string Get(IReadOnlyDictionary<string, string> values, string key, string defaultValue) =>
        values.TryGetValue(key, out var value) ? value : defaultValue;

    private static int Get(IReadOnlyDictionary<string, string> values, string key, int defaultValue) =>
        values.TryGetValue(key, out var value) ? int.Parse(value, CultureInfo.InvariantCulture) : defaultValue;

    private static long Get(IReadOnlyDictionary<string, string> values, string key, long defaultValue) =>
        values.TryGetValue(key, out var value) ? long.Parse(value, CultureInfo.InvariantCulture) : defaultValue;

    private static double Get(IReadOnlyDictionary<string, string> values, string key, double defaultValue) =>
        values.TryGetValue(key, out var value) ? double.Parse(value, CultureInfo.InvariantCulture) : defaultValue;

    private static bool Get(IReadOnlyDictionary<string, string> values, string key, bool defaultValue) =>
        values.TryGetValue(key, out var value) ? bool.Parse(value) : defaultValue;

    private static T GetEnum<T>(IReadOnlyDictionary<string, string> values, string key, T defaultValue) where T : struct, Enum =>
        values.TryGetValue(key, out var value) ? Enum.Parse<T>(value, ignoreCase: true) : defaultValue;

    private static T? GetEnumNullable<T>(IReadOnlyDictionary<string, string> values, string key) where T : struct, Enum =>
        values.TryGetValue(key, out var value) ? Enum.Parse<T>(value, ignoreCase: true) : null;
}
