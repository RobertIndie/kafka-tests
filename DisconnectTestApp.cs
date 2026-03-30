using Confluent.Kafka;

namespace ksn_disconnect;

internal sealed class DisconnectTestApp
{
    private readonly AppOptions _options;
    private readonly AppLogger _logger;
    private readonly MetricsCollector _metrics;

    public DisconnectTestApp(AppOptions options, AppLogger logger, MetricsCollector metrics)
    {
        _options = options;
        _logger = logger;
        _metrics = metrics;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _options.ProcessStartUtc = DateTimeOffset.UtcNow;
        _logger.Info("app", "start", "Kafka disconnect test started.", new Dictionary<string, object?>
        {
            ["scenario"] = _options.Scenario,
            ["topic"] = _options.Topic,
            ["groupId"] = _options.GroupId,
            ["producerCount"] = _options.ProducerCount,
            ["consumerCount"] = _options.ConsumerCount,
            ["partitionsHint"] = _options.PartitionsHint,
        });

        var tasks = new List<Task>();
        for (var i = 0; i < _options.ProducerCount; i++)
        {
            var worker = new ProducerWorker(i, _options, _logger, _metrics);
            tasks.Add(Task.Run(() => worker.RunAsync(cancellationToken), cancellationToken));
        }

        for (var i = 0; i < _options.ConsumerCount; i++)
        {
            var worker = new ConsumerWorker(i, _options, _logger, _metrics);
            tasks.Add(Task.Run(() => worker.RunAsync(cancellationToken), cancellationToken));
        }

        tasks.Add(Task.Run(() => ReportLoopAsync(cancellationToken), cancellationToken));

        await Task.WhenAll(tasks);
    }

    private async Task ReportLoopAsync(CancellationToken cancellationToken)
    {
        var interval = TimeSpan.FromSeconds(Math.Max(1, _options.StatsIntervalSec));
        using var timer = new PeriodicTimer(interval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                var snapshot = _metrics.Snapshot(_options);
                _logger.Info("app", "summary", "Intermediate summary.", new Dictionary<string, object?>
                {
                    ["severity"] = snapshot.Severity,
                    ["producedSucceeded"] = snapshot.ProducedSucceeded,
                    ["producedFailed"] = snapshot.ProducedFailed,
                    ["consumed"] = snapshot.Consumed,
                    ["commitFailed"] = snapshot.CommitFailed,
                    ["producerRecreated"] = snapshot.ProducerRecreated,
                    ["consumerRecreated"] = snapshot.ConsumerRecreated,
                    ["rebalances"] = snapshot.Rebalances,
                });
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            var snapshot = _metrics.Snapshot(_options);
            if (_options.OutputJson)
            {
                Console.WriteLine(snapshot.ToJson());
            }
            else
            {
                _logger.Info("app", "final-summary", "Run finished.", new Dictionary<string, object?>
                {
                    ["severity"] = snapshot.Severity,
                    ["producedAttempted"] = snapshot.ProducedAttempted,
                    ["producedSucceeded"] = snapshot.ProducedSucceeded,
                    ["producedFailed"] = snapshot.ProducedFailed,
                    ["consumed"] = snapshot.Consumed,
                    ["commitSucceeded"] = snapshot.CommitSucceeded,
                    ["commitFailed"] = snapshot.CommitFailed,
                    ["producerRecreated"] = snapshot.ProducerRecreated,
                    ["consumerRecreated"] = snapshot.ConsumerRecreated,
                    ["rebalances"] = snapshot.Rebalances,
                    ["fatalErrors"] = snapshot.FatalErrors,
                    ["statsEvents"] = snapshot.StatsEvents,
                    ["errorCounts"] = string.Join("; ", snapshot.ErrorCounts.Select(kv => $"{kv.Key}={kv.Value}")),
                    ["suspiciousSignals"] = snapshot.SuspiciousSignals.Count == 0
                        ? "none"
                        : string.Join("; ", snapshot.SuspiciousSignals),
                });
            }
        }
    }
}

internal sealed class ProducerWorker
{
    private readonly int _workerId;
    private readonly AppOptions _options;
    private readonly AppLogger _logger;
    private readonly MetricsCollector _metrics;

    public ProducerWorker(int workerId, AppOptions options, AppLogger logger, MetricsCollector metrics)
    {
        _workerId = workerId;
        _options = options;
        _logger = logger;
        _metrics = metrics;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var tokenBucket = 0d;
        var sequence = 0L;
        var lastTick = DateTimeOffset.UtcNow;
        var createdAt = DateTimeOffset.UtcNow;
        IProducer<string, string>? producer = null;
        var generation = 0;

        try
        {
            producer = BuildProducer(++generation);
            createdAt = DateTimeOffset.UtcNow;

            while (!cancellationToken.IsCancellationRequested)
            {
                if (ShouldRecreate(createdAt))
                {
                    producer.Flush(TimeSpan.FromSeconds(5));
                    producer.Dispose();
                    _metrics.RecordProducerRecreated();
                    producer = BuildProducer(++generation);
                    createdAt = DateTimeOffset.UtcNow;
                }

                var now = DateTimeOffset.UtcNow;
                var elapsed = (now - lastTick).TotalSeconds;
                lastTick = now;
                var enabled = _options.ProducerTrafficEnabled(now);
                var rate = enabled ? _options.GetCurrentRate(now) : 0d;
                tokenBucket += elapsed * rate;

                if (!enabled)
                {
                    await Task.Delay(250, cancellationToken);
                    continue;
                }

                var messagesToSend = (int)Math.Floor(tokenBucket);
                if (messagesToSend == 0)
                {
                    await Task.Delay(100, cancellationToken);
                    continue;
                }

                tokenBucket -= messagesToSend;
                for (var i = 0; i < messagesToSend; i++)
                {
                    sequence++;
                    var payload = PayloadFactory.Create(_options, _workerId, sequence);
                    _metrics.RecordProduceAttempt();
                    try
                    {
                        var result = await producer.ProduceAsync(_options.Topic, payload, cancellationToken);
                        _metrics.RecordProduceSuccess();
                        _logger.Debug(WorkerName, "produce-ok", "Produced message.", new Dictionary<string, object?>
                        {
                            ["generation"] = generation,
                            ["sequence"] = sequence,
                            ["partition"] = result.Partition.Value,
                            ["offset"] = result.Offset.Value,
                        });
                    }
                    catch (ProduceException<string, string> ex)
                    {
                        _metrics.RecordProduceFailure();
                        HandleKafkaError("produce-failed", ex.Error, ex.Message, generation);
                    }
                    catch (KafkaException ex)
                    {
                        _metrics.RecordProduceFailure();
                        HandleKafkaError("produce-kafka-exception", ex.Error, ex.Message, generation);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            if (producer is not null)
            {
                try
                {
                    producer.Flush(TimeSpan.FromSeconds(5));
                }
                catch
                {
                }

                producer.Dispose();
            }
        }
    }

    private string WorkerName => $"producer-{_workerId}";

    private bool ShouldRecreate(DateTimeOffset createdAt) =>
        _options.UsesProducerRecycle &&
        _options.ProducerRecreateIntervalSec > 0 &&
        (DateTimeOffset.UtcNow - createdAt).TotalSeconds >= _options.ProducerRecreateIntervalSec;

    private IProducer<string, string> BuildProducer(int generation)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = $"{_options.ClientIdPrefix}-producer-{_workerId}-g{generation}",
            SecurityProtocol = _options.SecurityProtocol,
            SaslMechanism = _options.SaslMechanism,
            SaslUsername = _options.SaslUsername,
            SaslPassword = _options.SaslPassword,
            SslCaLocation = _options.SslCaLocation,
            MetadataMaxAgeMs = _options.MetadataMaxAgeMs,
            SocketTimeoutMs = _options.SocketTimeoutMs,
            RequestTimeoutMs = _options.RequestTimeoutMs,
            ConnectionsMaxIdleMs = (int)_options.ConnectionsMaxIdleMs,
            StatisticsIntervalMs = _options.StatsIntervalSec * 1000,
            Acks = _options.Acks,
            LingerMs = _options.LingerMs,
            BatchSize = _options.BatchSize,
            EnableIdempotence = _options.EnableIdempotence,
            CompressionType = _options.CompressionType,
        };

        var builder = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, error) => HandleKafkaError("error", error, error.Reason, generation))
            .SetLogHandler((_, message) => HandleLog(message.Level.ToString(), message.Message, generation))
            .SetStatisticsHandler((_, statistics) =>
            {
                _metrics.RecordStatsEvent();
                _logger.Trace(WorkerName, "statistics", "Producer statistics event.", new Dictionary<string, object?>
                {
                    ["generation"] = generation,
                    ["payload"] = statistics,
                });
            });

        var producer = builder.Build();
        _logger.Info(WorkerName, "create", "Created producer client.", new Dictionary<string, object?>
        {
            ["generation"] = generation,
            ["clientId"] = config.ClientId,
        });
        return producer;
    }

    private void HandleLog(string level, string message, int generation) =>
        _logger.Debug(WorkerName, "kafka-log", message, new Dictionary<string, object?>
        {
            ["generation"] = generation,
            ["kafkaLevel"] = level,
        });

    private void HandleKafkaError(string eventName, Error error, string message, int generation)
    {
        _metrics.RecordError(WorkerName, error.Code.ToString(), error.IsFatal, error.IsBrokerError, message);
        var severity = error.IsFatal ? AppLogLevel.Error : AppLogLevel.Warn;
        _logger.Log(severity, WorkerName, eventName, message, new Dictionary<string, object?>
        {
            ["generation"] = generation,
            ["code"] = error.Code,
            ["isFatal"] = error.IsFatal,
            ["isBrokerError"] = error.IsBrokerError,
            ["reason"] = error.Reason,
        });
    }
}

internal sealed class ConsumerWorker
{
    private readonly int _workerId;
    private readonly AppOptions _options;
    private readonly AppLogger _logger;
    private readonly MetricsCollector _metrics;
    private readonly Random _random;

    public ConsumerWorker(int workerId, AppOptions options, AppLogger logger, MetricsCollector metrics)
    {
        _workerId = workerId;
        _options = options;
        _logger = logger;
        _metrics = metrics;
        _random = new Random(Random.Shared.Next());
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        IConsumer<string, string>? consumer = null;
        var generation = 0;
        var createdAt = DateTimeOffset.UtcNow;

        try
        {
            consumer = BuildConsumer(++generation);
            consumer.Subscribe(_options.Topic);
            createdAt = DateTimeOffset.UtcNow;

            while (!cancellationToken.IsCancellationRequested)
            {
                if (ShouldRecreate(createdAt))
                {
                    consumer.Close();
                    consumer.Dispose();
                    _metrics.RecordConsumerRecreated();
                    consumer = BuildConsumer(++generation);
                    consumer.Subscribe(_options.Topic);
                    createdAt = DateTimeOffset.UtcNow;
                }

                ConsumeResult<string, string>? result;
                try
                {
                    result = consumer.Consume(TimeSpan.FromMilliseconds(250));
                }
                catch (ConsumeException ex)
                {
                    HandleKafkaError("consume-exception", ex.Error, ex.Message, generation);
                    continue;
                }
                catch (KafkaException ex)
                {
                    HandleKafkaError("consume-kafka-exception", ex.Error, ex.Message, generation);
                    continue;
                }

                if (result is null)
                {
                    continue;
                }

                _metrics.RecordConsumed();
                if (_options.HandlerMaxLatencyMs > 0)
                {
                    var latency = _random.Next(_options.HandlerMinLatencyMs, _options.HandlerMaxLatencyMs + 1);
                    await Task.Delay(latency, cancellationToken);
                }

                if (_options.CommitMode == CommitMode.Manual)
                {
                    try
                    {
                        consumer.Commit(result);
                        _metrics.RecordCommitSuccess();
                    }
                    catch (KafkaException ex)
                    {
                        _metrics.RecordCommitFailure();
                        HandleKafkaError("commit-failed", ex.Error, ex.Message, generation);
                    }
                }

                _logger.Debug(WorkerName, "consume-ok", "Consumed message.", new Dictionary<string, object?>
                {
                    ["generation"] = generation,
                    ["partition"] = result.Partition.Value,
                    ["offset"] = result.Offset.Value,
                    ["key"] = result.Message.Key,
                });
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            if (consumer is not null)
            {
                try
                {
                    consumer.Close();
                }
                catch
                {
                }

                consumer.Dispose();
            }
        }
    }

    private string WorkerName => $"consumer-{_workerId}";

    private bool ShouldRecreate(DateTimeOffset createdAt)
    {
        if (!_options.UsesConsumerRecycle)
        {
            return false;
        }

        var intervalSec = _options.ConsumerRecreateIntervalSec;
        if (intervalSec <= 0 && _options.UsesRebalance)
        {
            intervalSec = 20;
        }

        if (intervalSec <= 0)
        {
            return false;
        }

        var stagger = _options.UsesRebalance ? _workerId * 3 : 0;
        return (DateTimeOffset.UtcNow - createdAt).TotalSeconds >= intervalSec + stagger;
    }

    private IConsumer<string, string> BuildConsumer(int generation)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.GroupId,
            ClientId = $"{_options.ClientIdPrefix}-consumer-{_workerId}-g{generation}",
            SecurityProtocol = _options.SecurityProtocol,
            SaslMechanism = _options.SaslMechanism,
            SaslUsername = _options.SaslUsername,
            SaslPassword = _options.SaslPassword,
            SslCaLocation = _options.SslCaLocation,
            MetadataMaxAgeMs = _options.MetadataMaxAgeMs,
            SocketTimeoutMs = _options.SocketTimeoutMs,
            ConnectionsMaxIdleMs = (int)_options.ConnectionsMaxIdleMs,
            StatisticsIntervalMs = _options.StatsIntervalSec * 1000,
            AutoOffsetReset = _options.AutoOffsetReset,
            EnableAutoCommit = _options.CommitMode == CommitMode.Auto,
            EnableAutoOffsetStore = _options.CommitMode == CommitMode.Auto,
        };

        var builder = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) => HandleKafkaError("error", error, error.Reason, generation))
            .SetLogHandler((_, message) => HandleLog(message.Level.ToString(), message.Message, generation))
            .SetStatisticsHandler((_, statistics) =>
            {
                _metrics.RecordStatsEvent();
                _logger.Trace(WorkerName, "statistics", "Consumer statistics event.", new Dictionary<string, object?>
                {
                    ["generation"] = generation,
                    ["payload"] = statistics,
                });
            })
            .SetPartitionsAssignedHandler((consumer, partitions) =>
            {
                _metrics.RecordRebalance();
                _logger.Info(WorkerName, "partitions-assigned", "Partitions assigned.", new Dictionary<string, object?>
                {
                    ["generation"] = generation,
                    ["partitions"] = string.Join(",", partitions.Select(item => $"{item.Topic}[{item.Partition}]")),
                });
            })
            .SetPartitionsRevokedHandler((consumer, partitions) =>
            {
                _metrics.RecordRebalance();
                _logger.Warn(WorkerName, "partitions-revoked", "Partitions revoked.", new Dictionary<string, object?>
                {
                    ["generation"] = generation,
                    ["partitions"] = string.Join(",", partitions.Select(item => $"{item.Topic}[{item.Partition}]")),
                });
            })
            .SetOffsetsCommittedHandler((_, committed) =>
            {
                if (committed.Error.IsError)
                {
                    _metrics.RecordCommitFailure();
                    HandleKafkaError("offsets-committed-error", committed.Error, committed.Error.Reason, generation);
                    return;
                }

                _metrics.RecordCommitSuccess();
                _logger.Debug(WorkerName, "offsets-committed", "Offsets committed.", new Dictionary<string, object?>
                {
                    ["generation"] = generation,
                    ["count"] = committed.Offsets.Count,
                });
            });

        var consumer = builder.Build();
        _logger.Info(WorkerName, "create", "Created consumer client.", new Dictionary<string, object?>
        {
            ["generation"] = generation,
            ["clientId"] = config.ClientId,
            ["groupId"] = config.GroupId,
        });
        return consumer;
    }

    private void HandleLog(string level, string message, int generation) =>
        _logger.Debug(WorkerName, "kafka-log", message, new Dictionary<string, object?>
        {
            ["generation"] = generation,
            ["kafkaLevel"] = level,
        });

    private void HandleKafkaError(string eventName, Error error, string message, int generation)
    {
        _metrics.RecordError(WorkerName, error.Code.ToString(), error.IsFatal, error.IsBrokerError, message);
        var severity = error.IsFatal ? AppLogLevel.Error : AppLogLevel.Warn;
        _logger.Log(severity, WorkerName, eventName, message, new Dictionary<string, object?>
        {
            ["generation"] = generation,
            ["code"] = error.Code,
            ["isFatal"] = error.IsFatal,
            ["isBrokerError"] = error.IsBrokerError,
            ["reason"] = error.Reason,
        });
    }
}

internal static class PayloadFactory
{
    public static Message<string, string> Create(AppOptions options, int producerId, long sequence)
    {
        var now = DateTimeOffset.UtcNow;
        var body = new BusinessMessage(
            options.RunId,
            producerId,
            sequence,
            now,
            options.MessageBytes,
            options.Scenario.ToString().ToLowerInvariant(),
            CreatePadding(options.MessageBytes));

        return new Message<string, string>
        {
            Key = $"{producerId}-{sequence}",
            Value = System.Text.Json.JsonSerializer.Serialize(body),
        };
    }

    private static string CreatePadding(int targetBytes)
    {
        var size = Math.Max(0, targetBytes - 200);
        return new string('x', size);
    }

    private sealed record BusinessMessage(
        string RunId,
        int ProducerId,
        long Sequence,
        DateTimeOffset CreatedAt,
        int PayloadSize,
        string ScenarioTag,
        string Payload);
}
