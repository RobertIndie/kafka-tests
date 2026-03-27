using System.Text.Json;

namespace ksn_disconnect;

internal sealed class AppLogger
{
    private readonly bool _outputJson;
    private readonly AppLogLevel _minimumLevel;
    private readonly string _runId;
    private readonly object _gate = new();

    public AppLogger(bool outputJson, AppLogLevel minimumLevel, string runId)
    {
        _outputJson = outputJson;
        _minimumLevel = minimumLevel;
        _runId = runId;
    }

    public void Trace(string worker, string eventName, string message, Dictionary<string, object?>? fields = null) =>
        Log(AppLogLevel.Trace, worker, eventName, message, fields);

    public void Debug(string worker, string eventName, string message, Dictionary<string, object?>? fields = null) =>
        Log(AppLogLevel.Debug, worker, eventName, message, fields);

    public void Info(string worker, string eventName, string message, Dictionary<string, object?>? fields = null) =>
        Log(AppLogLevel.Info, worker, eventName, message, fields);

    public void Warn(string worker, string eventName, string message, Dictionary<string, object?>? fields = null) =>
        Log(AppLogLevel.Warn, worker, eventName, message, fields);

    public void Error(string worker, string eventName, string message, Dictionary<string, object?>? fields = null) =>
        Log(AppLogLevel.Error, worker, eventName, message, fields);

    public void Log(AppLogLevel level, string worker, string eventName, string message, Dictionary<string, object?>? fields = null)
    {
        if (level < _minimumLevel)
        {
            return;
        }

        var payload = new Dictionary<string, object?>
        {
            ["ts"] = DateTimeOffset.UtcNow,
            ["level"] = level.ToString().ToUpperInvariant(),
            ["runId"] = _runId,
            ["worker"] = worker,
            ["event"] = eventName,
            ["message"] = message,
        };

        if (fields is not null)
        {
            foreach (var item in fields)
            {
                payload[item.Key] = item.Value;
            }
        }

        lock (_gate)
        {
            if (_outputJson)
            {
                Console.WriteLine(JsonSerializer.Serialize(payload));
                return;
            }

            var extras = fields is null || fields.Count == 0
                ? string.Empty
                : " | " + string.Join(", ", fields.Select(kv => $"{kv.Key}={kv.Value}"));
            Console.WriteLine($"{DateTimeOffset.Now:O} [{level.ToString().ToUpperInvariant()}] [{worker}] {eventName} - {message}{extras}");
        }
    }
}
