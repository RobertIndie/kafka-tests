using ksn_disconnect;

var options = AppOptions.Parse(args);
if (options.ShowHelp)
{
    Console.WriteLine(AppOptions.GetHelpText());
    return 0;
}

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cts.Cancel();
};

if (options.RunDurationSec > 0)
{
    cts.CancelAfter(TimeSpan.FromSeconds(options.RunDurationSec));
}

var logger = new AppLogger(options.OutputJson, options.LogLevel, options.RunId);
var metrics = new MetricsCollector(options.RunId, options.ErrorSampleLimit);
var app = new DisconnectTestApp(options, logger, metrics);

try
{
    await app.RunAsync(cts.Token);
    return 0;
}
catch (OperationCanceledException)
{
    logger.Info("app", "shutdown", "Cancellation requested.");
    return 0;
}
catch (Exception ex)
{
    logger.Error("app", "fatal", ex.Message, new Dictionary<string, object?>
    {
        ["exception"] = ex.GetType().FullName,
    });
    return 1;
}
