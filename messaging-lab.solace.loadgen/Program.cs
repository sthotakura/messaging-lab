using System.Diagnostics;
using messaging_lab.orders;
using messaging_lab.solace.fw;
using messaging_lab.solace.fw.publish;
using messaging_lab.solace.fw.serialization;
using messaging_lab.solace.loadgen.Configuration;
using messaging_lab.solace.loadgen.Orders;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Serilog;
using SolaceSystems.Solclient.Messaging;

var builder = Host.CreateApplicationBuilder(args);

// Adds a rolling daily log file alongside the default console provider, so a run's log survives
// after the process exits and can be reviewed for anomalies (publish failures, connection errors).
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.File(
        Path.Combine(AppContext.BaseDirectory, "logs", "loadgen-.log"),
        rollingInterval: RollingInterval.Day,
        retainedFileCountLimit: 14,
        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {SourceContext}: {Message:lj}{NewLine}{Exception}")
    .CreateLogger();
builder.Logging.AddSerilog(dispose: true);

builder.Services.Configure<SolaceOptions>(builder.Configuration.GetSection("Solace"));
builder.Services.Configure<LoadGenOptions>(builder.Configuration.GetSection("LoadGen"));

using var host = builder.Build();

var solaceOptions = host.Services.GetRequiredService<IOptions<SolaceOptions>>().Value;
var loadGenOptionsAccessor = host.Services.GetRequiredService<IOptions<LoadGenOptions>>();
var loadGenOptions = loadGenOptionsAccessor.Value;
var logger = host.Services.GetRequiredService<ILogger<Program>>();

// Top-level statements have no host-level try/catch to log an unhandled exception before the
// process exits, so wrap the run explicitly - otherwise a failed publish leaves no trace in the log file.
var published = 0;
try
{
    using var context = new SolaceContext();
    using var session = context.CreateSession(new SessionProperties
    {
        Host = solaceOptions.Host,
        VPNName = solaceOptions.VPNName,
        UserName = solaceOptions.UserName,
        Password = solaceOptions.Password,
    });

    var connectResult = session.Connect();
    if (connectResult != ReturnCode.SOLCLIENT_OK)
    {
        throw new InvalidOperationException($"Failed to connect Solace session: {connectResult}");
    }

    var deliveryMode = loadGenOptions.DeliveryMode == MessageDeliveryModeOption.Persistent
        ? MessageDeliveryMode.Persistent
        : MessageDeliveryMode.NonPersistent;

    using var publisher = new SolacePublisher<OrderPlaced>(
        session, new OrderTopicSettings(loadGenOptionsAccessor), new JsonMessageSerializer<OrderPlaced>(), deliveryMode);

    logger.LogInformation(
        "Publishing {Count} messages across {KeyCount} keys to topic '{Topic}'...",
        loadGenOptions.Count, loadGenOptions.KeyCount, loadGenOptions.Topic);

    var perKeySequence = new long[loadGenOptions.KeyCount];
    var stopwatch = Stopwatch.StartNew();

    for (; published < loadGenOptions.Count; published++)
    {
        var keyIndex = published % loadGenOptions.KeyCount;
        var sequence = ++perKeySequence[keyIndex];
        var message = new OrderPlaced($"order-{keyIndex}", 10.00m + keyIndex, sequence, DateTime.UtcNow);
        publisher.Publish(message);
    }

    stopwatch.Stop();
    session.Disconnect();

    logger.LogInformation(
        "Published {Count} messages in {Elapsed:g} ({Rate:F1} msgs/sec)",
        loadGenOptions.Count, stopwatch.Elapsed, loadGenOptions.Count / stopwatch.Elapsed.TotalSeconds);
}
catch (Exception ex)
{
    logger.LogError(ex, "Load generation failed after publishing {Published} of {Count} messages.", published, loadGenOptions.Count);
    throw;
}
