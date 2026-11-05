using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using messaging_lab.solace.fanout;
using messaging_lab.solace.fanout.Configuration;
using messaging_lab.solace.fw;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Serilog;
using SolaceSystems.Solclient.Messaging;

// Publishes at a fixed rate to a topic fanned out into several queues, and tracks the broker's
// ACK/rejection for every message. A rejection or send failure is logged and counted, never fatal:
// the point of this lab is to watch what the publisher experiences while a queue is full and after
// it recovers. See README > Fan-out and spool quotas.

var builder = Host.CreateApplicationBuilder(args);

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.File(
        Path.Combine(AppContext.BaseDirectory, "logs", "fanout-.log"),
        rollingInterval: RollingInterval.Day,
        retainedFileCountLimit: 14,
        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {SourceContext}: {Message:lj}{NewLine}{Exception}")
    .CreateLogger();
builder.Logging.AddSerilog(dispose: true);

builder.Services.Configure<SolaceOptions>(builder.Configuration.GetSection("Solace"));
builder.Services.Configure<FanoutOptions>(builder.Configuration.GetSection("Fanout"));

using var host = builder.Build();

var solaceOptions = host.Services.GetRequiredService<IOptions<SolaceOptions>>().Value;
var options = host.Services.GetRequiredService<IOptions<FanoutOptions>>().Value;
var logger = host.Services.GetRequiredService<ILogger<Program>>();

if (options.OutputDirectory is not null) Directory.CreateDirectory(options.OutputDirectory);

var tracker = new PublishOutcomeTracker();

using var context = new SolaceContext();
using var session = context.CreateSession(new SessionProperties
{
    Host = solaceOptions.Host,
    VPNName = solaceOptions.VPNName,
    UserName = solaceOptions.UserName,
    Password = solaceOptions.Password,
});

session.SessionEventOccurred += (_, sessionEvent) => tracker.OnSessionEvent(sessionEvent);

var connectResult = session.Connect();
if (connectResult != ReturnCode.SOLCLIENT_OK)
{
    throw new InvalidOperationException($"Failed to connect Solace session: {connectResult}");
}

using var topic = ContextFactory.Instance.CreateTopic(options.Topic);
var padding = new string('x', options.PayloadBytes);

logger.LogInformation(
    "Publishing to '{Topic}' at {Rate} msgs/sec for {Duration}s ({Payload} byte padding).",
    options.Topic, options.RatePerSecond, options.DurationSeconds, options.PayloadBytes);

using var stopReporting = new CancellationTokenSource();
var reporting = Task.Run(() => ReportEverySecond(stopReporting.Token));

var stopwatch = Stopwatch.StartNew();
var duration = TimeSpan.FromSeconds(options.DurationSeconds);
long sequence = 0;

while (stopwatch.Elapsed < duration)
{
    var due = (long)(stopwatch.Elapsed.TotalSeconds * options.RatePerSecond);
    if (sequence >= due)
    {
        Thread.Sleep(1);
        continue;
    }

    sequence++;
    try
    {
        PublishOne(sequence);
        tracker.RecordSent();
    }
    catch (Exception ex)
    {
        tracker.RecordSendFailure();
        logger.LogError(ex, "Publish of sequence {Sequence} failed; continuing.", sequence);
    }
}

// Let the outstanding verdicts arrive: disconnecting now would lose them, and they're the result.
var settle = Stopwatch.StartNew();
while (tracker.Outstanding > 0 && settle.Elapsed.TotalSeconds < options.SettleTimeoutSeconds)
{
    Thread.Sleep(50);
}

await stopReporting.CancelAsync();
await reporting;

session.Disconnect();

logger.LogInformation(
    "Finished: sent={Sent} acked={Acked} rejected={Rejected} sendFailures={SendFailures} noVerdict={Outstanding}",
    tracker.Sent, tracker.Acked, tracker.Rejected, tracker.SendFailures, tracker.Outstanding);
foreach (var (reason, count) in tracker.RejectionReasons)
{
    logger.LogWarning("Rejected {Count} messages: {Reason}", count, reason);
}

WriteSummary();
return;

void PublishOne(long messageSequence)
{
    using var message = ContextFactory.Instance.CreateMessage();
    message.Destination = topic;
    message.DeliveryMode = MessageDeliveryMode.Persistent;
    message.CorrelationKey = messageSequence;
    message.BinaryAttachment = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(
        new FanoutOrder("order-0", 10.00m, messageSequence, DateTime.UtcNow, padding)));

    // A full publisher window returns WOULD_BLOCK; waiting here is what bounds the in-flight count.
    ReturnCode returnCode;
    while ((returnCode = session.Send(message)) == ReturnCode.SOLCLIENT_WOULD_BLOCK)
    {
        Thread.Sleep(1);
    }

    if (returnCode != ReturnCode.SOLCLIENT_OK)
    {
        throw new InvalidOperationException($"Send returned {returnCode}");
    }
}

async Task ReportEverySecond(CancellationToken cancellation)
{
    StreamWriter? csv = null;
    if (options.OutputDirectory is not null)
    {
        csv = new StreamWriter(Path.Combine(options.OutputDirectory, "publisher.csv")) { AutoFlush = true };
        await csv.WriteLineAsync("utcMs,sent,acked,rejected,sendFailures");
    }

    long lastAcked = 0, lastRejected = 0;
    using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
    try
    {
        while (await timer.WaitForNextTickAsync(cancellation))
        {
            var (sent, acked, rejected, failures) = (tracker.Sent, tracker.Acked, tracker.Rejected, tracker.SendFailures);

            csv?.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()},{sent},{acked},{rejected},{failures}"));

            if (rejected > lastRejected)
            {
                logger.LogWarning(
                    "Broker rejected {Rejected} messages in the last second ({Acked} acked), {TotalRejected} rejected in total.",
                    rejected - lastRejected, acked - lastAcked, rejected);
            }

            (lastAcked, lastRejected) = (acked, rejected);
        }
    }
    catch (OperationCanceledException)
    {
        // Expected on shutdown.
    }
    finally
    {
        if (csv is not null) await csv.DisposeAsync();
    }
}

void WriteSummary()
{
    if (options.OutputDirectory is null) return;

    File.WriteAllLines(
        Path.Combine(options.OutputDirectory, "rejected-sequences.txt"),
        tracker.RejectedSequences.Select(s => s.ToString(CultureInfo.InvariantCulture)));

    File.WriteAllText(
        Path.Combine(options.OutputDirectory, "publisher-summary.json"),
        JsonSerializer.Serialize(new
        {
            tracker.Sent,
            tracker.Acked,
            tracker.Rejected,
            tracker.SendFailures,
            NoVerdict = tracker.Outstanding,
            tracker.FirstRejectedUtc,
            tracker.LastRejectedUtc,
            tracker.LastAckedUtc,
            Reasons = tracker.RejectionReasons,
        }));
}
