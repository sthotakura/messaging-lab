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
using SolaceSystems.Solclient.Messaging;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.Configure<SolaceOptions>(builder.Configuration.GetSection("Solace"));
builder.Services.Configure<LoadGenOptions>(builder.Configuration.GetSection("LoadGen"));

using var host = builder.Build();

var solaceOptions = host.Services.GetRequiredService<IOptions<SolaceOptions>>().Value;
var loadGenOptionsAccessor = host.Services.GetRequiredService<IOptions<LoadGenOptions>>();
var loadGenOptions = loadGenOptionsAccessor.Value;
var logger = host.Services.GetRequiredService<ILogger<Program>>();

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

for (var i = 0; i < loadGenOptions.Count; i++)
{
    var keyIndex = i % loadGenOptions.KeyCount;
    var sequence = ++perKeySequence[keyIndex];
    var message = new OrderPlaced($"order-{keyIndex}", 10.00m + keyIndex, sequence, DateTime.UtcNow);
    publisher.Publish(message);
}

stopwatch.Stop();
session.Disconnect();

logger.LogInformation(
    "Published {Count} messages in {Elapsed:g} ({Rate:F1} msgs/sec)",
    loadGenOptions.Count, stopwatch.Elapsed, loadGenOptions.Count / stopwatch.Elapsed.TotalSeconds);
