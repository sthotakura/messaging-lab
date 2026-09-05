using messaging_lab.orders;
using messaging_lab.solace.fw;
using messaging_lab.solace.fw.serialization;
using messaging_lab.solace.fw.subscribe;
using messaging_lab.solace.subscriber;
using messaging_lab.solace.subscriber.Configuration;
using messaging_lab.solace.subscriber.Metrics;
using messaging_lab.solace.subscriber.Orders;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using SolaceSystems.Solclient.Messaging;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<SolaceOptions>(builder.Configuration.GetSection("Solace"));
builder.Services.Configure<SubscriberOptions>(builder.Configuration.GetSection("Subscriber"));

builder.Services.AddSingleton<SolaceContext>(_ => new SolaceContext());

builder.Services.AddSingleton(sp =>
{
    var context = sp.GetRequiredService<SolaceContext>();
    var options = sp.GetRequiredService<IOptions<SolaceOptions>>().Value;

    var session = context.CreateSession(new SessionProperties
    {
        Host = options.Host,
        VPNName = options.VPNName,
        UserName = options.UserName,
        Password = options.Password,
    });

    // The subscriber singleton below binds a flow at construction time, which requires an
    // already-connected session, so connect here rather than deferring to the hosted service.
    var connectResult = session.Connect();
    if (connectResult != ReturnCode.SOLCLIENT_OK)
    {
        throw new InvalidOperationException($"Failed to connect Solace session: {connectResult}");
    }

    return session;
});

builder.Services.AddSingleton<IMessageDeserializer<OrderPlaced>, JsonMessageDeserializer<OrderPlaced>>();
builder.Services.AddSingleton<IMessageHandler<OrderPlaced>, OrderHandler>();
builder.Services.AddSingleton<IMessageKeySelector<OrderPlaced>, OrderKeySelector>();
builder.Services.AddSingleton<IMessageSubscriberSettings, OrderQueueSettings>();

builder.Services.AddSingleton<ThroughputTracker>();
builder.Services.AddSingleton<OrderingValidator>();
builder.Services.AddHostedService<MetricsReportingService>();

builder.Services.AddSingleton<IMessageSubscriber>(sp =>
{
    var subscriberOptions = sp.GetRequiredService<IOptions<SubscriberOptions>>().Value;
    var session = sp.GetRequiredService<SolaceSession>();
    var settings = sp.GetRequiredService<IMessageSubscriberSettings>();
    var deserializer = sp.GetRequiredService<IMessageDeserializer<OrderPlaced>>();
    var handler = sp.GetRequiredService<IMessageHandler<OrderPlaced>>();

    if (!subscriberOptions.UseConcurrentSubscriber)
    {
        return new SolaceSequentialSubscriber<OrderPlaced>(session, settings, deserializer, handler);
    }

    var keySelector = sp.GetRequiredService<IMessageKeySelector<OrderPlaced>>();
    return new SolaceConcurrentSubscriber<OrderPlaced>(
        session, settings, deserializer, handler, keySelector, subscriberOptions.Concurrency);
});

builder.Services.AddHostedService<SubscriberHostedService>();

var host = builder.Build();
await host.RunAsync();
