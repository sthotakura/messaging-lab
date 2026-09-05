using messaging_lab.solace.fw.subscribe;
using messaging_lab.solace.subscriber.Configuration;
using Microsoft.Extensions.Options;

namespace messaging_lab.solace.subscriber.Orders;

public sealed class OrderQueueSettings(IOptions<SubscriberOptions> options) : IMessageSubscriberSettings
{
    public string Queue => options.Value.Queue;
}
