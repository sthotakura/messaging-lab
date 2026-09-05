using messaging_lab.solace.fw.publish;
using messaging_lab.solace.loadgen.Configuration;
using Microsoft.Extensions.Options;

namespace messaging_lab.solace.loadgen.Orders;

public sealed class OrderTopicSettings(IOptions<LoadGenOptions> options) : IMessagePublisherSettings
{
    public string Topic => options.Value.Topic;
}
