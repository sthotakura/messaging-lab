using messaging_lab.orders;
using messaging_lab.solace.fw.subscribe;

namespace messaging_lab.solace.subscriber.Orders;

public sealed class OrderKeySelector : IMessageKeySelector<OrderPlaced>
{
    public string GetKey(OrderPlaced message) => message.OrderId;
}
