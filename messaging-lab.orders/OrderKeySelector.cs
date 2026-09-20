using messaging_lab.solace.fw;

namespace messaging_lab.orders;

public sealed class OrderKeySelector : IMessageKeySelector<OrderPlaced>
{
    public string GetKey(OrderPlaced message) => message.OrderId;
}
