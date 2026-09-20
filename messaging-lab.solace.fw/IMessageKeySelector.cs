namespace messaging_lab.solace.fw;

/// <summary>
/// Derives an ordering key from a message. Used on the subscribe side to route same-key
/// messages to the same worker lane within a process (see <c>SolaceConcurrentSubscriber&lt;T&gt;</c>),
/// and on the publish side to set the same key as a Solace partitioned-queue partition key
/// (see <c>SolaceMessageSender&lt;T&gt;</c>), so same-key messages stay in order both within
/// a process and across multiple subscriber processes bound to a partitioned queue.
/// </summary>
public interface IMessageKeySelector<in T>
{
    string GetKey(T message);
}
