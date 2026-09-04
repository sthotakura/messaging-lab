namespace messaging_lab.solace.fw.publish;

public interface IMessagePublisher<in T>
{
    void Publish(T message);
}