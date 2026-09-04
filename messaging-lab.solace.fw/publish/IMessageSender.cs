namespace messaging_lab.solace.fw.publish;

public interface IMessageSender<in T>
{
    void Send(T message);
}