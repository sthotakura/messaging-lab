namespace messaging_lab.solace.fw.subscribe;

public interface IMessageHandler<in T>
{
    bool Handle(T message);
}