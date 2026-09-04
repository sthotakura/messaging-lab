namespace messaging_lab.solace.fw.subscribe;

public interface IMessageSubscriber
{
    void Subscribe();

    void Unsubscribe();
}