namespace messaging_lab.solace.fw.serialization;

public interface IMessageSerializer<in T>
{
    string Serialize(T message);
}