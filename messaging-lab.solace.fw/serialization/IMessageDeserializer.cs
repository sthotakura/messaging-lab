namespace messaging_lab.solace.fw.serialization;

public interface IMessageDeserializer<out T>
{
    T Deserialize(string message);
}