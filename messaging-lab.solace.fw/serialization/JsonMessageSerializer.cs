using System.Text.Json;

namespace messaging_lab.solace.fw.serialization;

public class JsonMessageSerializer<T> : IMessageSerializer<T>
{
    public string Serialize(T message) => JsonSerializer.Serialize(message);
}
