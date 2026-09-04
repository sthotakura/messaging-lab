using System.Text.Json;

namespace messaging_lab.solace.fw.serialization;

public class JsonMessageDeserializer<T> : IMessageDeserializer<T>
{
    public T Deserialize(string message) => JsonSerializer.Deserialize<T>(message)
                                            ?? throw new JsonException(
                                                $"Deserialization of type {typeof(T)} returned null.");
}
