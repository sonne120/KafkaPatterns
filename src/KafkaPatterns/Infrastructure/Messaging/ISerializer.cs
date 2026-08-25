using System.Text.Json;

namespace KafkaPatterns.Infrastructure.Messaging;

public interface ISerializer
{
    string Serialize<T>(T data) where T : class;

    Result<T> TryDeserialize<T>(string data) where T : class;
}

public sealed class Serializer : ISerializer
{
    public string Serialize<T>(T data) where T : class
        => MessageJson.Serialize(data);

    public T Deserialize<T>(string data) where T : class
        => MessageJson.Deserialize<T>(data)!;

    public Result<T> TryDeserialize<T>(string data) where T : class
    {
        try
        {
            var value = MessageJson.Deserialize<T>(data);
            return value is null
                ? Result.Failure<T>($"payload deserialized to null as {typeof(T).Name}")
                : Result.Success(value);
        }
        catch (JsonException ex)
        {
            return Result.Failure<T>($"malformed {typeof(T).Name} payload: {ex.Message}");
        }
    }
}


public sealed class Serializer<T> : Confluent.Kafka.ISerializer<T>, Confluent.Kafka.IDeserializer<T>
{
    public byte[] Serialize(T data, Confluent.Kafka.SerializationContext context)
        => MessageJson.SerializeToUtf8Bytes(data);

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, Confluent.Kafka.SerializationContext context)
        => isNull ? default! : MessageJson.Deserialize<T>(data)!;
}
