using System.Text;
using Confluent.Kafka;

namespace KafkaPatterns.Infrastructure.Messaging;

public static class HeaderExtensions
{
    public static Result<byte[]> TryGetBytes(this Headers? headers, string key)
        => headers is not null && headers.TryGetLastBytes(key, out var bytes)
            ? Result.Success(bytes)
            : Result.Failure<byte[]>($"missing '{key}' header");

    public static Result<string> TryGetString(this Headers? headers, string key)
    {
        var bytes = headers.TryGetBytes(key);
        return bytes.IsFailure
            ? Result.Failure<string>(bytes.Error)
            : Result.Success(Encoding.UTF8.GetString(bytes.Value));
    }
}
