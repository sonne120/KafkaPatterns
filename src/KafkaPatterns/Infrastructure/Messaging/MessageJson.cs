using System.Text.Json;
using System.Text.Json.Serialization;

namespace KafkaPatterns.Infrastructure.Messaging;

public static class MessageJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        Converters = { new DomainEventConverter() }
    };

    public static string Serialize<T>(T value) => JsonSerializer.Serialize(value, Options);

    public static byte[] SerializeToUtf8Bytes<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, Options);

    public static T? Deserialize<T>(string json) => JsonSerializer.Deserialize<T>(json, Options);

    public static T? Deserialize<T>(ReadOnlySpan<byte> utf8Json) => JsonSerializer.Deserialize<T>(utf8Json, Options);
}

public sealed class DomainEventConverter : JsonConverter<IDomainEvent>
{
    private const string Discriminator = "$type";

    private static readonly Dictionary<string, Type> AllowList =
        typeof(DomainEventConverter).Assembly
            .GetTypes()
            .Where(t => typeof(IDomainEvent).IsAssignableFrom(t) && t is { IsAbstract: false, IsInterface: false })
            .ToDictionary(t => t.Name, t => t, StringComparer.Ordinal);

    public override IDomainEvent? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var document = JsonDocument.ParseValue(ref reader);

        if (!document.RootElement.TryGetProperty(Discriminator, out var discriminator) ||
            discriminator.GetString() is not { } typeName)
        {
            throw new JsonException($"Domain event payload is missing its '{Discriminator}' discriminator.");
        }

        if (!AllowList.TryGetValue(typeName, out var concreteType))
        {
            throw new JsonException($"'{typeName}' is not an allow-listed domain event; refusing to deserialize.");
        }

        return (IDomainEvent?)document.RootElement.Deserialize(concreteType, options);
    }

    public override void Write(Utf8JsonWriter writer, IDomainEvent value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString(Discriminator, value.GetType().Name);

        using var document = JsonSerializer.SerializeToDocument(value, value.GetType(), options);
        foreach (var property in document.RootElement.EnumerateObject())
            property.WriteTo(writer);

        writer.WriteEndObject();
    }
}
