using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaPatterns.Infrastructure;

public static class TopicAdmin
{
    public static Task EnsureTopicsAsync(int partitions, params string[] topics)
        => EnsureTopicsAsync(KafkaConfig.BootstrapServers, partitions, topics);

    public static async Task EnsureTopicsAsync(string bootstrapServers, int partitions, params string[] topics)
    {
        using var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

        var specs = topics.Select(t => new TopicSpecification
        {
            Name = t,
            NumPartitions = partitions,
            ReplicationFactor = 1
        }).ToList();

        try
        {
            await admin.CreateTopicsAsync(specs);
        }
        catch (CreateTopicsException e) when (
            e.Results.All(r => r.Error.Code is ErrorCode.TopicAlreadyExists or ErrorCode.NoError))
        {
        }

        await Task.Delay(500);
    }

    public static void Log(string who, string msg)
        => Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{who}] {msg}");
}
