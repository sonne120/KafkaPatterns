using System.Text;
using Confluent.Kafka;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging;

namespace KafkaPatterns.Patterns.Partitioning;

public record TelemetryPoint(string DeviceId, string Region, double Value);

public static class PartitioningDemo
{
    private const string KeyedTopic  = "telemetry.by-device";
    private const string RegionTopic = "telemetry.by-region";

    public static async Task RunAsync(CancellationToken ct)
    {
        await TopicAdmin.EnsureTopicsAsync(4, KeyedTopic, RegionTopic);

        var points = Enumerable.Range(1, 12).Select(i =>
            new TelemetryPoint(
                DeviceId: $"dev-{i % 4}",
                Region: (i % 3) switch { 0 => "eu", 1 => "us", _ => "apac" },
                Value: Math.Round(Random.Shared.NextDouble() * 100, 1))).ToArray();

        // 1) Default: partition = murmur2(key) % partitionCount
        using (var producer = new ProducerBuilder<string, TelemetryPoint>(KafkaConfig.Producer())
                   .SetValueSerializer(new Serializer<TelemetryPoint>()).Build())
        {
            Console.WriteLine("--- key-hash partitioning (key = DeviceId) ---");
            foreach (var p in points)
            {
                var dr = await producer.ProduceAsync(KeyedTopic,
                    new Message<string, TelemetryPoint> { Key = p.DeviceId, Value = p }, ct);
                TopicAdmin.Log("producer", $"{p.DeviceId,-6} -> partition {dr.Partition.Value}");
            }
        }

        // 2) Custom partitioner
        using (var producer = new ProducerBuilder<string, TelemetryPoint>(KafkaConfig.Producer())
                   .SetValueSerializer(new Serializer<TelemetryPoint>())
                   .SetDefaultPartitioner((topic, partitionCount, keyData, keyIsNull) =>
                   {
                       var key = keyIsNull ? "" : Encoding.UTF8.GetString(keyData);
                       return key switch
                       {
                           "eu" => new Partition(0),                       // data residency: EU stays on p0
                           "us" => new Partition(1),
                           _    => new Partition(2 + Math.Abs(key.GetHashCode()) % (partitionCount - 2))
                       };
                   })
                   .Build())
        {
            Console.WriteLine("\n--- custom partitioner (key = Region) ---");
            foreach (var p in points)
            {
                var dr = await producer.ProduceAsync(RegionTopic,
                    new Message<string, TelemetryPoint> { Key = p.Region, Value = p }, ct);
                TopicAdmin.Log("producer", $"{p.Region,-5} -> partition {dr.Partition.Value}");
            }
        }

        Console.WriteLine("\nNote: same key ALWAYS lands on the same partition — that is the ordering guarantee.");
    }
}
