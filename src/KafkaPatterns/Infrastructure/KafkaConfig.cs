using Confluent.Kafka;

namespace KafkaPatterns.Infrastructure;

public static class KafkaConfig
{
    public static string BootstrapServers =>
        Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092";

    public static ProducerConfig Producer(Action<ProducerConfig>? tune = null)
    {
        var cfg = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,               // wait for full ISR ack
            EnableIdempotence = true,      // exactly-once delivery
            MessageSendMaxRetries = 3,     // retry transient failures
            MessageTimeoutMs = 30_000,    // wait for retries
            RequestTimeoutMs = 10_000,     // wait for broker response
            MaxInFlight = 5,                // allow pipelining of requests
            LingerMs = 5,                  // small batching window
            CompressionType = CompressionType.Lz4
        };
        tune?.Invoke(cfg);
        return cfg;
    }

    public static ConsumerConfig Consumer(string groupId, Action<ConsumerConfig>? tune = null)
    {
        var cfg = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,      // commit only after successful processing
            SessionTimeoutMs = 10_000,
            MaxPollIntervalMs = 300_000
        };
        tune?.Invoke(cfg);
        return cfg;
    }
}
