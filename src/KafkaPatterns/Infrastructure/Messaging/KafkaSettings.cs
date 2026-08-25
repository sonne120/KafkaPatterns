namespace KafkaPatterns.Infrastructure.Messaging;

public class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string ConsumerGroupId { get; set; } = "kafka-patterns-consumer";
    public int MaxInFlight { get; set; } = 1;
    public int MessageSendMaxRetries { get; set; } = 3;
    public int RetryBackoffMs { get; set; } = 1000;
    public int MessageTimeoutMs { get; set; } = 30_000;
}