using Confluent.Kafka;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging;
using KafkaPatterns.Infrastructure.Messaging.Serialization;

namespace KafkaPatterns.Patterns.PubSub;

public record OrderPlaced(Guid OrderId, string Number, decimal Total);


public static class PubSubDemo
{
    private const string Topic = "orders.placed";

    public static async Task RunAsync(CancellationToken ct)
    {
        await TopicAdmin.EnsureTopicsAsync(3, Topic);

        using var demoCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var analytics     = Task.Run(() => Consume("analytics-service", demoCts.Token), CancellationToken.None);
        var notifications = Task.Run(() => Consume("notification-service", demoCts.Token), CancellationToken.None);

        using var producer = new ProducerBuilder<string, OrderPlaced>(KafkaConfig.Producer())
            .SetValueSerializer(new Serializer<OrderPlaced>())
            .Build();

        for (var i = 1; i <= 8 && !ct.IsCancellationRequested; i++)
        {
            var evt = new OrderPlaced(Guid.NewGuid(), $"ORD-{i:D4}", 99.90m * i);
            await producer.ProduceAsync(Topic,
                new Message<string, OrderPlaced> { Key = evt.Number, Value = evt }, ct);
            TopicAdmin.Log("producer", $"published {evt.Number}");
            await Task.Delay(300, ct);
        }

        await Task.Delay(2000, ct);
        await DemoRunner.ShutdownAsync(demoCts, analytics, notifications);
    }

    private static void Consume(string group, CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, OrderPlaced>(KafkaConfig.Consumer(group))
            .SetValueDeserializer(new Serializer<OrderPlaced>())
            .Build();
        consumer.Subscribe(Topic);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                TopicAdmin.Log(group, $"got {cr.Message.Value.Number} (total {cr.Message.Value.Total:F2})");
                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }
}
