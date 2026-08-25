using Confluent.Kafka;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging;
using KafkaPatterns.Infrastructure.Messaging.Serialization;

namespace KafkaPatterns.Patterns.CompetingConsumers;

public record EmailJob(string To, string Template);

public static class CompetingConsumersDemo
{
    private const string Topic = "emails.outgoing";
    private const string Group = "email-sender";

    public static async Task RunAsync(CancellationToken ct)
    {
        await TopicAdmin.EnsureTopicsAsync(4, Topic);

        using var demoCts  = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var crashCts = CancellationTokenSource.CreateLinkedTokenSource(demoCts.Token);
        var c1 = Task.Run(() => Consume("sender-1", demoCts.Token), CancellationToken.None);
        var c2 = Task.Run(() => Consume("sender-2", demoCts.Token), CancellationToken.None);
        var c3 = Task.Run(() => Consume("sender-3", crashCts.Token), CancellationToken.None); // will die

        using var producer = new ProducerBuilder<string, EmailJob>(KafkaConfig.Producer())
            .SetValueSerializer(new Serializer<EmailJob>()).Build();

        for (var i = 1; i <= 20 && !ct.IsCancellationRequested; i++)
        {
            await producer.ProduceAsync(Topic, new Message<string, EmailJob>
                { Key = $"user{i}", Value = new EmailJob($"user{i}@example.com", "welcome") }, ct);
            await Task.Delay(250, ct);

            if (i == 8)
            {
                TopicAdmin.Log("chaos", ">>> killing sender-3, expect a rebalance <<<");
                crashCts.Cancel();
            }
        }

        await Task.Delay(3000, ct);
        await DemoRunner.ShutdownAsync(demoCts, c1, c2, c3);
    }

    private static void Consume(string name, CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, EmailJob>(KafkaConfig.Consumer(Group))
            .SetValueDeserializer(new Serializer<EmailJob>())
            .SetPartitionsAssignedHandler((_, parts) =>
                TopicAdmin.Log(name, $"assigned: [{string.Join(",", parts.Select(p => p.Partition.Value))}]"))
            .SetPartitionsRevokedHandler((_, parts) =>
                TopicAdmin.Log(name, $"revoked:  [{string.Join(",", parts.Select(p => p.Partition.Value))}]"))
            .Build();

        consumer.Subscribe(Topic);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                TopicAdmin.Log(name, $"sent '{cr.Message.Value.Template}' to {cr.Message.Value.To} (p{cr.Partition.Value})");
                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); } 
    }
}
