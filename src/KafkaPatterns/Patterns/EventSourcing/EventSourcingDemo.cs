using Confluent.Kafka;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging;

namespace KafkaPatterns.Patterns.EventSourcing;

public record AccountEvent(string AccountId, string Type, decimal Amount, DateTimeOffset At);

public static class EventSourcingDemo
{
    private const string Topic = "bank.account-events";

    public static async Task RunAsync(CancellationToken ct)
    {
        await TopicAdmin.EnsureTopicsAsync(1, Topic); 

        using (var producer = new ProducerBuilder<string, AccountEvent>(KafkaConfig.Producer())
                   .SetValueSerializer(new Serializer<AccountEvent>()).Build())
        {
            var now = DateTimeOffset.UtcNow;
            AccountEvent[] history =
            [
                new("ACC-1", "Opened",    0,   now),
                new("ACC-1", "Deposited", 500, now),
                new("ACC-2", "Opened",    0,   now),
                new("ACC-1", "Withdrawn", 120, now),
                new("ACC-2", "Deposited", 300, now),
                new("ACC-1", "Deposited", 50,  now),
            ];
            foreach (var e in history)
                await producer.ProduceAsync(Topic,
                    new Message<string, AccountEvent> { Key = e.AccountId, Value = e }, ct);
            TopicAdmin.Log("writer", $"{history.Length} events appended");
        }

        var balances = new Dictionary<string, decimal>();
        using var reader = new ConsumerBuilder<string, AccountEvent>(
                KafkaConfig.Consumer($"balance-projector-{Guid.NewGuid():N}"))
            .SetValueDeserializer(new Serializer<AccountEvent>())
            .Build();
        reader.Subscribe(Topic);

        var idleSince = DateTime.UtcNow;
        while (!ct.IsCancellationRequested && DateTime.UtcNow - idleSince < TimeSpan.FromSeconds(3))
        {
            var cr = reader.Consume(TimeSpan.FromMilliseconds(250));
            if (cr is null) continue;
            idleSince = DateTime.UtcNow;

            var e = cr.Message.Value;
            balances.TryGetValue(e.AccountId, out var b);
            balances[e.AccountId] = e.Type switch
            {
                "Deposited" => b + e.Amount,
                "Withdrawn" => b - e.Amount,
                _           => b
            };
            TopicAdmin.Log("projector", $"replay #{cr.Offset.Value}: {e.AccountId} {e.Type} {e.Amount}");
        }
        reader.Close();

        Console.WriteLine("\n--- Rebuilt read model ---");
        foreach (var (acc, bal) in balances.OrderBy(x => x.Key))
            Console.WriteLine($"  {acc}: {bal:F2}");
    }
}
