using Confluent.Kafka;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging;

namespace KafkaPatterns.Patterns.ParentChild;

public record UserEvent(string UserId, string Action, string? Payload);
public record UserProfile(string UserId, int EventCount, string LastAction);

public static class ParentChildDemo
{
    private const string Parent = "user.events";        // raw
    private const string Child  = "user.profiles";      // derived

    public static async Task RunAsync(CancellationToken ct)
    {
        await TopicAdmin.EnsureTopicsAsync(3, Parent, Child);

        var deriver = Task.Run(() => Deriver(ct), ct);
        var reader  = Task.Run(() => ChildReader(ct), ct);

        using var producer = new ProducerBuilder<string, UserEvent>(KafkaConfig.Producer())
            .SetValueSerializer(new Serializer<UserEvent>()).Build();

        string[] users = ["u-alice", "u-bob", "u-carol"];
        string[] actions = ["login", "view_item", "add_to_cart", "checkout"];
        for (var i = 0; i < 10 && !ct.IsCancellationRequested; i++)
        {
            var e = new UserEvent(users[i % users.Length], actions[i % actions.Length], null);
            await producer.ProduceAsync(Parent,
                new Message<string, UserEvent> { Key = e.UserId, Value = e }, ct); // key = UserId
            await Task.Delay(200, ct);
        }

        await Task.Delay(3000, ct);
        await Task.WhenAny(Task.WhenAll(deriver, reader), Task.Delay(1000));
    }

    private static void Deriver(CancellationToken ct)
    {
        var state = new Dictionary<string, UserProfile>();

        using var consumer = new ConsumerBuilder<string, UserEvent>(KafkaConfig.Consumer("profile-deriver"))
            .SetValueDeserializer(new Serializer<UserEvent>()).Build();
        using var producer = new ProducerBuilder<string, UserProfile>(KafkaConfig.Producer())
            .SetValueSerializer(new Serializer<UserProfile>()).Build();

        consumer.Subscribe(Parent);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                var e = cr.Message.Value;

                var prev = state.GetValueOrDefault(e.UserId, new UserProfile(e.UserId, 0, "-"));
                var next = prev with { EventCount = prev.EventCount + 1, LastAction = e.Action };
                state[e.UserId] = next;

                producer.Produce(Child, new Message<string, UserProfile> { Key = e.UserId, Value = next });
                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }

    private static void ChildReader(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, UserProfile>(KafkaConfig.Consumer("profile-reader"))
            .SetValueDeserializer(new Serializer<UserProfile>()).Build();
        consumer.Subscribe(Child);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                var p = cr.Message.Value;
                TopicAdmin.Log("child-reader", $"{p.UserId}: {p.EventCount} events, last={p.LastAction} (p{cr.Partition.Value})");
                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }
}
