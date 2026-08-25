using Confluent.Kafka;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging;

namespace KafkaPatterns.Patterns.Saga;

public record SagaMessage(string OrderId, string Step, string Status, string? Reason = null);

public static class SagaDemo
{
    private const string Orders    = "saga.orders";
    private const string Payments  = "saga.payments";
    private const string Inventory = "saga.inventory";
    private const string Results   = "saga.results";

    public static async Task RunAsync(CancellationToken ct)
    {
        await TopicAdmin.EnsureTopicsAsync(1, Orders, Payments, Inventory, Results);

        using var demoCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var payment   = Task.Run(() => PaymentService(demoCts.Token), CancellationToken.None);
        var inventory = Task.Run(() => InventoryService(demoCts.Token), CancellationToken.None);
        var orderSvc  = Task.Run(() => OrderService(demoCts.Token), CancellationToken.None);

        using var producer = Producer<SagaMessage>();
        foreach (var (id, ok) in new[] { ("ORD-1", true), ("ORD-2", false) })
        {
            await producer.ProduceAsync(Orders, new Message<string, SagaMessage>
                { Key = id, Value = new SagaMessage(id, "order.created", ok ? "in-stock-item" : "rare-item") }, ct);
            TopicAdmin.Log("order-service", $"{id} created, saga started");
        }

        await Task.Delay(6000, ct);
        await DemoRunner.ShutdownAsync(demoCts, payment, inventory, orderSvc);
    }

    private static void PaymentService(CancellationToken ct)
    {
        using var consumer = Consumer("payment-service", Orders, Inventory);
        using var producer = Producer<SagaMessage>();
        Loop(consumer, cr =>
        {
            var m = cr.Message.Value;
            switch (m.Step)
            {
                case "order.created":
                    TopicAdmin.Log("payment-service", $"{m.OrderId}: charged card");
                    producer.Produce(Payments, Msg(m.OrderId, new SagaMessage(m.OrderId, "payment.captured", m.Status)));
                    break;
                case "inventory.failed":
                    TopicAdmin.Log("payment-service", $"{m.OrderId}: COMPENSATING — refund issued");
                    producer.Produce(Results, Msg(m.OrderId, new SagaMessage(m.OrderId, "order.cancelled", "refunded", m.Reason)));
                    break;
            }
        }, ct);
    }
    private static void InventoryService(CancellationToken ct)
    {
        using var consumer = Consumer("inventory-service", Payments);
        using var producer = Producer<SagaMessage>();
        Loop(consumer, cr =>
        {
            var m = cr.Message.Value;
            if (m.Step != "payment.captured") return;

            if (m.Status == "rare-item")
            {
                TopicAdmin.Log("inventory-service", $"{m.OrderId}: reservation FAILED (out of stock)");
                producer.Produce(Inventory, Msg(m.OrderId, new SagaMessage(m.OrderId, "inventory.failed", "failed", "out of stock")));
            }
            else
            {
                TopicAdmin.Log("inventory-service", $"{m.OrderId}: stock reserved");
                producer.Produce(Results, Msg(m.OrderId, new SagaMessage(m.OrderId, "order.completed", "ok")));
            }
        }, ct);
    }

    private static void OrderService(CancellationToken ct)
    {
        using var consumer = Consumer("order-service", Results);
        Loop(consumer, cr =>
        {
            var m = cr.Message.Value;
            TopicAdmin.Log("order-service", $"{m.OrderId} FINAL: {m.Step} ({m.Status}{(m.Reason is null ? "" : $", {m.Reason}")})");
        }, ct);
    }

    private static Message<string, SagaMessage> Msg(string key, SagaMessage v) => new() { Key = key, Value = v };

    private static IProducer<string, T> Producer<T>() =>
        new ProducerBuilder<string, T>(KafkaConfig.Producer()).SetValueSerializer(new Serializer<T>()).Build();

    private static IConsumer<string, SagaMessage> Consumer(string group, params string[] topics)
    {
        var c = new ConsumerBuilder<string, SagaMessage>(KafkaConfig.Consumer(group))
            .SetValueDeserializer(new Serializer<SagaMessage>()).Build();
        c.Subscribe(topics);
        return c;
    }

    private static void Loop(IConsumer<string, SagaMessage> consumer,
        Action<ConsumeResult<string, SagaMessage>> handle, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                handle(cr);
                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }
}
