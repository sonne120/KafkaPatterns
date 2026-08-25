using Confluent.Kafka;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging;

namespace KafkaPatterns.Patterns.StreamProcessing;

public record RawPayment(string PaymentId, string Currency, decimal Amount);
public record EnrichedPayment(string PaymentId, decimal AmountUsd, string RiskLevel);
public static class StreamProcessingDemo
{
    private const string In  = "payments.raw";
    private const string Out = "payments.enriched";

    private static readonly Dictionary<string, decimal> UsdRate = new()
        { ["USD"] = 1m, ["EUR"] = 1.09m, ["UAH"] = 0.024m };

    public static async Task RunAsync(CancellationToken ct)
    {
        await TopicAdmin.EnsureTopicsAsync(3, In, Out);
        await SeedInputAsync(ct);

        using var demoCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var processor = Task.Run(() => Process(demoCts.Token), CancellationToken.None);
        var sink      = Task.Run(() => Sink(demoCts.Token), CancellationToken.None);

        await Task.Delay(6000, ct);
        await DemoRunner.ShutdownAsync(demoCts, processor, sink);
    }

    private static void Process(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, RawPayment>(
                KafkaConfig.Consumer("payment-enricher", c => c.IsolationLevel = IsolationLevel.ReadCommitted))
            .SetValueDeserializer(new Serializer<RawPayment>())
            .Build();

        using var producer = new ProducerBuilder<string, EnrichedPayment>(
                KafkaConfig.Producer(p => p.TransactionalId = "payment-enricher-tx-1"))
            .SetValueSerializer(new Serializer<EnrichedPayment>())
            .Build();

        producer.InitTransactions(TimeSpan.FromSeconds(10));
        consumer.Subscribe(In);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                var outcome = ProcessOne(producer, consumer, cr);

                if (outcome.IsFailure)
                {
                    TopicAdmin.Log("enricher", $"{cr.Message.Key}: {outcome.Error}");
                    if (outcome.IsTransient)
                        Thread.Sleep(1000);
                }
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }

    private static Result ProcessOne(
        IProducer<string, EnrichedPayment> producer,
        IConsumer<string, RawPayment> consumer,
        ConsumeResult<string, RawPayment> cr)
    {
        var raw = cr.Message.Value;
        var offsets = new[] { new TopicPartitionOffset(cr.TopicPartition, cr.Offset + 1) };

        if (!UsdRate.TryGetValue(raw.Currency, out var rate))
        {
            SkipPoison(producer, consumer, offsets);
            return Result.Failure($"no USD rate for currency '{raw.Currency}' — skipped, not published");
        }

        var usd = raw.Amount * rate;
        var enriched = new EnrichedPayment(raw.PaymentId, usd, usd > 1000 ? "HIGH" : "LOW");

        producer.BeginTransaction();
        try
        {
            producer.Produce(Out, new Message<string, EnrichedPayment>
                { Key = cr.Message.Key, Value = enriched });

            // offsets committed atomically with the output record
            producer.SendOffsetsToTransaction(offsets, consumer.ConsumerGroupMetadata, TimeSpan.FromSeconds(10));

            producer.CommitTransaction();
            TopicAdmin.Log("enricher", $"{raw.PaymentId}: {raw.Amount} {raw.Currency} -> {usd:F2} USD [{enriched.RiskLevel}]");
            return Result.Success();
        }
        catch (KafkaException ex)
        {
            producer.AbortTransaction();
            return Result.Transient($"transaction aborted: {ex.Error.Reason}");
        }
    }

    private static void SkipPoison(
        IProducer<string, EnrichedPayment> producer,
        IConsumer<string, RawPayment> consumer,
        TopicPartitionOffset[] offsets)
    {
        producer.BeginTransaction();
        try
        {
            producer.SendOffsetsToTransaction(offsets, consumer.ConsumerGroupMetadata, TimeSpan.FromSeconds(10));
            producer.CommitTransaction();
        }
        catch (KafkaException)
        {
            producer.AbortTransaction();
        }
    }

    private static void Sink(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, EnrichedPayment>(
                KafkaConfig.Consumer("enriched-sink", c => c.IsolationLevel = IsolationLevel.ReadCommitted))
            .SetValueDeserializer(new Serializer<EnrichedPayment>())
            .Build();
        consumer.Subscribe(Out);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                TopicAdmin.Log("sink", $"downstream received {cr.Message.Value.PaymentId} ({cr.Message.Value.RiskLevel})");
                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }

    private static async Task SeedInputAsync(CancellationToken ct)
    {
        using var producer = new ProducerBuilder<string, RawPayment>(KafkaConfig.Producer())
            .SetValueSerializer(new Serializer<RawPayment>()).Build();

        RawPayment[] payments =
        [
            new("PAY-1", "USD", 250),  new("PAY-2", "EUR", 1200),
            new("PAY-3", "UAH", 45000), new("PAY-4", "USD", 3100),
        ];
        foreach (var p in payments)
            await producer.ProduceAsync(In, new Message<string, RawPayment> { Key = p.PaymentId, Value = p }, ct);
    }
}
