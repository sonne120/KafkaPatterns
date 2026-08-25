using Confluent.Kafka;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KafkaPatterns.Patterns.DeadLetterQueue;

public record WebhookDelivery(string Id, string Url, string PayloadJson);

public static class DeadLetterQueueDemo
{
    private const string Topic = "webhooks.outgoing";
    private const int MaxAttempts = 3;

    public static async Task RunAsync(CancellationToken ct)
    {
        var kafkaSettings = Options.Create(new KafkaSettings { BootstrapServers = KafkaConfig.BootstrapServers });
        using var deadLetterProducer = new DeadLetterProducer(kafkaSettings, NullLogger<DeadLetterProducer>.Instance);
        await deadLetterProducer.EnsureTopicsAsync();

        await TopicAdmin.EnsureTopicsAsync(1, Topic);

        using (var producer = new ProducerBuilder<string, WebhookDelivery>(KafkaConfig.Producer())
                   .SetValueSerializer(new Serializer<WebhookDelivery>()).Build())
        {
            WebhookDelivery[] deliveries =
            [
                new("wh-1", "https://ok.example.com/hook",      """{"event":"order.created"}"""),
                new("wh-2", "https://always-500.example.com/x", """{"event":"order.paid"}"""),   // poison
                new("wh-3", "https://ok.example.com/hook",      """{"event":"order.shipped"}"""),
            ];
            foreach (var d in deliveries)
                await producer.ProduceAsync(Topic,
                    new Message<string, WebhookDelivery> { Key = d.Id, Value = d }, ct);
        }

        using var demoCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var worker = Task.Run(() => Worker(deadLetterProducer, demoCts.Token), CancellationToken.None);
        var dlqMonitor = Task.Run(() => DlqMonitor(demoCts.Token), CancellationToken.None);

        await Task.Delay(8000, ct);

        await DemoRunner.ShutdownAsync(demoCts, worker, dlqMonitor);
    }

    private static async Task Worker(DeadLetterProducer dlqProducer, CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, WebhookDelivery>(KafkaConfig.Consumer("webhook-sender"))
            .SetValueDeserializer(new Serializer<WebhookDelivery>()).Build();

        consumer.Subscribe(Topic);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                var delivery = cr.Message.Value;

                var delivered = await DeliverWithRetriesAsync(delivery, ct);

                if (delivered.IsFailure)
                {
                    var parked = await ParkAsync(dlqProducer, cr, delivered.Error);

                    TopicAdmin.Log("webhook-sender", parked.IsSuccess
                        ? $"{delivery.Id} PARKED to DLQ"
                        : $"{delivery.Id} could NOT be parked ({parked.Error}) — it will be redelivered");
                }

                consumer.Commit(cr); // commit either way — partition must not stay blocked
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }

    private static async Task<Result> DeliverWithRetriesAsync(WebhookDelivery d, CancellationToken ct)
    {
        var outcome = Result.Failure("not attempted");

        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            outcome = SendWebhook(d);

            if (outcome.IsSuccess)
            {
                TopicAdmin.Log("webhook-sender",
                    $"{d.Id} delivered{(attempt > 1 ? $" on attempt {attempt}" : "")}");
                return outcome;
            }

            if (!outcome.IsTransient)
            {
                TopicAdmin.Log("webhook-sender", $"{d.Id} failed permanently: {outcome.Error}");
                break;
            }

            if (attempt < MaxAttempts)
            {
                TopicAdmin.Log("webhook-sender",
                    $"{d.Id} attempt {attempt}/{MaxAttempts} failed: {outcome.Error}, retrying...");
                await Task.Delay(200 * attempt, ct); 
            }
        }

        return outcome;
    }

    private static Task<Result> ParkAsync(
        DeadLetterProducer dlqProducer, ConsumeResult<string, WebhookDelivery> cr, string reason)
        => dlqProducer.ProduceAsync(
            dlqProducer.DeadLetterTopic, cr.Message.Key, MessageJson.Serialize(cr.Message.Value), MaxAttempts, reason);

    private static Result SendWebhook(WebhookDelivery d)
    {
        Thread.Sleep(100);

        return d.Url.Contains("always-500")
            ? Result.Transient("endpoint returned 500 Internal Server Error")
            : Result.Success();
    }

    private static void DlqMonitor(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, string>(KafkaConfig.Consumer("dlq-monitor")).Build();
        consumer.Subscribe("deadletter");

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                var reason = cr.Message.Headers.TryGetString(DeadLetterProducer.ReasonHeader);
                var attempts = DeadLetterProducer.GetAttempts(cr);

                TopicAdmin.Log("dlq-monitor",
                    $"DLQ record key={cr.Message.Key}, attempts={attempts}, " +
                    $"error=\"{(reason.IsSuccess ? reason.Value : "")}\" — alert on-call / schedule redrive");
                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }
}
