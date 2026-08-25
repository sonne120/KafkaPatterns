using System.Collections.Concurrent;
using System.Text;
using Confluent.Kafka;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging;

namespace KafkaPatterns.Patterns.RequestReply;

public record PriceRequest(string Sku, int Quantity);
public record PriceResponse(string Sku, decimal UnitPrice, decimal Total);

public static class RequestReplyDemo
{
    private const string RequestTopic = "pricing.requests";
    private const string ReplyTopic   = "pricing.replies.client-1"; 
    private static readonly TimeSpan ReplyTimeout = TimeSpan.FromSeconds(5);

    public static async Task RunAsync(CancellationToken ct)
    {
        await TopicAdmin.EnsureTopicsAsync(1, RequestTopic, ReplyTopic);

        using var demoCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var server = Task.Run(() => PricingServer(demoCts.Token), CancellationToken.None);
        var pending = new ConcurrentDictionary<string, TaskCompletionSource<PriceResponse>>();
        var replyListener = Task.Run(() => ReplyListener(pending, demoCts.Token), CancellationToken.None);

        using var producer = new ProducerBuilder<string, PriceRequest>(KafkaConfig.Producer())
            .SetValueSerializer(new Serializer<PriceRequest>()).Build();

        async Task<Result<PriceResponse>> CallAsync(PriceRequest req)
        {
            var correlationId = Guid.NewGuid().ToString("N");
            var tcs = new TaskCompletionSource<PriceResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
            pending[correlationId] = tcs;

            try
            {
                await producer.ProduceAsync(RequestTopic, new Message<string, PriceRequest>
                {
                    Key = req.Sku,
                    Value = req,
                    Headers = new Headers
                    {
                        { "correlation-id", Encoding.UTF8.GetBytes(correlationId) },
                        { "reply-to",       Encoding.UTF8.GetBytes(ReplyTopic) }
                    }
                }, ct);

                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                timeout.CancelAfter(ReplyTimeout);
                await using var registration = timeout.Token.Register(() => tcs.TrySetCanceled(timeout.Token));

                return Result.Success(await tcs.Task);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                return Result.Transient<PriceResponse>($"no reply for {req.Sku} within {ReplyTimeout.TotalSeconds:0}s");
            }
            catch (ProduceException<string, PriceRequest> ex)
            {
                return Result.Transient<PriceResponse>($"could not send request for {req.Sku}: {ex.Error.Reason}");
            }
            finally
            {
                pending.TryRemove(correlationId, out _);
            }
        }

        await ReportAsync(CallAsync, new PriceRequest("SKU-KEYBOARD", 3));
        await ReportAsync(CallAsync, new PriceRequest("SKU-MONITOR", 2));


        await DemoRunner.ShutdownAsync(demoCts, server, replyListener);
    }

    private static async Task ReportAsync(Func<PriceRequest, Task<Result<PriceResponse>>> call, PriceRequest req)
    {
        var result = await call(req);

        TopicAdmin.Log("client", result.IsSuccess
            ? $"got reply: {result.Value.Sku} x{req.Quantity} = {result.Value.Total:F2}"
            : $"{req.Sku} unanswered ({result.Error}) — {(result.IsTransient ? "retryable" : "giving up")}");
    }

    private static void PricingServer(CancellationToken ct)
    {
        var priceList = new Dictionary<string, decimal>
            { ["SKU-KEYBOARD"] = 79.99m, ["SKU-MONITOR"] = 349.00m };

        using var consumer = new ConsumerBuilder<string, PriceRequest>(KafkaConfig.Consumer("pricing-service"))
            .SetValueDeserializer(new Serializer<PriceRequest>()).Build();
        using var producer = new ProducerBuilder<string, PriceResponse>(KafkaConfig.Producer())
            .SetValueSerializer(new Serializer<PriceResponse>()).Build();

        consumer.Subscribe(RequestTopic);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                var req = cr.Message.Value;

                var replyTo = cr.Message.Headers.TryGetString("reply-to");
                var corr    = cr.Message.Headers.TryGetBytes("correlation-id");

                if (replyTo.IsFailure || corr.IsFailure)
                {
                    TopicAdmin.Log("pricing-service",
                        $"dropping unaddressable request at offset {cr.Offset.Value}: " +
                        (replyTo.IsFailure ? replyTo.Error : corr.Error));
                    consumer.Commit(cr);
                    continue;
                }

                if (!priceList.TryGetValue(req.Sku, out var unit))
                {
                    TopicAdmin.Log("pricing-service", $"no price for {req.Sku} — not replying");
                    consumer.Commit(cr);
                    continue;
                }

                producer.Produce(replyTo.Value, new Message<string, PriceResponse>
                {
                    Key = req.Sku,
                    Value = new PriceResponse(req.Sku, unit, unit * req.Quantity),
                    Headers = new Headers { { "correlation-id", corr.Value } }
                });
                TopicAdmin.Log("pricing-service", $"priced {req.Sku} x{req.Quantity}");
                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }

    private static void ReplyListener(
        ConcurrentDictionary<string, TaskCompletionSource<PriceResponse>> pending, CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, PriceResponse>(KafkaConfig.Consumer("client-1-replies"))
            .SetValueDeserializer(new Serializer<PriceResponse>()).Build();
        consumer.Subscribe(ReplyTopic);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);

                var corr = cr.Message.Headers.TryGetString("correlation-id");
                if (corr.IsFailure)
                {
                    TopicAdmin.Log("client-replies", $"reply without a correlation-id ({corr.Error}) — cannot match, dropping");
                }
                else if (pending.TryRemove(corr.Value, out var tcs))
                {
                    tcs.TrySetResult(cr.Message.Value);
                }
                else
                {
                    TopicAdmin.Log("client-replies", $"reply with unknown correlation-id {corr.Value} — cannot match, dropping");
                }

                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }
}
