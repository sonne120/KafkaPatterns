using Confluent.Kafka;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging;
using KafkaPatterns.Infrastructure.Messaging.Serialization;

namespace KafkaPatterns.Patterns.TimeWindowing;

public record PageView(string Page, DateTimeOffset At);
public record WindowedCount(string Page, DateTimeOffset WindowStart, DateTimeOffset WindowEnd, int Count);

public static class TimeWindowingDemo
{
    private const string In  = "site.pageviews";
    private const string Out = "site.pageviews.per-window";
    private static readonly TimeSpan WindowSize = TimeSpan.FromSeconds(5);

    public static async Task RunAsync(CancellationToken ct)
    {
        await TopicAdmin.EnsureTopicsAsync(1, In, Out);

        using var demoCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var aggregator = Task.Run(() => Aggregator(demoCts.Token), CancellationToken.None);
        var reporter   = Task.Run(() => Reporter(demoCts.Token), CancellationToken.None);

        using var producer = new ProducerBuilder<string, PageView>(KafkaConfig.Producer())
            .SetValueSerializer(new Serializer<PageView>()).Build();

        string[] pages = ["/home", "/pricing", "/docs"];
        var stopAt = DateTime.UtcNow.AddSeconds(14);
        while (DateTime.UtcNow < stopAt && !ct.IsCancellationRequested)
        {
            var pv = new PageView(pages[Random.Shared.Next(pages.Length)], DateTimeOffset.UtcNow);
            await producer.ProduceAsync(In, new Message<string, PageView> { Key = pv.Page, Value = pv }, ct);
            await Task.Delay(Random.Shared.Next(100, 400), ct);
        }

        await Task.Delay(7000, ct); 
        await DemoRunner.ShutdownAsync(demoCts, aggregator, reporter);
    }

    private static void Aggregator(CancellationToken ct)
    {
        var windows = new Dictionary<(string Page, long WindowStartMs), int>();

        using var consumer = new ConsumerBuilder<string, PageView>(KafkaConfig.Consumer("pageview-windower"))
            .SetValueDeserializer(new Serializer<PageView>()).Build();
        using var producer = new ProducerBuilder<string, WindowedCount>(KafkaConfig.Producer())
            .SetValueSerializer(new Serializer<WindowedCount>()).Build();

        consumer.Subscribe(In);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (cr is not null)
                {
                    var pv = cr.Message.Value;
                    var startMs = pv.At.ToUnixTimeMilliseconds()
                                  / (long)WindowSize.TotalMilliseconds
                                  * (long)WindowSize.TotalMilliseconds; // tumbling bucket
                    var key = (pv.Page, startMs);
                    windows[key] = windows.GetValueOrDefault(key) + 1;
                    consumer.Commit(cr);
                }

                var watermark = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                foreach (var (k, count) in windows.Where(w =>
                             w.Key.WindowStartMs + (long)WindowSize.TotalMilliseconds < watermark).ToArray())
                {
                    var start = DateTimeOffset.FromUnixTimeMilliseconds(k.WindowStartMs);
                    producer.Produce(Out, new Message<string, WindowedCount>
                    {
                        Key = k.Page,
                        Value = new WindowedCount(k.Page, start, start + WindowSize, count)
                    });
                    windows.Remove(k);
                }
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }

    private static void Reporter(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, WindowedCount>(KafkaConfig.Consumer("window-reporter"))
            .SetValueDeserializer(new Serializer<WindowedCount>()).Build();
        consumer.Subscribe(Out);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                var w = cr.Message.Value;
                TopicAdmin.Log("reporter", $"[{w.WindowStart:HH:mm:ss}–{w.WindowEnd:HH:mm:ss}] {w.Page}: {w.Count} views");
                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }
}
