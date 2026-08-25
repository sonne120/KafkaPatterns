using Confluent.Kafka;
using KafkaPatterns.Infrastructure;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Stream;

namespace KafkaPatterns.Patterns.KafkaStreams;

public static class KafkaStreamsDemo
{
    private const string In  = "streamiz.pageviews";
    private const string Out = "streamiz.pageviews.counts";

    public static async Task RunAsync(CancellationToken ct)
    {
        await TopicAdmin.EnsureTopicsAsync(1, In, Out);

        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId    = "streamiz-windowed-counts", 
            BootstrapServers = KafkaConfig.BootstrapServers,
            AutoOffsetReset  = AutoOffsetReset.Earliest,
            CommitIntervalMs = 1000
        };

        var builder = new StreamBuilder();

        builder.Stream<string, string>(In)                                 
            .GroupByKey()
            .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(5)))  // same 5s tumbling window as pattern 12
            .Count(InMemoryWindows.As<string, long, StringSerDes, Int64SerDes>("pageview-window-store"))
            .ToStream()
            .Map((windowedKey, count) => KeyValuePair.Create(
                windowedKey.Key,
                $"[{FromMs(windowedKey.Window.StartMs):HH:mm:ss}-{FromMs(windowedKey.Window.EndMs):HH:mm:ss}] " +
                $"{windowedKey.Key}: {count} views"))
            .To(Out);

        var streams = new KafkaStream(builder.Build(), config);

        using var demoCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var reporter = Task.Run(() => Reporter(demoCts.Token), CancellationToken.None);
        var seeder   = Task.Run(() => SeedAsync(demoCts.Token), CancellationToken.None);

        TopicAdmin.Log("streamiz", "starting topology (first start also creates the changelog topic)...");


        await streams.StartAsync(CancellationToken.None);

        try
        {
            await Task.Delay(TimeSpan.FromSeconds(20), ct);
        }
        catch (OperationCanceledException)
        {
            TopicAdmin.Log("streamiz", "demo cancelled — stopping the topology");
        }
        finally
        {
            streams.Dispose(); // graceful: flush stores, commit, leave group
            await DemoRunner.ShutdownAsync(demoCts, reporter, seeder);
        }
    }

    private static async Task SeedAsync(CancellationToken ct)
    {
        using var producer = new ProducerBuilder<string, string>(KafkaConfig.Producer()).Build();
        string[] pages = ["/home", "/pricing", "/docs"];

        var stopAt = DateTime.UtcNow.AddSeconds(14);
        while (DateTime.UtcNow < stopAt && !ct.IsCancellationRequested)
        {
            var page = pages[Random.Shared.Next(pages.Length)];
            await producer.ProduceAsync(In, new Message<string, string> { Key = page, Value = "1" }, ct);
            await Task.Delay(Random.Shared.Next(100, 400), ct);
        }
        TopicAdmin.Log("seeder", "done producing page views");
    }

    private static void Reporter(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, string>(KafkaConfig.Consumer("streamiz-reporter")).Build();
        consumer.Subscribe(Out);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                TopicAdmin.Log("reporter", cr.Message.Value); 
                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }

    private static DateTimeOffset FromMs(long ms) => DateTimeOffset.FromUnixTimeMilliseconds(ms).ToLocalTime();
}
