using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using KafkaPatterns.Infrastructure.Messaging;
using KafkaPatterns.Patterns.Cdc.Variants;
using KafkaPatterns;
using KafkaPatterns.Patterns.Cdc;
using KafkaPatterns.Patterns.CompetingConsumers;
using KafkaPatterns.Patterns.DeadLetterQueue;
using KafkaPatterns.Patterns.EventSourcing;
using KafkaPatterns.Patterns.KafkaStreams;
using KafkaPatterns.Patterns.Ksql;
using KafkaPatterns.Patterns.ParentChild;
using KafkaPatterns.Patterns.Partitioning;
using KafkaPatterns.Patterns.PubSub;
using KafkaPatterns.Patterns.RequestReply;
using KafkaPatterns.Patterns.Saga;
using KafkaPatterns.Patterns.StreamProcessing;
using KafkaPatterns.Patterns.TimeWindowing;
using KafkaPatterns.Patterns.WorkQueue;

using KafkaPatterns.Infrastructure.Persistence;

const string CdcTopic = CdcTopics.CustomerEvents;

var builder = Host.CreateDefaultBuilder(args);


builder.ConfigureServices((hostContext, services) =>
{
    services.AddKafkaInfrastructure(hostContext.Configuration);

    void AddDemo(string name, Func<CancellationToken, Task> run) =>
        services.AddHostedService(sp => new LegacyDemoAdapter(
            name, run, sp.GetRequiredService<IHostApplicationLifetime>()));
    void EnsureTopics(int partitions, params string[] topics) =>
        services.AddHostedService(sp => new TopicProvisioner(
            sp.GetRequiredService<IOptions<KafkaSettings>>(),
            sp.GetRequiredService<ILogger<TopicProvisioner>>(),
            partitions, topics));

    var pattern = args.FirstOrDefault()?.ToLowerInvariant();

    switch (pattern)
    {
        case "cdc-state":
            EnsureTopics(3, CdcTopic);

            services.AddHostedService<StateMachineOutboxRelay>();
            services.AddHostedService<SearchIndexerCdcConsumer>();
            services.AddHostedService<CustomerWriteSimulator>();
            break;

        case "cdc-batch":
            EnsureTopics(3, CdcTopic);
            services.AddHostedService<BatchOutboxRelay>();
            services.AddHostedService<SearchIndexerCdcConsumer>();
            services.AddHostedService<CustomerWriteSimulator>();
            break;

        case "cdc-poll":
            EnsureTopics(3, CdcTopic);

            services.AddHostedService<PollingOutboxRelay>();
            services.AddHostedService<SearchIndexerCdcConsumer>();
            services.AddHostedService<CustomerWriteSimulator>();
            break;

        case "cdc-rx":
            EnsureTopics(3, CdcTopic);

            // 1. Strategy logic
            services.AddSingleton<RxMessageProcessor>();

            // 2. The outbox processor
            services.AddHostedService<StateMachineOutboxRelay>();

            // 3. Add the actual KafkaConsumerRx directly injected
            services.AddHostedService(sp =>
            {
                var processor = sp.GetRequiredService<RxMessageProcessor>();
                return new KafkaConsumerRx(
                    sp.GetRequiredService<Microsoft.Extensions.Configuration.IConfiguration>(),
                    sp.GetRequiredService<ILogger<KafkaConsumerRx>>(),
                    sp.GetRequiredService<DeadLetterProducer>(),
                    processor.ProcessMessageAsync
                );
            });

            // 4. Keep Demo host to simulate pushing commands to it
            services.AddHostedService<CustomerWriteSimulator>();
            break;

        case "pubsub":       AddDemo("Publish/Subscribe",    PubSubDemo.RunAsync); break;
        case "workqueue":    AddDemo("Work Queue",           WorkQueueDemo.RunAsync); break;
        case "eventsource":  AddDemo("Event Sourcing",       EventSourcingDemo.RunAsync); break;
        case "stream":       AddDemo("Stream Processing",    StreamProcessingDemo.RunAsync); break;
        case "cdc":          AddDemo("Change Data Capture",  CdcOutboxDemo.RunAsync); break;
        case "dlq":          AddDemo("Dead Letter Queue",    DeadLetterQueueDemo.RunAsync); break;
        case "parentchild":  AddDemo("Parent-Child Topics",  ParentChildDemo.RunAsync); break;
        case "reqreply":     AddDemo("Request/Reply",        RequestReplyDemo.RunAsync); break;
        case "competing":    AddDemo("Competing Consumers",  CompetingConsumersDemo.RunAsync); break;
        case "partitioning": AddDemo("Partitioning Strategy", PartitioningDemo.RunAsync); break;
        case "saga":         AddDemo("Saga",                 SagaDemo.RunAsync); break;
        case "windowing":    AddDemo("Time Windowing",       TimeWindowingDemo.RunAsync); break;
        case "streams":      AddDemo("Kafka Streams (Streamiz)", KafkaStreamsDemo.RunAsync); break;
        case "ksql":         AddDemo("ksqlDB — SQL over topics",  KsqlDemo.RunAsync); break;

        default:
            Console.WriteLine("Usage: dotnet run -- <pattern>\n");
            foreach (var key in DemoCatalog.Keys)
                Console.WriteLine($"  {key}");
            Environment.Exit(1);
            break;
    }
});

var host = builder.Build();

try
{
    await host.RunAsync();
}
catch (OperationCanceledException) { }
Console.WriteLine("\nDone.");

static class DemoCatalog
{
    public static readonly string[] Keys =
    [
        "pubsub", "workqueue", "eventsource", "stream", "cdc", "dlq", "parentchild",
        "reqreply", "competing", "partitioning", "saga", "windowing",
        "streams", "ksql",
        "cdc-state", "cdc-batch", "cdc-poll", "cdc-rx"
    ];
}

sealed class LegacyDemoAdapter : BackgroundService
{
    private readonly string _name;
    private readonly Func<CancellationToken, Task> _demoFunc;
    private readonly IHostApplicationLifetime _lifetime;

    public LegacyDemoAdapter(string name, Func<CancellationToken, Task> func, IHostApplicationLifetime lifetime)
    {
        _name = name;
        _demoFunc = func;
        _lifetime = lifetime;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Console.WriteLine($"----{_name}---\n");
        try
        {
            await _demoFunc(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            _lifetime.StopApplication();
        }
    }
}
