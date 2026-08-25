using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;
using System.Text;

namespace KafkaPatterns.Infrastructure.Messaging;

public interface IKafkaMessagePub
{

    Task<Result> PublishAsync<T>(string topic, T domainEvent, string? key = null, CancellationToken cancellationToken = default)
        where T : IDomainEvent;

    Task<Result> PublishBatchAsync<T>(string topic, IEnumerable<T> events, Func<T, string>? keySelector = null, CancellationToken cancellationToken = default)
        where T : IDomainEvent;
}

public class KafkaMessagePub : IKafkaMessagePub, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly KafkaSettings _settings;
    private readonly ILogger<KafkaMessagePub> _logger;

    public KafkaMessagePub(
        IOptions<KafkaSettings> settings,
        ILogger<KafkaMessagePub> logger)
    {
        _settings = settings.Value;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            ClientId = "kafka-patterns-producer",
            Acks = Acks.All,
            EnableIdempotence = true,
            MaxInFlight = _settings.MaxInFlight,
            MessageSendMaxRetries = _settings.MessageSendMaxRetries,
            RetryBackoffMs = _settings.RetryBackoffMs,
            MessageTimeoutMs = _settings.MessageTimeoutMs,
            LingerMs = 200,
            BatchSize = 10 * 1024,
            EnableDeliveryReports = true,
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task<Result> PublishAsync<T>(string topic, T domainEvent, string? key = null, CancellationToken cancellationToken = default)
        where T : IDomainEvent
    {
        var eventType = domainEvent.GetType().Name;

        try
        {
            var result = await _producer.ProduceAsync(topic, BuildMessage(domainEvent, key), cancellationToken);
            _logger.LogInformation("Published event {EventType} to Kafka topic {Topic} at offset {Offset}", eventType, topic, result.Offset);
            return Result.Success();
        }
        catch (ProduceException<string, string> ex)
        {

            _logger.LogError(ex, "Error publishing event {EventType} to Kafka", eventType);
            return Result.Transient($"publish of {eventType} failed: {ex.Error.Reason}");
        }
        catch (OperationCanceledException)
        {
            return Result.Transient($"publish of {eventType} cancelled during shutdown");
        }
    }

    public async Task<Result> PublishBatchAsync<T>(string topic, IEnumerable<T> events, Func<T, string>? keySelector = null, CancellationToken cancellationToken = default)
        where T : IDomainEvent
    {
        var failures = 0;

        foreach (var domainEvent in events)
        {
            _producer.Produce(topic, BuildMessage(domainEvent, keySelector?.Invoke(domainEvent)), report =>
            {
                if (report.Status != PersistenceStatus.Persisted)
                {
                    Interlocked.Increment(ref failures);
                    _logger.LogError("Failed kafka message producing with Key {Key}, Error: {Error}", report.Message.Key, report.Error.Code);
                }
            });
        }
        await Task.Run(() => _producer.Flush(cancellationToken), cancellationToken);

        var failed = Volatile.Read(ref failures);
        return failed == 0
            ? Result.Success()
            : Result.Transient($"{failed} message(s) in the batch were not persisted to {topic}");
    }

    private static Message<string, string> BuildMessage<T>(T domainEvent, string? key) where T : IDomainEvent
    {
        var occurredOn = new DateTimeOffset(domainEvent.OccurredOnUtc, TimeSpan.Zero);

        return new Message<string, string>
        {
            Key = key ?? domainEvent.EventId.ToString(),
            Value = MessageJson.Serialize(domainEvent),
            Headers = new Headers
            {
                { "event-type",  Encoding.UTF8.GetBytes(domainEvent.GetType().Name) },
                { "occurred-on", Encoding.UTF8.GetBytes(domainEvent.OccurredOnUtc.ToString("O", CultureInfo.InvariantCulture)) },
                { "timestamp",   Encoding.UTF8.GetBytes(occurredOn.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture)) }
            }
        };
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
        GC.SuppressFinalize(this);
    }
}
