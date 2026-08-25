using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaPatterns.Infrastructure.Messaging;

/// <summary>
/// Parks messages a consumer could not handle.
///
/// Every operation reports the same three-way outcome the consumers use, because the caller's
/// next move depends on which one it is:
///
///   Result.Success   -> the record is durably on the topic; the caller may commit past the original.
///   Result.Transient -> the broker refused it for now; the caller MUST NOT commit — rewind and retry,
///                       or the message is lost: neither processed nor parked.
///   Result.Failure   -> this record can never be delivered (too large, topic forbidden); parking it
///                       is hopeless, so the caller should commit past it and shout, rather than
///                       wedging the partition forever on something that will never move.
/// </summary>
public sealed class DeadLetterProducer : IDisposable
{
    public const string AttemptsHeader = "attempts";
    public const string ReasonHeader = "x-failure-reason";

    private readonly ILogger<DeadLetterProducer> _logger;
    private readonly IProducer<string, string> _producer;
    private readonly string _bootstrap;
    private readonly int _partitions;

    public string DeadLetterTopic { get; }

    public DeadLetterProducer(IOptions<KafkaSettings> options, ILogger<DeadLetterProducer> logger)
    {
        _logger = logger;
        // Typically these would be extracted from the settings class directly
        _bootstrap = options.Value.BootstrapServers;
        DeadLetterTopic = "deadletter";
        _partitions = 1;

        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = _bootstrap,
            // The dead-letter topic is the last place a message can land, so it needs to be at
            // least as durable as the path that failed. Left on the defaults, acks=1 meant a
            // leader failover could lose the very record we already know we cannot reprocess.
            Acks = Acks.All,
            EnableIdempotence = true,
            EnableDeliveryReports = true,
            MessageTimeoutMs = 10000
        }).Build();
    }

    /// <summary>
    /// Success   -> the dead-letter topic exists.
    /// Transient -> the broker was unreachable; parking will fail until it comes back.
    /// Failure   -> the topic cannot be created (authorization, invalid config).
    /// </summary>
    public async Task<Result> EnsureTopicsAsync()
    {
        try
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _bootstrap }).Build();

            await admin.CreateTopicsAsync(
            [
                new TopicSpecification { Name = DeadLetterTopic, ReplicationFactor = 1, NumPartitions = _partitions }
            ]);

            _logger.LogInformation("Created topic {Topic}", DeadLetterTopic);
            return Result.Success();
        }
        catch (CreateTopicsException e) when (
            e.Results.All(r => r.Error.Code is ErrorCode.TopicAlreadyExists or ErrorCode.NoError))
        {
            return Result.Success();   // already there — fine
        }
        catch (CreateTopicsException e)
        {
            var reason = e.Results[0].Error.Reason;
            _logger.LogWarning("Could not create topic {Topic}: {Reason}", DeadLetterTopic, reason);

            return IsTerminal(e.Results[0].Error.Code)
                ? Result.Failure($"cannot create {DeadLetterTopic}: {reason}")
                : Result.Transient($"could not create {DeadLetterTopic} right now: {reason}");
        }
        catch (KafkaException e)
        {
            _logger.LogWarning(e, "Broker unreachable while ensuring {Topic}", DeadLetterTopic);
            return Result.Transient($"broker unreachable while ensuring {DeadLetterTopic}: {e.Error.Reason}");
        }
    }

    /// <summary>See the type-level summary for what each outcome obliges the caller to do.</summary>
    public async Task<Result> ProduceAsync(string topic, string? key, string value, int attempts, string? reason = null)
    {
        var headers = new Headers
        {
            { AttemptsHeader, Encoding.UTF8.GetBytes(attempts.ToString(CultureInfo.InvariantCulture)) }
        };

        if (reason is not null)
            headers.Add(ReasonHeader, Encoding.UTF8.GetBytes(reason));

        try
        {
            var delivery = await _producer.ProduceAsync(topic, new Message<string, string>
            {
                // Pass a null key through rather than inventing a GUID: a fabricated key moves the
                // record to a different partition, which is exactly what you don't want on a retry.
                Key = key!,
                Value = value,
                Headers = headers
            });

            return delivery.Status == PersistenceStatus.Persisted
                ? Result.Success()
                : Result.Transient($"record to {topic} was not persisted (status {delivery.Status})");
        }
        catch (ProduceException<string, string> ex)
        {
            return IsTerminal(ex.Error.Code)
                ? Result.Failure($"record can never be delivered to {topic}: {ex.Error.Reason}")
                : Result.Transient($"could not reach {topic} right now: {ex.Error.Reason}");
        }
    }

    /// <summary>Errors that retrying cannot fix — the record or the topic is the problem, not the moment.</summary>
    private static bool IsTerminal(ErrorCode code) => code is
        ErrorCode.MsgSizeTooLarge or
        ErrorCode.InvalidMsgSize or
        ErrorCode.RecordListTooLarge or
        ErrorCode.TopicAuthorizationFailed or
        ErrorCode.ClusterAuthorizationFailed or
        ErrorCode.InvalidConfig or
        ErrorCode.Local_MsgTimedOut;

    /// <summary>Reads the current attempt count from the message headers (0 if absent).</summary>
    public static int GetAttempts(ConsumeResult<string, string> result)
    {
        var bytes = result.Message.Headers.TryGetBytes(AttemptsHeader);

        return bytes.IsSuccess && int.TryParse(Encoding.UTF8.GetString(bytes.Value), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n)
            ? n
            : 0;
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
