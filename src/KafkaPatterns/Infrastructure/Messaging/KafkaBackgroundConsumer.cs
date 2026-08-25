using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;
using Confluent.Kafka;

namespace KafkaPatterns.Infrastructure.Messaging;

public abstract class KafkaBackgroundConsumer<T> : BackgroundService where T : IDomainEvent
{
    private readonly KafkaSettings _settings;
    private readonly ILogger _logger;
    private readonly IConsumer<string, string> _consumer;

    protected string Topic { get; }

    protected KafkaBackgroundConsumer(
        IOptions<KafkaSettings> settings,
        ILogger logger,
        string topic)
    {
        _settings = settings.Value;
        _logger = logger;
        Topic = topic;

        var config = new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _settings.ConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false, 
            SessionTimeoutMs = 10_000,
            MaxPollIntervalMs = 300_000
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Kafka Consumer for {Topic} starting in group {GroupId}", Topic, _settings.ConsumerGroupId);

        try
        {
            _consumer.Subscribe(Topic);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(stoppingToken);
                    if (consumeResult is null) continue;

                    _logger.LogDebug("Received message key: {Key} on partition {Partition} at offset {Offset}",
                        consumeResult.Message.Key, consumeResult.Partition, consumeResult.Offset);

                    var outcome = await HandleAsync(consumeResult, stoppingToken);

                    if (outcome.IsSuccess)
                    {
                        _consumer.Commit(consumeResult);
                        _logger.LogDebug("Successfully processed and committed offset {Offset}", consumeResult.Offset);
                    }
                    else if (outcome.IsTransient)
                    {
                        _logger.LogWarning("Transient failure on offset {Offset} ({Error}); rewinding",
                            consumeResult.Offset, outcome.Error);
                        _consumer.Seek(consumeResult.TopicPartitionOffset);
                        await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                    }
                    else
                    {
                        _logger.LogError("Dropping poison message at offset {Offset}: {Error}",
                            consumeResult.Offset, outcome.Error);
                        _consumer.Commit(consumeResult);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException e)
                {
                    _logger.LogError(e, "Error consuming Kafka message on {Topic}", Topic);
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unexpected error in Kafka consumer for {Topic}", Topic);
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Kafka Consumer for {Topic} is stopping due to cancellation", Topic);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Kafka Consumer for {Topic} encountered an unexpected error", Topic);
        }
        finally
        {
            _consumer.Close();
            _consumer.Dispose();
            _logger.LogInformation("Kafka Consumer for {Topic} closed", Topic);
        }
    }

    private async Task<Result> HandleAsync(ConsumeResult<string, string> consumeResult, CancellationToken cancellationToken)
    {
        T? messageObject;
        try
        {
            messageObject = MessageJson.Deserialize<T>(consumeResult.Message.Value);
        }
        catch (JsonException ex)
        {
            return Result.Failure($"malformed {typeof(T).Name} payload: {ex.Message}");
        }

        return messageObject is null
            ? Result.Failure($"payload deserialized to null as {typeof(T).Name}")
            : await ProcessMessageAsync(messageObject, cancellationToken);
    }

    /// Implemented by derived classes to handle the domain event.
    /// Result.Success   -> commit the offset.
    /// Result.Transient -> dependency is down; rewind and retry this same message.
    /// Result.Failure   -> poison; commit past it so the partition keeps moving.
    protected abstract Task<Result> ProcessMessageAsync(T message, CancellationToken cancellationToken);
}
