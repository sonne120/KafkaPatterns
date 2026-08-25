using System.Reactive.Linq;
using Confluent.Kafka;
using KafkaPatterns.Infrastructure.Persistence;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaPatterns.Infrastructure.Messaging;

public sealed class KafkaConsumerRx : BackgroundService
{
    private const int MaxConsumeBatchSize = 100;

    private readonly string _topic;
    private readonly int _maxAttempts;
    private readonly DeadLetterProducer _deadLetter;
    private readonly ILogger<KafkaConsumerRx> _logger;
    private readonly IConsumer<string, string> _consumer;

    /// <summary>
    /// Strategy hook. Its Result decides what happens to the offset:
    ///   Result.Success   -> commit the offset.
    ///   Result.Transient -> dependency is down; rewind and retry this same message.
    ///   Result.Failure   -> poison; dead-letter it and commit past it so the partition keeps moving.
    /// </summary>
    private readonly Func<string, CancellationToken, Task<Result>> _messageProcessor;
    private IDisposable? _subscription;

    public KafkaConsumerRx(
        IConfiguration configuration,
        ILogger<KafkaConsumerRx> logger,
        DeadLetterProducer deadLetter,
        Func<string, CancellationToken, Task<Result>> messageProcessor)
    {
        _topic = configuration["Topic"] ?? CdcTopics.CustomerEvents;
        _maxAttempts = int.TryParse(configuration["MaxAttempts"], out var m) ? m : 3;
        _deadLetter = deadLetter;
        _logger = logger;
        _messageProcessor = messageProcessor;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = configuration["KafkaServer"] ?? KafkaConfig.BootstrapServers,
            GroupId = configuration["ConsumerGroup"] ?? "ConsumerGroup",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var ensured = await _deadLetter.EnsureTopicsAsync();
        if (ensured.IsFailure)
        {
            // Keep going: the retry path still works, but poison messages will have nowhere to go.
            _logger.LogWarning("Dead-letter topic unavailable ({Error}); poison messages cannot be parked", ensured.Error);
        }

        _subscription = StartKafkaObservable(stoppingToken).Subscribe(
            onNext: _ => { },
            onError: ex => _logger.LogError(ex, "Kafka stream error"),
            onCompleted: () => _logger.LogInformation("Kafka stream completed"));

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // normal shutdown
        }
    }

    private IObservable<ConsumeResult<string, string>> StartKafkaObservable(CancellationToken stoppingToken)
    {
        return Observable.Create<ConsumeResult<string, string>>(observer =>
        {
            _consumer.Subscribe(_topic);

            var pump = Task.Run(async () =>
            {
                try
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        try
                        {
                            var batch = _consumer.ConsumeBatch(TimeSpan.FromSeconds(5), MaxConsumeBatchSize, stoppingToken);
                            if (batch.Count == 0)
                                continue;

                            if (!await ProcessBatchAsync(batch, observer, stoppingToken))
                                break;
                        }
                        catch (ConsumeException e) when (!e.Error.IsFatal)
                        {
                            _logger.LogError(e, "Non-fatal consume error");
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                        catch (Exception e)
                        {
                            _logger.LogError(e, "Error consuming message");
                            observer.OnError(e);
                            return;
                        }
                    }

                    observer.OnCompleted();
                }
                finally
                {
                    // Close() exactly once, here — the observable's dispose action used to close
                    // it a second time, and Dispose() disposed the DI-owned DeadLetterProducer.
                    _consumer.Close();
                }
            }, CancellationToken.None);

            // The pump owns the consumer and unwinds on stoppingToken; nothing to tear down here.
            _ = pump;
            return () => { };
        });
    }

    /// <summary>Returns false when the pump should stop.</summary>
    private async Task<bool> ProcessBatchAsync(
        IReadOnlyList<ConsumeResult<string, string>> batch,
        IObserver<ConsumeResult<string, string>> observer,
        CancellationToken stoppingToken)
    {
        for (var i = 0; i < batch.Count; i++)
        {
            var result = batch[i];
            var outcome = await _messageProcessor(result.Message.Value, stoppingToken);

            if (outcome.IsSuccess)
            {
                if (!TryCommit(result)) return true;   // partition reassigned mid-batch; stop here
                observer.OnNext(result);
            }
            else if (outcome.IsTransient)
            {
                _logger.LogWarning("Transient failure ({Error}); rewinding from offset {Offset}",
                    outcome.Error, result.TopicPartitionOffset);

                // Rewind THIS record and every record after it in the batch. Seeking only the
                // current partition and breaking used to silently drop the batch's remaining
                // records on other partitions: already consumed, never processed, never committed.
                RewindFrom(batch, i);

                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                return true;
            }
            else
            {
                // Poison: count the attempt, then re-queue or dead-letter it.
                var routed = await RouteFailureAsync(result, outcome.Error);

                if (routed.IsTransient)
                {
                    // Parking failed for now. Committing here would lose the message outright —
                    // neither processed nor parked — so rewind and come back to it.
                    _logger.LogWarning("Could not park offset {Offset} ({Error}); rewinding",
                        result.TopicPartitionOffset, routed.Error);
                    RewindFrom(batch, i);
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                    return true;
                }

                if (routed.IsFailure)
                {
                    // Unparkable for good. Commit anyway rather than wedging the partition
                    // forever on a record that will never move — but say so loudly.
                    _logger.LogError("Dropping unparkable message at offset {Offset}: {Error}",
                        result.TopicPartitionOffset, routed.Error);
                }

                if (!TryCommit(result)) return true;
            }
        }

        return true;
    }

    /// <summary>
    /// Commits, tolerating the rebalance window. Between losing a partition and re-joining the
    /// group a commit fails with UnknownMemberId / IllegalGeneration / RebalanceInProgress. None
    /// of those are worth killing the consumer over — the new generation simply redelivers from
    /// the last committed offset — but they used to reach the pump's catch-all, which called
    /// observer.OnError and stopped consuming for good.
    /// </summary>
    private bool TryCommit(ConsumeResult<string, string> result)
    {
        try
        {
            _consumer.Commit(result);
            return true;
        }
        catch (KafkaException ex) when (IsRebalanceInFlight(ex.Error.Code))
        {
            _logger.LogWarning("Commit of {Offset} skipped ({Code}); partition reassigned, it will be redelivered",
                result.TopicPartitionOffset, ex.Error.Code);
            return false;
        }
    }

    private static bool IsRebalanceInFlight(ErrorCode code) => code is
        ErrorCode.UnknownMemberId or
        ErrorCode.IllegalGeneration or
        ErrorCode.RebalanceInProgress;

    /// <summary>Seeks every partition represented in batch[startIndex..] back to its earliest unprocessed offset.</summary>
    private void RewindFrom(IReadOnlyList<ConsumeResult<string, string>> batch, int startIndex)
    {
        var earliest = new Dictionary<TopicPartition, Offset>();

        for (var i = startIndex; i < batch.Count; i++)
        {
            var tp = batch[i].TopicPartition;
            if (!earliest.TryGetValue(tp, out var seen) || batch[i].Offset < seen)
                earliest[tp] = batch[i].Offset;
        }

        foreach (var (topicPartition, offset) in earliest)
            _consumer.Seek(new TopicPartitionOffset(topicPartition, offset));
    }

    /// <summary>
    /// Success   -> parked or re-queued; safe to commit past the original.
    /// Transient -> could not park right now; the caller must NOT commit.
    /// Failure   -> can never be parked; the caller should commit past it and log loudly.
    /// </summary>
    private async Task<Result> RouteFailureAsync(ConsumeResult<string, string> result, string reason)
    {
        var attempts = DeadLetterProducer.GetAttempts(result) + 1;
        var key = result.Message.Key;
        var value = result.Message.Value;

        if (attempts >= _maxAttempts)
        {
            var parked = await _deadLetter.ProduceAsync(_deadLetter.DeadLetterTopic, key, value, attempts, reason);
            if (parked.IsSuccess)
                _logger.LogWarning("Dead-lettered after {Attempts} attempt(s) -> {Topic}", attempts, _deadLetter.DeadLetterTopic);
            return parked;
        }

        // Re-produce to the source topic with an incremented attempt header. Bounded, because
        // the next pass reads that header and dead-letters once it reaches _maxAttempts.
        var requeued = await _deadLetter.ProduceAsync(_topic, key, value, attempts, reason);
        if (requeued.IsSuccess)
            _logger.LogWarning("Rejected; retry {Attempts}/{Max} re-queued -> {Topic}", attempts, _maxAttempts, _topic);
        return requeued;
    }

    public override void Dispose()
    {
        _subscription?.Dispose();
        _consumer.Dispose();
        // NOTE: _deadLetter is a DI singleton — the container owns its lifetime, not us.
        base.Dispose();
    }
}
