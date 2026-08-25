using KafkaPatterns.Infrastructure.Caching;
using KafkaPatterns.Infrastructure.Messaging;
using KafkaPatterns.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging.Serialization;

namespace KafkaPatterns.Patterns.ResilientConsumer;

public class RxMessageProcessor
{
    private readonly ISerializer _serializer;
    private readonly IIdempotencyCache _idempotencyCache;
    private readonly IDbContextFactory<CdcDbContext> _dbContextFactory;
    private readonly ILogger<RxMessageProcessor> _logger;

    public RxMessageProcessor(
        ISerializer serializer,
        IIdempotencyCache idempotencyCache,
        IDbContextFactory<CdcDbContext> dbContextFactory,
        ILogger<RxMessageProcessor> logger)
    {
        _serializer = serializer;
        _idempotencyCache = idempotencyCache;
        _dbContextFactory = dbContextFactory;
        _logger = logger;
    }

    /// Result.Success   = handled; commit.
    /// Result.Failure   = poison; route to the dead-letter topic and commit the source partition.
    /// Result.Transient = dependency unreachable; rewind the partition and try again later.


    public async Task<Result> ProcessMessageAsync(string payload, CancellationToken stoppingToken)
    {
        var envelope = _serializer.TryDeserialize<PaymentCommand>(payload);
        if (envelope.IsFailure)
        {
            _logger.LogError("Malformed message; routing to dead-letter: {Error}", envelope.Error);
            return Result.Failure(envelope.Error);
        }

        var txId = envelope.Value.EventId.ToString();

        // 1. Fast-path filter (Redis)
        var seen = await _idempotencyCache.WasProcessedAsync(txId, stoppingToken);
        if (seen.IsSuccess && seen.Value)
        {
            _logger.LogWarning("Message {Key} skipped (Idempotency Hit)", txId);
            return Result.Success();
        }

        // 2. Durable commit, idempotent on the ledger's primary key
        var committed = await TryCommitAsync(txId, payload, stoppingToken);
        if (committed.IsFailure)
            return committed;

        // 3. Mark AFTER the durable commit
        await _idempotencyCache.MarkProcessedAsync(txId, stoppingToken);

        _logger.LogInformation("Message {Key} committed fully.", txId);
        return Result.Success();
    }

    private async Task<Result> TryCommitAsync(string txId, string payload, CancellationToken cancellationToken)
    {
        await Task.Delay(10, cancellationToken); 

        if (Random.Shared.Next(0, 100) < 10)
        {
            return Result.Transient("postgres unavailable (simulated)");
        }

        await using var db = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        if (await db.ProcessedMessages.FindAsync([txId], cancellationToken) is not null)
            return Result.Success();

        db.ProcessedMessages.Add(new ProcessedMessageEntity
        {
            TransactionId = txId,
            ProcessedOnUtc = DateTime.UtcNow,
            Payload = payload
        });

        await db.SaveChangesAsync(cancellationToken);
        return Result.Success();
    }
}
