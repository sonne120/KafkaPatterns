using KafkaPatterns.Infrastructure.Messaging;
using KafkaPatterns.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging.Producers;
using KafkaPatterns.Infrastructure.Messaging.Serialization;

namespace KafkaPatterns.Patterns.Cdc.Variants;

/// <summary>
/// The plainest variant: poll every 500ms, publish, mark dispatched. No state machine, no retry
/// accounting, no batch reporting — just the loop, so the pattern itself is easy to see.
/// (Classic TxOutbox)
/// </summary>
public class PollingOutboxRelay : BackgroundService
{
    private readonly IDbContextFactory<CdcDbContext> _dbContextFactory;
    private readonly IKafkaMessagePub _publisher;
    private readonly ILogger<PollingOutboxRelay> _logger;

    public PollingOutboxRelay(
        IDbContextFactory<CdcDbContext> dbContextFactory,
        IKafkaMessagePub publisher,
        ILogger<PollingOutboxRelay> logger)
    {
        _dbContextFactory = dbContextFactory;
        _publisher = publisher;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await using var db = await _dbContextFactory.CreateDbContextAsync(stoppingToken);

                var batch = await db.OutboxMessages
                    .Where(m => m.ProcessedOnUtc == null)
                    .OrderBy(m => m.Sequence)
                    .Take(50)
                    .ToListAsync(stoppingToken);

                var dispatched = 0;

                foreach (var message in batch)
                {
                    IDomainEvent? domainEvent;
                    try
                    {
                        domainEvent = MessageJson.Deserialize<IDomainEvent>(message.Content);
                    }
                    catch (JsonException ex)
                    {
                        message.Error = ex.Message;
                        message.ProcessedOnUtc = DateTime.UtcNow;   // poison
                        continue;
                    }

                    if (domainEvent is null)
                    {
                        message.Error = "Failed to deserialize";
                        message.ProcessedOnUtc = DateTime.UtcNow;
                        continue;
                    }

                    var published = await _publisher.PublishAsync(message.Topic, domainEvent, message.EntityId, stoppingToken);

                    if (published.IsFailure)
                    {
                        _logger.LogError("Failed to relay outbox row {Id}: {Error}; will retry next poll.",
                            message.Id, published.Error);
                        break;
                    }

                    message.ProcessedOnUtc = DateTime.UtcNow;   // "UPDATE outbox SET dispatched = 1"
                    dispatched++;
                }

                if (batch.Count > 0)
                    await db.SaveChangesAsync(stoppingToken);

                if (dispatched > 0)
                    _logger.LogInformation("Outbox Relay pushed {Count} messages.", dispatched);

                await Task.Delay(500, stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
             _logger.LogInformation("Polling outbox relay stopped");
        }
    }
}
