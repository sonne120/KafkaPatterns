using KafkaPatterns.Infrastructure.Messaging;
using KafkaPatterns.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace KafkaPatterns.Patterns.Cdc.Variants;


public class StateMachineOutboxRelay : BackgroundService
{
    private readonly IDbContextFactory<CdcDbContext> _dbContextFactory;
    private readonly IKafkaMessagePub _publisher;
    private readonly ILogger<StateMachineOutboxRelay> _logger;
    private readonly TimeSpan _interval = TimeSpan.FromSeconds(2);
    private const int BatchSize = 100;
    private const int MaxRetryCount = 3;

    public StateMachineOutboxRelay(
        IDbContextFactory<CdcDbContext> dbContextFactory,
        IKafkaMessagePub publisher,
        ILogger<StateMachineOutboxRelay> logger)
    {
        _dbContextFactory = dbContextFactory;
        _publisher = publisher;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox Processor started");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await ProcessOutboxMessagesAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing outbox messages");
                }

                await Task.Delay(_interval, stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            // shutdown
        }

        _logger.LogInformation("Outbox Processor stopped");
    }

    private async Task ProcessOutboxMessagesAsync(CancellationToken cancellationToken)
    {
        await using var db = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var pendingMessages = await db.OutboxMessages
            .Where(m => m.State == OutboxState.Pending)
            .OrderBy(m => m.Sequence)    
            .Take(BatchSize)
            .ToListAsync(cancellationToken);

        if (pendingMessages.Count == 0)
            return;

        foreach (var message in pendingMessages)
        {
            _logger.LogDebug("Processing outbox message: Id={MessageId}, EntityType={EntityType}, Topic={Topic}",
                message.Id, message.EntityType, message.Topic);

            IDomainEvent? domainEvent;
            try
            {
                domainEvent = MessageJson.Deserialize<IDomainEvent>(message.Content);
            }
            catch (JsonException ex)
            {
                message.State = OutboxState.Failed;
                message.Error = $"Failed to deserialize: {ex.Message}";
                message.ProcessedOnUtc = DateTime.UtcNow;
                _logger.LogWarning("Outbox message {MessageId} is poison: {Error}", message.Id, ex.Message);
                continue;
            }

            if (domainEvent is null)
            {
                message.State = OutboxState.Failed;
                message.Error = "Failed to deserialize";
                message.ProcessedOnUtc = DateTime.UtcNow;
                continue;
            }

           
            var published = await _publisher.PublishAsync(message.Topic, domainEvent, message.EntityId, cancellationToken);

            if (published.IsSuccess)
            {
                message.State = OutboxState.Completed;
                message.ProcessedOnUtc = DateTime.UtcNow;
                message.Error = null;

                _logger.LogInformation("Outbox message published successfully: Id={MessageId}, EntityType={EntityType}",
                    message.Id, message.EntityType);
                continue;
            }

            message.RetryCount++;
            message.Error = published.Error;

            if (message.RetryCount >= MaxRetryCount)
            {
            
                message.State = OutboxState.Failed;
                message.ProcessedOnUtc = DateTime.UtcNow;
                _logger.LogWarning("Message {MessageId} exceeded max retry count ({Max}), marking as failed: {Error}",
                    message.Id, MaxRetryCount, published.Error);
            }
            else
            {
                message.State = OutboxState.Pending;
                _logger.LogWarning("Publish failed for {MessageId} (attempt {Attempt}/{Max}): {Error}; will retry",
                    message.Id, message.RetryCount, MaxRetryCount, published.Error);
            }

          
            break;
        }

        await db.SaveChangesAsync(cancellationToken);
    }
}
