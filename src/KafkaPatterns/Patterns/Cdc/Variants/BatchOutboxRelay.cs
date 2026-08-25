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

public class BatchOutboxRelay : BackgroundService
{
    private readonly IDbContextFactory<CdcDbContext> _dbContextFactory;
    private readonly IKafkaMessagePub _publisher;
    private readonly ILogger<BatchOutboxRelay> _logger;

    public BatchOutboxRelay(
        IDbContextFactory<CdcDbContext> dbContextFactory,
        IKafkaMessagePub publisher,
        ILogger<BatchOutboxRelay> logger)
    {
        _dbContextFactory = dbContextFactory;
        _publisher = publisher;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Batch outbox relay started");

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

                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            // shutdown
        }

        _logger.LogInformation("Batch outbox relay stopped");
    }

    private async Task ProcessOutboxMessagesAsync(CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var messages = await dbContext.OutboxMessages
            .Where(m => m.ProcessedOnUtc == null)
            .OrderBy(m => m.Sequence)
            .Take(20)
            .ToListAsync(cancellationToken);

        if (messages.Count == 0)
        {
            return;
        }

        _logger.LogInformation("Found {Count} outbox messages to process via EF Core", messages.Count);

        var published = 0;

        foreach (var message in messages)
        {
            IDomainEvent? domainEvent;
            try
            {
                // The discriminator is resolved against an allow-list of IDomainEvent types.
                domainEvent = MessageJson.Deserialize<IDomainEvent>(message.Content);
            }
            catch (JsonException ex)
            {
                _logger.LogWarning("Failed to deserialize message {MessageId}: {Error}", message.Id, ex.Message);
                message.Error = $"Failed to deserialize: {ex.Message}";
                message.State = OutboxState.Failed;
                message.ProcessedOnUtc = DateTime.UtcNow;   // poison: stop reprocessing it
                continue;
            }

            if (domainEvent is null)
            {
                _logger.LogWarning("Message {MessageId} deserialized to null", message.Id);
                message.Error = "Failed to deserialize";
                message.State = OutboxState.Failed;
                message.ProcessedOnUtc = DateTime.UtcNow;
                continue;
            }

            var result = await _publisher.PublishAsync(message.Topic, domainEvent, message.EntityId, cancellationToken);

            if (result.IsFailure)
            {
                 
                message.Error = result.Error;
                message.RetryCount++;
                _logger.LogError("Could not publish outbox message {MessageId}: {Error}; leaving pending",
                    message.Id, result.Error);
                break;
            }

            message.Error = null;
            message.State = OutboxState.Completed;
            message.ProcessedOnUtc = DateTime.UtcNow;
            published++;

            _logger.LogInformation("Successfully processed EF outbox message {MessageId} of type {Type}", message.Id, message.EntityType);
        }

        await dbContext.SaveChangesAsync(cancellationToken);
        _logger.LogInformation("Finished processing {Count}/{Total} outbox messages mapped via EF Core", published, messages.Count);
    }
}
