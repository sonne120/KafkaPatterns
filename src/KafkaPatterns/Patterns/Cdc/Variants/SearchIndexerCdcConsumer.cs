using KafkaPatterns.Infrastructure.Messaging;
using KafkaPatterns.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging.Configuration;
using KafkaPatterns.Infrastructure.Messaging.Consumers;

namespace KafkaPatterns.Patterns.Cdc.Variants;

public class CustomerCdcEvent : IDomainEvent
{
    public Guid EventId { get; init; } = Guid.NewGuid();
    public DateTime OccurredOnUtc { get; init; } = DateTime.UtcNow;

    public string Operation { get; init; } = string.Empty; // "Create", "Update", "Delete"
    public int CustomerId { get; init; }
    public string? Email { get; init; }
    public long Version { get; init; }
}

public class SearchIndexerCdcConsumer : KafkaBackgroundConsumer<CustomerCdcEvent>
{
    private readonly IDbContextFactory<CdcDbContext> _dbContextFactory;
    private readonly ILogger<SearchIndexerCdcConsumer> _logger;

    public SearchIndexerCdcConsumer(
        IOptions<KafkaSettings> settings,
        IDbContextFactory<CdcDbContext> dbContextFactory,
        ILogger<SearchIndexerCdcConsumer> logger)
        : base(settings, logger, CdcTopics.CustomerEvents)
    {
        _dbContextFactory = dbContextFactory;
        _logger = logger;
    }

    protected override async Task<Result> ProcessMessageAsync(CustomerCdcEvent message, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Received CDC event: EventId={EventId}, Operation={Operation}, EntityId={CustomerId}",
            message.EventId, message.Operation, message.CustomerId);

        try
        {
            await using var db = await _dbContextFactory.CreateDbContextAsync(cancellationToken);
            var existing = await db.CustomerSearchIndex.FindAsync([message.CustomerId], cancellationToken);

            switch (message.Operation)
            {
                case "Create":
                case "Update":
                    if (existing is null)
                    {
                        db.CustomerSearchIndex.Add(new CustomerSearchEntity
                        {
                            CustomerId = message.CustomerId,
                            Email = message.Email
                        });
                        _logger.LogInformation("Read model created/indexed for customer {CustomerId}", message.CustomerId);
                    }
                    else
                    {
                        existing.Email = message.Email;
                        existing.IsDeleted = false;
                        _logger.LogInformation("Read model updated/re-indexed for customer {CustomerId}", message.CustomerId);
                    }
                    break;

                case "Delete":
                    if (existing is not null)
                        existing.IsDeleted = true;
                    _logger.LogInformation("Read model soft-deleted for customer {CustomerId}", message.CustomerId);
                    break;

                default:
                    _logger.LogWarning("Unknown CDC operation: {Operation}", message.Operation);
                    return Result.Failure($"unknown CDC operation '{message.Operation}'");
            }

            await db.SaveChangesAsync(cancellationToken);
            return Result.Success();   // commits the offset
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing CDC event for customer {CustomerId}", message.CustomerId);
        
            return Result.Transient(ex.Message);
        }
    }
}
