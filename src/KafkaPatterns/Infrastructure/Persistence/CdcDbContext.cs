using Microsoft.EntityFrameworkCore;

namespace KafkaPatterns.Infrastructure.Persistence;

public enum OutboxState { Pending, Processing, Completed, Failed }


public class OutboxMessageEntity
{
    private static long _sequence;

    public Guid Id { get; set; } = Guid.NewGuid();

    public long Sequence { get; set; } = Interlocked.Increment(ref _sequence);
    public string Topic { get; set; } = string.Empty;
    public string EntityType { get; set; } = string.Empty;

    
    public string EntityId { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;

    public DateTime OccurredOnUtc { get; set; }
    public DateTime? ProcessedOnUtc { get; set; }
    public OutboxState State { get; set; } = OutboxState.Pending;
    public int RetryCount { get; set; }
    public string? Error { get; set; }
}


public class CustomerEntity
{
    public int Id { get; set; }
    public string? Email { get; set; }
    public long Version { get; set; }
    public bool IsDeleted { get; set; }
}


public class CustomerSearchEntity
{
    public int CustomerId { get; set; }
    public string? Email { get; set; }
    public bool IsDeleted { get; set; }
}


public class ProcessedMessageEntity
{
    public string TransactionId { get; set; } = string.Empty;
    public DateTime ProcessedOnUtc { get; set; }
    public string Payload { get; set; } = string.Empty;
}

public class CdcDbContext : DbContext
{
    public DbSet<OutboxMessageEntity> OutboxMessages => Set<OutboxMessageEntity>();
    public DbSet<CustomerEntity> Customers => Set<CustomerEntity>();
    public DbSet<CustomerSearchEntity> CustomerSearchIndex => Set<CustomerSearchEntity>();
    public DbSet<ProcessedMessageEntity> ProcessedMessages => Set<ProcessedMessageEntity>();

    public CdcDbContext(DbContextOptions<CdcDbContext> options) : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OutboxMessageEntity>(e =>
        {
            e.HasKey(x => x.Id);
            e.Property(x => x.Topic).IsRequired();
            e.Property(x => x.Content).IsRequired();
            e.HasIndex(x => new { x.State, x.Sequence });
        });

        modelBuilder.Entity<CustomerEntity>(e => e.HasKey(x => x.Id));
        modelBuilder.Entity<CustomerSearchEntity>(e => e.HasKey(x => x.CustomerId));
        modelBuilder.Entity<ProcessedMessageEntity>(e => e.HasKey(x => x.TransactionId));

        base.OnModelCreating(modelBuilder);
    }
}
public static class CdcTopics
{

    public const string Customers = "cdc.customers";

    public const string CustomerEvents = "cdc.customer-events";
}
