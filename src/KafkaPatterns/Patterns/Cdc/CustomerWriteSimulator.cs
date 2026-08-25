using KafkaPatterns.Infrastructure.Messaging;
using KafkaPatterns.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using KafkaPatterns.Patterns.Cdc.Variants;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging.Serialization;

namespace KafkaPatterns.Patterns.Cdc;

public class CustomerWriteSimulator : BackgroundService
{
    private readonly IDbContextFactory<CdcDbContext> _dbContextFactory;
    private readonly ILogger<CustomerWriteSimulator> _logger;

    public CustomerWriteSimulator(IDbContextFactory<CdcDbContext> dbContextFactory, ILogger<CustomerWriteSimulator> logger)
    {
        _dbContextFactory = dbContextFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Simulating customer writes; each one commits a business row and an outbox row together...");

        for (int i = 1; i <= 5 && !stoppingToken.IsCancellationRequested; i++)
        {
            await WriteCustomerAsync(i, $"customer{i}@example.com", "Create", stoppingToken);
            await Task.Delay(1000, stoppingToken);
        }

        if (!stoppingToken.IsCancellationRequested)
            await WriteCustomerAsync(1, "customer1.updated@example.com", "Update", stoppingToken);
    }

    private async Task WriteCustomerAsync(int customerId, string email, string operation, CancellationToken ct)
    {
        await using var db = await _dbContextFactory.CreateDbContextAsync(ct);

        var customer = await db.Customers.FindAsync([customerId], ct);
        if (customer is null)
        {
            customer = new CustomerEntity { Id = customerId, Email = email, Version = 1 };
            db.Customers.Add(customer);
        }
        else
        {
            customer.Email = email;
            customer.Version++;
        }

        var @event = new CustomerCdcEvent
        {
            CustomerId = customerId,
            Email = email,
            Operation = operation,
            Version = customer.Version
        };

        db.OutboxMessages.Add(new OutboxMessageEntity
        {
            Topic = CdcTopics.CustomerEvents,
            EntityType = nameof(CustomerEntity),
            EntityId = customerId.ToString(),
            Content = MessageJson.Serialize<IDomainEvent>(@event),
            OccurredOnUtc = @event.OccurredOnUtc
        });

        await db.SaveChangesAsync(ct);
        _logger.LogInformation("db tx committed: {Operation} customer {CustomerId}", operation, customerId);
    }
}
