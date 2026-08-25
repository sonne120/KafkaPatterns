namespace KafkaPatterns.Infrastructure.Messaging;

public interface IDomainEvent
{
    Guid EventId { get; }
    DateTime OccurredOnUtc { get; }
}