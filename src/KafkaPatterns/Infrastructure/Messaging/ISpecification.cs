namespace KafkaPatterns.Infrastructure.Messaging;

public interface ISpecification<T>
{
    bool IsSatisfiedBy(T entity);
}