using Confluent.Kafka;

namespace KafkaPatterns.Infrastructure.Messaging;

public static class ConsumerExtensions
{
    public static List<ConsumeResult<TKey, TValue>> ConsumeBatch<TKey, TValue>(
        this IConsumer<TKey, TValue> consumer,
        TimeSpan maxWaitTime,
        int maxBatchSize,
        CancellationToken cancellationToken)
    {
        var batch = new List<ConsumeResult<TKey, TValue>>(maxBatchSize);
        var deadline = DateTime.UtcNow.Add(maxWaitTime);

        while (batch.Count < maxBatchSize && DateTime.UtcNow < deadline)
        {
            try
            {
                var remainingTime = deadline - DateTime.UtcNow;
                if (remainingTime <= TimeSpan.Zero)
                    break;

                var result = consumer.Consume(remainingTime);
                
                if (result != null)
                {
                    batch.Add(result);
                }
                else
                {
                    break;
                }
            }
            catch (ConsumeException)
            {
                throw; 
            }
        }

        return batch;
    }
}