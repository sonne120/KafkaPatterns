using Confluent.Kafka;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging;

namespace KafkaPatterns.Patterns.WorkQueue;

public record ResizeJob(string ImageId, int TargetWidth);

public static class WorkQueueDemo
{
    private const string Topic = "images.resize-jobs";

    public static async Task RunAsync(CancellationToken ct)
    {
        await TopicAdmin.EnsureTopicsAsync(3, Topic);

        using var demoCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var workers = Enumerable.Range(1, 3)
            .Select(i => Task.Run(() => Worker($"worker-{i}", demoCts.Token), CancellationToken.None))
            .ToArray();

        using var producer = new ProducerBuilder<string, ResizeJob>(KafkaConfig.Producer())
            .SetValueSerializer(new Serializer<ResizeJob>())
            .Build();

        for (var i = 1; i <= 12 && !ct.IsCancellationRequested; i++)
        {
            var job = new ResizeJob($"img-{i:D3}.jpg", 1280);
            // key = imageId => same image always lands on the same partition/worker
            await producer.ProduceAsync(Topic,
                new Message<string, ResizeJob> { Key = job.ImageId, Value = job }, ct);
        }

        // One job no decoder will ever handle, so the poison path is visible too.
        var poison = new ResizeJob("notes-013.txt", 1280);
        await producer.ProduceAsync(Topic,
            new Message<string, ResizeJob> { Key = poison.ImageId, Value = poison }, ct);

        TopicAdmin.Log("dispatcher", "13 jobs enqueued (one of them undecodable)");

        await Task.Delay(6000, ct);
        await DemoRunner.ShutdownAsync(demoCts, workers);
    }

    private static void Worker(string name, CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, ResizeJob>(KafkaConfig.Consumer("image-resizer"))
            .SetValueDeserializer(new Serializer<ResizeJob>())
            .Build();
        consumer.Subscribe(Topic);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                var job = cr.Message.Value;

                switch (Resize(job))
                {
                    case { IsSuccess: true }:
                        TopicAdmin.Log(name, $"resized {job.ImageId} -> {job.TargetWidth}px  (p{cr.Partition.Value})");
                        consumer.Commit(cr);
                        break;

                    // Transient -> rewind and retry this same message.
                    case { IsTransient: true } deferred:
                        TopicAdmin.Log(name, $"{job.ImageId} deferred ({deferred.Error}) — rewinding to retry");
                        consumer.Seek(cr.TopicPartitionOffset);
                        Thread.Sleep(500);
                        break;

                    // Failure -> poison; commit past it so the partition keeps moving. Blocking the
                    // whole partition on one undecodable file would starve every job behind it.
                    case var rejected:
                        TopicAdmin.Log(name, $"{job.ImageId} rejected ({rejected.Error}) — committing past it");
                        consumer.Commit(cr);
                        break;
                }
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }

    private static Result Resize(ResizeJob job)
    {
        Thread.Sleep(Random.Shared.Next(200, 600)); 

        if (!job.ImageId.EndsWith(".jpg", StringComparison.OrdinalIgnoreCase))
            return Result.Failure("no decoder for this file type");

        return Random.Shared.Next(0, 100) < 10
            ? Result.Transient("resize pool saturated")
            : Result.Success();
    }
}
