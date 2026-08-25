using System.Collections.Concurrent;
using Confluent.Kafka;
using KafkaPatterns.Infrastructure;
using KafkaPatterns.Infrastructure.Messaging;

namespace KafkaPatterns.Patterns.Cdc;

public record CustomerChanged(string Op, int CustomerId, string? Email, long Version);

public static class CdcOutboxDemo
{
    private const string Topic = "cdc.customers";

    private sealed record OutboxRow(long Id, string Key, CustomerChanged Payload)
    {
        public bool Dispatched { get; set; }
    }

    private static readonly ConcurrentDictionary<int, string> CustomersTable = new();
    private static readonly List<OutboxRow> Outbox = [];
    private static readonly object DbTx = new();
    private static long _seq;

    public static async Task RunAsync(CancellationToken ct)
    {
        await TopicAdmin.EnsureTopicsAsync(1, Topic);

        var relay = Task.Run(() => OutboxRelay(ct), ct);
        var sink  = Task.Run(() => Sink(ct), ct);

        // Business operations: table + outbox change atomically 
        SaveCustomer(1, "vitah@example.com", "c");
        SaveCustomer(2, "ivan@example.com",  "c");
        SaveCustomer(1, "vitah.p@example.com", "u");
        DeleteCustomer(2);

        await Task.Delay(4000, ct);
        await Task.WhenAny(Task.WhenAll(relay, sink), Task.Delay(1000));
    }

    private static void SaveCustomer(int id, string email, string op)
    {
        lock (DbTx)
        {
            CustomersTable[id] = email;
            Outbox.Add(new OutboxRow(++_seq, id.ToString(), new CustomerChanged(op, id, email, _seq)));
        }
        TopicAdmin.Log("app", $"db tx committed: {op} customer {id}");
    }

    private static void DeleteCustomer(int id)
    {
        lock (DbTx)
        {
            CustomersTable.TryRemove(id, out _);
            Outbox.Add(new OutboxRow(++_seq, id.ToString(), new CustomerChanged("d", id, null, _seq)));
        }
        TopicAdmin.Log("app", $"db tx committed: d customer {id}");
    }

    private static async Task OutboxRelay(CancellationToken ct)
    {
        using var producer = new ProducerBuilder<string, CustomerChanged>(KafkaConfig.Producer())
            .SetValueSerializer(new Serializer<CustomerChanged>()).Build();

        while (!ct.IsCancellationRequested)
        {
            OutboxRow[] batch;
            lock (DbTx) batch = Outbox.Where(r => !r.Dispatched).OrderBy(r => r.Id).Take(50).ToArray();

            foreach (var row in batch)
            {
                await producer.ProduceAsync(Topic,
                    new Message<string, CustomerChanged> { Key = row.Key, Value = row.Payload }, ct);
                lock (DbTx) row.Dispatched = true; // in SQL: UPDATE outbox SET dispatched = 1
                TopicAdmin.Log("outbox-relay", $"published change #{row.Id} ({row.Payload.Op} {row.Key})");
            }
            await Task.Delay(500, ct); 
        }
    }

    private static void Sink(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, CustomerChanged>(KafkaConfig.Consumer("search-indexer"))
            .SetValueDeserializer(new Serializer<CustomerChanged>()).Build();
        consumer.Subscribe(Topic);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                var c = cr.Message.Value;
                TopicAdmin.Log("search-indexer", c.Op switch
                {
                    "c" => $"index customer {c.CustomerId} ({c.Email})",
                    "u" => $"reindex customer {c.CustomerId} ({c.Email})",
                    "d" => $"drop customer {c.CustomerId} from index",
                    _   => "unknown op"
                });
                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException) { }
        finally { consumer.Close(); }
    }
}
