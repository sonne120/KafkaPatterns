using KafkaPatterns.Infrastructure;
using ksqlDB.RestApi.Client.KSql.Linq;
using ksqlDB.RestApi.Client.KSql.Linq.PullQueries;
using ksqlDB.RestApi.Client.KSql.Query.Context;
using ksqlDB.RestApi.Client.KSql.Query.Options;
using ksqlDB.RestApi.Client.KSql.RestApi;
using ksqlDB.RestApi.Client.KSql.RestApi.Http;
using ksqlDB.RestApi.Client.KSql.RestApi.Serialization;
using ksqlDB.RestApi.Client.KSql.RestApi.Statements;
using ksqlDB.RestApi.Client.KSql.RestApi.Statements.Properties;

namespace KafkaPatterns.Patterns.Ksql;

public class RideRequest
{
    public string City   { get; set; } = "";
    public string RideId { get; set; } = "";
    public double Fare   { get; set; }
}
public class RidesPerCity
{
    public string City      { get; set; } = "";
    public long   Rides     { get; set; }
    public double TotalFare { get; set; }
}

///   1. DDL     — CREATE STREAM over a topic (schema-on-read)
///   2. CSAS    — CREATE TABLE AS SELECT: a continuously maintained aggregate
///   3. push    — a standing query that streams matching rows as they arrive
///   4. inserts — INSERT INTO the stream (i.e. produce through ksqlDB)
public static class KsqlDemo
{
    private const string StreamName = "ride_requests";
    private const string TableName  = "rides_per_city";
    private const string Topic      = "ksql.rides";

    public static async Task RunAsync(CancellationToken ct)
    {
        var ksqlDbUrl = Environment.GetEnvironmentVariable("KSQL_URL") ?? "http://localhost:8088";
        TopicAdmin.Log("ksql", $"connecting to ksqlDB at {ksqlDbUrl}");

        using var httpClient = new HttpClient { BaseAddress = new Uri(ksqlDbUrl) };
        var restApi = new KSqlDbRestApiClient(new HttpClientFactory(httpClient));

        // 1. DDL: a STREAM over a Kafka topic — schema-on-read, topic created if missing
        var metadata = new EntityCreationMetadata(kafkaTopic: Topic, partitions: 1)
        {
            EntityName = StreamName,
            ShouldPluralizeEntityName = false,
            ValueFormat = SerializationFormats.Json
        };
        var ddl = await restApi.CreateOrReplaceStreamAsync<RideRequest>(metadata, ct);
        TopicAdmin.Log("ksql", $"CREATE STREAM {StreamName}: {(int)ddl.StatusCode}");

        // 2. CSAS: server-side, continuously maintained aggregate 
        var csas = new KSqlDbStatement($"""
            CREATE TABLE IF NOT EXISTS {TableName} AS
              SELECT city,
                     COUNT(*)  AS rides,
                     SUM(fare) AS totalFare
              FROM {StreamName}
              GROUP BY city
              EMIT CHANGES;
            """);
        var csasResp = await restApi.ExecuteStatementAsync(csas, ct);
        TopicAdmin.Log("ksql", $"CREATE TABLE {TableName} AS SELECT: {(int)csasResp.StatusCode}");

        // 3. Push query: a standing subscription — rows stream in as they match
        await using var context = new KSqlDBContext(new KSqlDBContextOptions(ksqlDbUrl)
        {
            ShouldPluralizeFromItemName = false
        });

        using var subscription = context.CreatePushQuery<RideRequest>(StreamName)
            .WithOffsetResetPolicy(AutoOffsetReset.Earliest)
            .Where(r => r.Fare > 10)
            .Subscribe(
                r  => TopicAdmin.Log("push-query", $"fare>10: {r.RideId} {r.City} {r.Fare:F2}"),
                ex => TopicAdmin.Log("push-query",
                    ex is OperationCanceledException ? "closed" : $"error: {ex.Message}"));

        // 4. Inserts THROUGH ksqlDB (it produces to the backing topic for us)
        var insertProps = new InsertProperties { EntityName = StreamName, ShouldPluralizeEntityName = false };
        RideRequest[] rides =
        [
            new() { RideId = "R-1", City = "kyiv", Fare = 8.50 },
            new() { RideId = "R-2", City = "kyiv", Fare = 14.20 },
            new() { RideId = "R-3", City = "lviv", Fare = 11.00 },
            new() { RideId = "R-4", City = "kyiv", Fare = 22.75 },
        ];
        foreach (var ride in rides)
        {
            await restApi.InsertIntoAsync(ride, insertProps, ct);
            TopicAdmin.Log("producer", $"INSERT INTO {StreamName}: {ride.RideId} ({ride.City}, {ride.Fare:F2})");
            await Task.Delay(400, ct);
        }

        // Let the CSAS table catch up, then read the aggregate back with a PULL query.

        var deadline = DateTime.UtcNow.AddSeconds(30);
        var rows = 0;

        while (rows == 0 && DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
        {
        await Task.Delay(8000, ct);

            await foreach (var row in context.CreatePullQuery<RidesPerCity>(TableName).GetManyAsync(ct))
            {
                if (row is null) continue;
                rows++;
                TopicAdmin.Log("pull-query", $"{row.City}: {row.Rides} rides, {row.TotalFare:F2} total");
            }
        }

        if (rows == 0)
            TopicAdmin.Log("pull-query", "table still empty — the CSAS query had not caught up");

        await Task.Delay(2000, ct); 
    }
}
