# KafkaPatterns

> **KafkaPatterns — the classic Kafka patterns implemented in C# / .NET 8, plus Kafka Streams
> (Streamiz) and ksqlDB. The same transactional outbox is then wired four different ways as
> DI-hosted variants (state machine, batch job, plain polling relay, Rx consumer), with an
> optional Redis fast-path duplicate filter — the cache is a fast path, the ledger is the
> guarantee. 18 runnable commands, one broker in Docker.**

Not toy snippets: every demo uses idempotent producers (`Acks=All`), manual offset commits
**after** processing, `ReadCommitted` isolation where transactions are involved, and deliberately
chosen message keys. Failure is modelled with `Result`, not `bool` — a consumer distinguishes
**handled**, **poison**, and **transient**, and a genuine retry has to `Seek` back, because *not
committing is not a retry*.

**Contents:** [Run it](#run-it) · [Pattern catalog](#pattern-catalog) · [The 12 core patterns](#1-publish--subscribe) · [Resilient consumer](#13-resilient-consumer) · [Kafka Streams](#14-kafka-streams-streamiz) · [ksqlDB](#15-ksqldb) · [CDC variants](#cdc-variants--one-outbox-three-relays) · [Redis duplicate filter](#redis-duplicate-filter-optional) · [Failure model](#failure-model-result-not-bool) · [Production notes](#production-notes-baked-into-the-code)

## Run it

```
docker compose up -d kafka    # single-node Kafka (KRaft, no ZooKeeper) on localhost:9092
cd src/KafkaPatterns
dotnet run -- <pattern>       # dotnet run -- dlq

docker compose up -d redis    # optional dedup cache for cdc-*  (REDIS_CONNECTION=localhost:6379)
docker compose up -d ksqldb   # required by `dotnet run -- ksql` (KSQL_URL overrides :8088)
```


## Pattern catalog

| Command        | Pattern               | What the demo shows                                                                               |
| -------------- | --------------------- | ------------------------------------------------------------------------------------------------- |
| `pubsub`       | Publish / Subscribe   | One topic, two independent consumer groups — every group gets every message                       |
| `workqueue`    | Queue (Work Queue)    | One group, 3 workers, jobs split by partitions, commit-after-processing                           |
| `eventsource`  | Event Sourcing        | Append account events, rebuild balances by replaying from offset 0                                |
| `stream`       | Stream Processing     | Consume-transform-produce with Kafka transactions (`SendOffsetsToTransaction`) = exactly-once     |
| `cdc`          | Change Data Capture   | Transactional-outbox relay: DB change + outbox row → poller → Kafka (Debezium's app-level cousin) |
| `dlq`          | Dead Letter Queue     | Bounded retries with backoff, then park to `deadletter` with `attempts` / `x-failure-reason` headers |
| `parentchild`  | Parent-Child Topics   | Derived topic keyed by the SAME key (UserId) — traceability + ordering preserved                  |
| `reqreply`     | Request / Reply       | `correlation-id` + `reply-to` headers, client matches replies via `TaskCompletionSource`          |
| `competing`    | Competing Consumers   | One group competes for partitions; one consumer is killed mid-run → watch the rebalance           |
| `partitioning` | Partitioning Strategy | Default murmur2 key hashing vs a custom region-pinning partitioner                                |
| `saga`         | Saga                  | Order → Payment → Inventory choreography; inventory failure triggers a refund compensation        |
| `windowing`    | Time Windowing        | Tumbling 5s windows over a click stream, flushed by watermark to an output topic                  |
| `resilient`    | Resilient Consumer    | Batched consume with a pluggable strategy: commit, rewind-and-retry, or dead-letter — plus two-tier idempotency |
| `streams`      | Kafka Streams (Streamiz) | The manual `windowing` job as a declared topology — state store + changelog for free           |
| `ksql`         | ksqlDB                | SQL over topics: CREATE STREAM, CSAS aggregate table, push query, pull query, INSERT via REST     |

Two invariants hold across every demo:

- **Durability before acknowledgment** — an offset is committed only after the work it
  represents (a DB write, a downstream produce, a delivered webhook) has succeeded.
- **The key is the contract** — every topic's key is chosen deliberately, because the key is
  what carries ordering, partition placement, and traceability from producer to consumer.

## 1. Publish / Subscribe

One topic, independent consumer groups — classic fan-out. Both groups receive **every** message,
each tracking its own offsets.

```text
┌──────────┐                 ┌─────────────────┐
│ Producer │ ──────────────▶ │  Kafka Topic    │
└──────────┘                 │  orders.placed  │
                             └────────┬────────┘
                                      │ every group gets every message
                      ┌───────────────┴───────────────┐
                      ▼                               ▼
         ┌─────────────────────────┐    ┌──────────────────────────────┐
         │ group: analytics-service│    │ group: notification-service  │
         └─────────────────────────┘    └──────────────────────────────┘
```

Use when: multiple systems need the same data.

## 2. Work Queue

One consumer group, three workers. Kafka assigns partitions to members, so each job is processed
by exactly one worker; the offset is committed **after** the work is done (at-least-once).

```text
┌────────────┐        ┌───────────────────────┐
│ Dispatcher │ ─────▶ │  Kafka Topic          │
└────────────┘        │  images.resize-jobs   │
                      │  (3 partitions)       │
                      └──────────┬────────────┘
                     group: image-resizer
              ┌──────────┬───────┴─────┬──────────┐
              │ p0       │ p1          │ p2       │
              ▼          ▼             ▼
         ┌─────────┐ ┌─────────┐ ┌──────────┐
         │worker-1 │ │worker-2 │ │worker-3  │   commit offset
         └─────────┘ └─────────┘ └──────────┘   AFTER processing
```

Use when: distributing tasks or work among multiple consumers.

## 3. Event Sourcing

The topic **is** the source of truth. Account events are appended; a projector with a fresh
`group.id` replays from offset 0 and rebuilds balances from scratch.

```text
┌────────┐  append events  ┌──────────────────────┐
│ Writer │ ──────────────▶ │ Kafka Topic          │
└────────┘                 │ bank.account-events  │
                           │ (long retention)     │
                           └──────────┬───────────┘
                                      │ replay from offset 0
                                      ▼
                           ┌──────────────────────┐      ┌───────────────────┐
                           │      Projector       │ ───▶ │ read model:       │
                           │  (fresh group.id)    │      │ ACC-1 → 430.00    │
                           └──────────────────────┘      │ ACC-2 → 300.00    │
                                                         └───────────────────┘
```

Use when: you need a full audit trail and reproducible state.

## 4. Stream Processing

.NET has no Kafka Streams, so real projects do **consume-transform-produce**: the output record
and the consumed offsets are committed atomically in one Kafka transaction
(`SendOffsetsToTransaction`) — exactly-once from input topic to output topic.

```text
┌───────────────┐      ┌────────────────────────┐      ┌────────────────────┐
│ Kafka Topic   │ ───▶ │        Enricher        │ ───▶ │ Kafka Topic        │
│ payments.raw  │      │ currency → USD, risk   │      │ payments.enriched  │
└───────────────┘      └───────────┬────────────┘      └─────────┬──────────┘
                                   │                             │
                    one Kafka transaction:                       ▼
                    output record + input offsets     ┌────────────────────┐
                    commit together, or not at all    │ sink               │
                                                      │ (ReadCommitted)    │
                                                      └────────────────────┘
```

Use when: real-time transformations where duplicates are unacceptable.

## 5. Change Data Capture

Production CDC is Debezium tailing the DB WAL. The app-level equivalent you own in code is the
**transactional outbox**: business row + outbox row commit in one DB transaction, then a relay
polls the outbox, publishes, and stamps rows dispatched. Keyed by **entity id**, so every change
to one entity stays on one partition. Three hosted relay styles: [CDC variants](#cdc-variants--one-outbox-three-relays).

```text
┌─────┐  one SaveChangesAsync  ┌──────────────────┐
│ App │ ─────────────────────▶ │  source table    │
└─────┘                        │  + outbox table  │
                               └────────┬─────────┘
                                        │ poll unsent rows
                                        ▼
                               ┌──────────────────┐  publish   ┌────────────────┐
                               │   outbox relay   │ ─────────▶ │ Kafka Topic    │
                               │ (stamp after ack)│            │ cdc.customers  │
                               └──────────────────┘            │ key = entity id│
                                                               └───────┬────────┘
                                                                       ▼
                                                              ┌────────────────┐
                                                              │   consumer →   │
                                                              │   read model   │
                                                              └────────────────┘
```

Use when: downstream systems must see every DB change, reliably and in order — with no
dual-write problem (Kafka down → rows simply wait in the outbox).

## 6. Dead Letter Queue

A failing record is retried with backoff; when retries are exhausted the original payload is
parked to `<topic>.dlq` with diagnostic headers, and the main partition is unblocked.

```text
┌───────────────────┐      ┌──────────┐  success   ✓ delivered, commit
│ Kafka Topic       │ ───▶ │ consumer │ ─────────▶
│ webhooks.outgoing │      └────┬─────┘
└───────────────────┘           │ retries exhausted
                                ▼
                   ┌────────────────────────────────┐     ┌────────────────┐
                   │ Kafka Topic: deadletter        │ ──▶ │  dlq-monitor   │
                   │ x-error · x-original-offset ·  │     │ alert / redrive│
                   │ x-retry-count · x-failed-at    │     └────────────────┘
                   └────────────────────────────────┘
```

Use when: you need to handle poison messages without blocking the stream.

## 7. Parent-Child Topics

The child topic is derived from the parent using the **same key** (`UserId`) — per-user ordering
holds across the whole pipeline and every derived record is traceable to its source partition.

```text
┌─────────────────────┐      ┌────────────────────┐      ┌─────────────────────┐
│ Kafka Topic         │ ───▶ │      Deriver       │ ───▶ │ Kafka Topic         │
│ user.events (parent)│      │ fold events →      │      │ user.profiles(child)│
│ key = UserId        │      │ profile state      │      │ key = UserId        │
└─────────────────────┘      └────────────────────┘      └─────────────────────┘
                                                           SAME key — that is
                                                           the traceability
                                                           contract
```

Use when: maintaining relationships between raw and derived data.

## 8. Request / Reply

Async RPC over Kafka: `correlation-id` + `reply-to` headers on the request, one reply topic per
client instance; the client matches responses to awaiting callers via
`ConcurrentDictionary<corrId, TaskCompletionSource>`.

```text
┌────────┐  {corr-id, reply-to}  ┌──────────────────┐      ┌─────────────────┐
│ Client │ ────────────────────▶ │ Kafka Topic:     │ ───▶ │ pricing-service │
└────────┘                       │ pricing.requests │      └────────┬────────┘
    ▲                            └──────────────────┘               │ corr-id
    │ TaskCompletionSource.SetResult                                │ echoed back
    │                            ┌──────────────────────────┐       │
    └─────────────────────────── │ Kafka Topic:             │ ◀─────┘
                                 │ pricing.replies.client-1 │
                                 └──────────────────────────┘
```

Use when: implementing async request-response workflows.

## 9. Competing Consumers

Members of one group compete for partitions — each message is processed by exactly one member.
Mid-demo one consumer is killed so the rebalance is visible in the logs
(`SetPartitionsAssignedHandler` / `SetPartitionsRevokedHandler`).

```text
┌───────────────────┐        group: email-sender
│ Kafka Topic       │   ┌──────────┬──────────┬──────────┐
│ emails.outgoing   │──▶│ sender-1 │ sender-2 │ sender-3 │
│ (4 partitions)    │   │ (p0,p1)  │  (p2)    │  (p3) ✗  │
└───────────────────┘   └──────────┴──────────┴────┬─────┘
                                                   │ crash → rebalance
                                                   ▼
                                       p3 reassigned to survivors —
                                       no message consumed twice
```

Use when: you want parallel processing without duplicate consumption.

## 10. Partitioning Strategy

Two strategies side by side: the default **murmur2 key hash** (same device → same partition →
per-device ordering), and a **custom partitioner** that pins whole regions to partitions — what
you do when consumers are region-affine (data residency, cache locality).

```text
              ┌──────────┐
              │ Producer │
              └────┬─────┘
      key = 'eu' │   key = 'us'    key = 'apac'
      ┌────────────┼────────────┬─────────────────┐
      ▼            ▼            ▼                 ▼
┌───────────┐ ┌───────────┐ ┌────────────────────────┐
│ partition │ │ partition │ │  partitions 2..N       │
│ 0 (EU)    │ │ 1 (US)    │ │  (hashed spread)       │
└───────────┘ └───────────┘ └────────────────────────┘
```

Use when: you need throughput, ordering guarantees, or placement rules.
## 11. Saga

Choreography across three services: Order → Payment → Inventory. If inventory reservation fails,
a compensating **refund** rolls the money back — the order ends `Cancelled`, never
half-committed.

```
┌───────────────┐ order.created ┌─────────────────┐ payment.captured ┌───────────────────┐
│ order-service │ ────────────▶ │ payment-service │ ───────────────▶ │ inventory-service │
└───────▲───────┘               └────────▲────────┘                  └─────────┬─────────┘
        │                                │                                     │
        │ order.completed  ◀─────────────┼────────────────────  in stock ──────┤
        │                                │                                     │
        │ order.cancelled                │ inventory.failed                    │
        │ (refund issued)  ◀── COMPENSATE┴──────────────────── out of stock ───┘
```

Use when: managing transactions across multiple microservices.


## 12. Time Windowing

Tumbling 5-second windows over a click stream, keyed by page. The window bucket is
`floor(eventTime / windowSize)`; when the watermark passes a window's end, its aggregate is
flushed to the output topic — the manual equivalent of `windowedBy(TimeWindows.of(...))`.

```text
┌────────────────┐      ┌───────────────────────────┐      ┌───────────────────────────┐
│ Kafka Topic    │ ───▶ │         Windower          │ ───▶ │ Kafka Topic               │
│ site.pageviews │      │ bucket = floor(t / 5s)    │      │ site.pageviews.per-window │
└────────────────┘      │ flush when watermark      │      └────────────┬──────────────┘
                        │ passes window end         │                   ▼
                        └───────────────────────────┘   [12:00:05–12:00:10] /home: 7 views
```

Use when: you need time-based aggregations or metrics.
## 13. Resilient Consumer (Rx)

This pattern demonstrates a production-grade, domain-agnostic consumer (`KafkaConsumerRx`) separating the mechanical complexities of Kafka consumption from business logic. It handles reactive batching, dual-tier idempotency, and strict Failure/Result mapping.

`KafkaConsumerRx` takes an incoming topic, batches it implicitly via Rx.NET, and injects the records into your pluggable strategy (`Func<string, CancellationToken, Task<Result>>`). It translates your business outcome into the exact required partition offset movements.

```text
        ┌───────────────────────────────┐
        │ Kafka Topic: cdc.customers    │   
        └──────────────┬────────────────┘
                       │ ConsumeBatch (≤100 records, 5s window)
                       ▼
        ┌───────────────────────────────┐      ┌───────────────────────────────┐
        │      KafkaConsumerRx          │ ───▶ │ PacketShardMessageProcessor   │
        │     (domain-agnostic)         │      │     (business outcome)        │
        └──────────────┬────────────────┘      └───────────────┬───────────────┘
                       │                                       │ 
                       │                                       ▼
                       │                        ┌───────────────────────────────┐
                       │                        │ 1. Redis Cache (Fast Lookup)  │
                       │                        │    Hit → Skip                 │
                       │                        │    Miss → Fallthrough         │
                       │                        └──────────────┬────────────────┘
                       │                                       │ 
                       │                                       ▼
                       │                        ┌───────────────────────────────┐
                       │                        │ 2. EF Core Ledger (Guarantee) │
                       │                        │    Handle record & Save       │
                       │                        └──────────────┬────────────────┘
                       │                                       │
                       ▼                                       ▼ 
   ┌───────────────────────────────────────────────────────────────────────┐
   │ bool? (Outcome mapped to Partition Operations)                        │
   ├───────────────────────────────────────────────────────────────────────┤
   │ true  (Success)   → Commit offset and gracefully move forward         │
   │ null  (Transient) → Seek partition back natively to retry later       │
   │ false (Poison)    → Produce to `deadletter` topic, then commit past   │
   └───────────────────────────────────────────────────────────────────────┘
```

Three things are handled natively so the business layer doesnt get it wrong:

- **Skipping a commit isn't a retry**: Simply failing to commit a message means it gets permanently skipped, not retried. To actually retry a failed message, `KafkaConsumerRx` explicitly calls `Seek` to rewind the entire batch on the affected partitions.
- **Terminal failures get no retry budget:** `Result.Failure` denotes a record that can never be processed (e.g. malformed payload). Re-queueing burns budget. It is routed immediately to the `deadletter` topic via the `DeadLetterProducer` and bypassed.
- **Idempotency is two-tier:** A Redis lookup is placed in front of an EF Core durable ledger keyed by event ID. The ledger is the guarantee. The cache saves the DB round trips, but if Redis is down, it fails-open allowing the ledger to make the final constraint guarantee.

Use when: you want one battle-tested reactive consumer wrapper that every service shares, enforcing strict delivery guarantees.

## 14. Kafka Streams (Streamiz)

Kafka Streams is JVM-only; the community-standard .NET port is **Streamiz.Kafka.Net** — same
DSL, same mechanics: state lives in a local store, the store is backed by a **changelog topic**,
and on restart or rebalance the library restores state by replaying it.

This demo is deliberately the *same job* as pattern 12: tumbling 5s page-view counts. There the
window bookkeeping, watermark and flush are ~60 lines of hand-written state; here they are one
topology declaration, and fault tolerance is not our code's problem anymore.

```
┌────────────────────┐      ┌─────────────────────────────────────┐
│ streamiz.pageviews │ ───▶ │  KafkaStream (Streamiz topology)    │
└────────────────────┘      │                                     │
                            │  stream(In)                         │
                            │    .GroupByKey()                    │
                            │    .WindowedBy(Tumbling 5s)         │
                            │    .Count(store)                    │
                            │    .ToStream().To(Out)              │
                            └───────┬───────────────────┬─────────┘
                                    │                   │ state store backed by
                                    ▼                   ▼ changelog topic
                   ┌────────────────────────────┐   ┌──────────────────────────────┐
                   │ streamiz.pageviews.counts  │   │ streamiz-windowed-counts-    │
                   └────────────────────────────┘   │ pageview-window-store-       │
                                                    │ changelog (auto-created)     │
                                                    └──────────────────────────────┘
```

Compare with pattern 12 line by line: `ApplicationId` plays the role of the consumer group,
the changelog topic replaces our "hope the process doesn't die" in-memory dictionary, and
repartitioning on `GroupBy` happens automatically. The trade: counts are emitted on the commit
interval (update stream), not once per closed window — suppression semantics differ from the
manual watermark flush.

**Version note.** Streamiz 1.8.x requires `Confluent.Kafka` ≥ 2.12, which would drag the client
upgrade across all eighteen demos. This project pins **Streamiz 1.6.0**, whose floor is 2.4.0 —
satisfied by the 2.5.3 already in use, so nothing else in the solution had to move.

**Lifecycle note.** `StartAsync` registers a callback on whatever `CancellationToken` you hand
it, and `Dispose()` does not reliably unregister it. Passing the host's stopping token means the
callback fires against disposed stream state at shutdown and takes the process down. Pass
`CancellationToken.None` and let `Dispose()` do the graceful stop it is designed for.

Use when: stateful stream processing (joins, windows, aggregations) where hand-rolling state
management stops being educational and starts being a liability.

## 15. ksqlDB

ksqlDB is SQL over Kafka topics, executed **server-side**: the ksqlDB server compiles each
statement into a Kafka Streams job it runs itself. The app talks to it over REST — here through
`ksqlDB.RestApi.Client`, which adds LINQ for push queries. Four moves make up the pattern:

```
            ┌──────────────────────── app (C#) ────────────────────────┐
            │                                                          │
            │  ① CREATE STREAM ride_requests  (DDL, schema-on-read)    │
            │  ② CREATE TABLE rides_per_city AS SELECT … GROUP BY city │
            │  ③ push query: SELECT … WHERE fare > 10 EMIT CHANGES     │
            │  ④ INSERT INTO ride_requests VALUES (…)                  │
            └───────────────┬──────────────────────────────────────────┘
                            │ REST :8088
                            ▼
                  ┌───────────────────┐  runs the compiled      ┌─────────────────┐
                  │   ksqldb-server   │  Kafka Streams jobs ──▶ │      Kafka      │
                  └───────────────────┘                         │  ksql.rides     │
                            │                                   │  rides_per_city │
              ②' table is continuously                          │  (changelog)    │
                 maintained server-side                         └─────────────────┘
                            │
                            ▼
              pull query: SELECT city, rides, totalFare FROM rides_per_city
```

The CSAS table (②) is the point: it is the same "consumer with a dictionary" as pattern 12 and
the same windowed store as pattern 13 — except no consumer of ours is running at all. The
aggregate is maintained by the server for as long as the statement exists.

Three things this demo had to get right, each of which fails quietly otherwise:

- **Push and pull go to different endpoints.** A `SELECT` sent through `ExecuteStatementAsync`
  (the `/ksql` DDL endpoint) is rejected with `error_code 40002`. Pull queries belong on
  `/query` — `context.CreatePullQuery<T>(...).GetManyAsync()`.
- **A CSAS table is eventually consistent.** `CREATE TABLE` returns when the query is
  *registered*, not when it has caught up, so a freshly created table answers pull queries with
  zero rows for a while. The demo polls until rows appear instead of sleeping a guessed interval.
- **Column names must survive ksqlDB's case folding.** Identifiers are folded to upper case, so
  `SUM(fare) AS total_fare` becomes `TOTAL_FARE` and never binds to a `TotalFare` property —
  it reads back as `0` with no error. Aliasing it `totalFare` (→ `TOTALFARE`) matches.

Requires `docker compose up -d ksqldb`; `KSQL_URL` overrides the endpoint. On Apple Silicon the
image is amd64-only and runs under emulation — it works, but it is slow to start. Worth knowing
for interviews: ksqlDB is in maintenance mode (Confluent is steering new SQL-on-Kafka work
toward Flink), but it remains widespread in existing stacks.

Use when: the transformation is expressible in SQL and you'd rather operate one ksqlDB server
than deploy a fleet of small stream-processing apps.


## CDC variants — one outbox, three relays

The same transactional outbox relayed three different ways, all DI-hosted over an EF Core
in-memory database, so the business row and its outbox row commit in one `SaveChangesAsync`.
All three share one outbox table, one write simulator (`CustomerWriteSimulator`) and one
read-model consumer (`SearchIndexerCdcConsumer`) — the relay is the only thing that varies.
These run until Ctrl+C; the fifteen above exit on their own.

| Command     | Relay                                | What's different                                                                                                              |
| ----------- | ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `cdc-state` | `StateMachineOutboxRelay`            | Per-row Pending/Processing/Completed/Failed plus a retry budget — a failed publish stays Pending until `MaxRetryCount`. Polls every 2s |
| `cdc-batch` | `BatchOutboxRelay`                   | Periodic job rather than a tight loop: wake every 5s, claim 20 rows, publish, report what the batch achieved. No per-row state |
| `cdc-poll`  | `PollingOutboxRelay`                 | The plainest one — poll every 500ms, publish, stamp. No state machine, no retry accounting                                    |

```
                     ┌────────────────────┐
                     │  EF Core outbox    │
                     │ (one SaveChanges   │
                     │  with the business │
                     │  row)              │
                     └────┬───────┬───────┬┘
        ┌─────────────────┘       │        └────────────────┐
        ▼                         ▼                         ▼
┌───────────────┐        ┌───────────────┐        ┌───────────────┐
│  cdc-state    │        │   cdc-batch   │        │   cdc-poll    │
│ StateMachine  │        │  BatchOutbox  │        │ PollingOutbox │
│ + retry budget│        │  every 5s     │        │ every 500ms   │
└───────┬───────┘        └───────┬───────┘        └───────┬───────┘
        └─────────────────────────┴─────────────────┬─────┘
                                                    ▼
                              ┌─────────────────────────────────┐
                              │  Kafka → SearchIndexerCdcConsumer│
                              │        → CustomerSearchIndex     │
                              └─────────────────────────────────┘
```

### Redis duplicate filter (optional)

The CDC pipeline keeps a duplicate filter in front of its durable ledger, through
`IDistributedCache`. With no configuration it uses the in-process provider, so nothing extra
needs to run:

```
dotnet run -- resilient                                    # in-process cache
docker compose up -d redis
REDIS_CONNECTION=localhost:6379 dotnet run -- resilient    # real Redis
```

Same code either way — `Redis:ConnectionString` in config, or the `REDIS_CONNECTION` env var, is
the only switch. The one behavioural difference is that an in-process cache is **not** shared
between instances, so two replicas each keep their own filter.

The cache is a fast path, never the guarantee: a Redis outage is logged and *stepped over*
(entries are read fail-open) because the ledger is what actually makes processing idempotent.
Treating an outage as "already processed" would silently drop live messages.

```
┌───────────────────┐     ┌───────────────────┐  hit          skip (duplicate)
│ incoming message  │ ──▶ │    Redis cache    │ ─────────▶
└───────────────────┘     │   seen before?    │
                          └────┬─────────┬────┘
                        miss   │         │ Redis down → FAIL-OPEN
                               ▼         ▼ (never "assume processed")
                          ┌───────────────────┐  known        skip (duplicate)
                          │  durable ledger   │ ─────────▶
                          │ (source of truth) │
                          └────────┬──────────┘
                                   │ new
                                   ▼
                          handle + record + mark cache
```

## Failure model: Result, not bool

A consumer needs **three** answers, not two — and *not committing is not a retry*: the
consumer's position has already advanced, so a genuine retry has to `Seek` back.

```
                       ┌─────────────┐
                       │  processing │
                       └──┬────┬───┬─┘
              success     │    │   │  transient failure
          ┌───────────────┘    │   └──────────────────┐
          ▼                    ▼                      ▼
   ┌────────────┐    ┌──────────────────┐    ┌──────────────────┐
   │  Handled   │    │      Poison      │    │    Transient     │
   │  commit ✓  │    │ dead-letter,     │    │ Seek back →      │
   └────────────┘    │ commit, move on  │    │ reprocess        │
                     └──────────────────┘    └──────────────────┘
```


## Production notes baked into the code

- `Acks=All` + `EnableIdempotence=true` on every producer
- Manual commits **after** successful processing (at-least-once)
- `IsolationLevel.ReadCommitted` where transactions are involved
- Idempotent topic creation via AdminClient before anything subscribes
- Keys chosen deliberately everywhere — the key IS the ordering/partitioning contract.
  The outbox relays key by **entity id**, so every change to one entity keeps one partition
- Failure is modelled with `Result`, not `bool`: handled (commit), poison (dead-letter and move
  on), transient (rewind and retry). Not committing is **not** a retry — a genuine retry has to
  `Seek` back
- Polymorphic payloads carry a `$type` discriminator resolved against an allow-list.
  Never `TypeNameHandling.All` on a payload that arrives off a topic

