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

**Contents:** [Run it](#run-it) · [Pattern catalog](#pattern-catalog)

## Run it

```bash
docker compose up -d kafka    
cd src/KafkaPatterns
dotnet run -- <pattern>       # dotnet run -- dlq
```

## Pattern catalog

| Command        | Pattern               | What the demo shows                                                                               |
| -------------- | --------------------- | ------------------------------------------------------------------------------------------------- |
| `pubsub`       | Publish / Subscribe   | One topic, two independent consumer groups — every group gets every message                       |
| `workqueue`    | Queue                 | One group, 3 workers, jobs split by partitions, commit-after-processing                           |
| `eventsource`  | Event Sourcing        | Append account events, rebuild balances by replaying from offset 0                                |
| `stream`       | Stream Processing     | Consume-transform-produce exactly-once using transactions                                         |
| `cdc`          | Change Data Capture   | Transactional-outbox relay: DB change + outbox row → Kafka                                        |
| `dlq`          | Dead Letter Queue     | Bounded retries with backoff, then park to `deadletter`                                           |
| `parentchild`  | Parent-Child Topics   | Derived topic keyed by the SAME key — traceability + ordering preserved                           |
| `reqreply`     | Request / Reply       | `correlation-id` + `reply-to` headers, client matches replies via `TaskCompletionSource`          |
| `competing`    | Competing Consumers   | One group competes for partitions; one consumer is killed mid-run → watch rebalance               |
| `partitioning` | Partitioning          | Default murmur2 key hashing vs a custom region-pinning partitioner                                |
| `saga`         | Saga                  | Order → Payment → Inventory choreography; inventory failure triggers refund compensation          |
| `windowing`    | Time Windowing        | Tumbling 5s windows over a click stream, flushed by watermark                                     |
| `cdc-state`    | State Machine Relay   | Per-row Pending/Processing/Completed/Failed plus a retry budget                                   |
| `cdc-batch`    | Batch Job Relay       | Periodic job: wake, claim batch, publish, report                                                  |
| `cdc-poll`     | Plain Polling Relay   | High-churn simple sweep: poll every 500ms, publish, stamp                                         |
| `cdc-rx`       | Rx Batch Consumer     | Batched consume with dual-tier Redis Idempotency and native DeadLetter publishing                 |

---

## 1. Publish / Subscribe
One topic, independent consumer groups. Both groups receive **every** message.
```text
                     ┌───────────────┴───────────────┐
                     ▼                               ▼
        ┌─────────────────────────┐    ┌──────────────────────────────┐
        │ group: analytics-service│    │ group: notification-service  │
        └─────────────────────────┘    └──────────────────────────────┘
```

## 2. Work Queue
One consumer group, three workers. Kafka assigns partitions to members, the offset is committed **after** the work is done.
```text
              ┌──────────┬──────┴─────┬──────────┐
              │ p0       │ p1         │ p2       │
              ▼          ▼            ▼
         ┌─────────┐ ┌─────────┐ ┌─────────┐
         │worker-1 │ │worker-2 │ │worker-3 │   commit offset
         └─────────┘ └─────────┘ └─────────┘   AFTER processing
```

## 3. Event Sourcing
The topic **is** the source of truth. A projector replays from offset 0 to rebuild completely.
```text
                           ┌──────────────────────┐      ┌───────────────────┐
                           │      Projector       │ ───▶ │ read model:       │
                           │  (fresh group.id)    │      │ ACC-1 → 430.00    │
                           └──────────────────────┘      └───────────────────┘
```

## 4. Stream Processing
Exactly-once processing using Kafka Transactions (`SendOffsetsToTransaction`).
```text
                    one Kafka transaction:            ┌────────────────────┐
                    output record + input offsets     │ sink               │
                    commit together, or not at all    │ (ReadCommitted)    │
                                                      └────────────────────┘
```

## 5. Change Data Capture (CDC Outbox)
Business row + outbox row commit in one DB transaction, assuring zero dual-write drops. 
```text
                                        │ poll unsent rows
                                        ▼
                               ┌──────────────────┐  publish   ┌────────────────┐
                               │   outbox relay   │ ─────────▶ │ cdc.customers  │
                               │ (stamp after ack)│            │ key = entity id│
                               └──────────────────┘            └────────────────┘
```

## 6. Dead Letter Queue
A failing record retries. When exhausted, park with diagnostic headers.
```text
                   ┌────────────────────────────────┐     ┌────────────────┐
                   │          deadletter            │ ──▶ │  dlq-monitor   │
                   │ attempts · x-failure-reason    │     │ alert / redrive│
                   └────────────────────────────────┘     └────────────────┘
```

## 7. Parent-Child Topics
Derived topics use the **same key**, maintaining strict partition isolation.
```text
┌─────────────────────┐      ┌────────────────────┐      ┌─────────────────────┐
│ user.events (parent)│ ───▶ │      Deriver       │ ───▶ │ user.profiles(child)│
│    key = UserId     │      │ fold events →      │      │    key = UserId     │
└─────────────────────┘      └────────────────────┘      └─────────────────────┘
```

## 8. Request / Reply
Async RPC over Kafka mapping `correlation-id`s to awaiting Tasks.
```text
┌────────┐  {corr-id, reply-to}  ┌──────────────────┐      ┌─────────────────┐
│ Client │ ────────────────────▶ │ pricing.requests │ ───▶ │ pricing-service │
└────────┘                       └──────────────────┘      └────────┬────────┘
    ▲                            ┌──────────────────────────┐       │ corr-id
    └─────────────────────────── │ pricing.replies.client-1 │ ◀─────┘ echoed
                                 └──────────────────────────┘
```

## 9. Competing Consumers
Members of one group compete. When one crashes, Kafka gracefully reassigned partitions.
```text
┌───────────────────┐        group: email-sender
│  emails.outgoing  │   ┌──────────┬──────────┬──────────┐
│   (4 partitions)  │──▶│ sender-1 │ sender-2 │ sender-3 │
└───────────────────┘   │ (p0,p1)  │  (p2)    │  (p3) ✗  │
                        └──────────┴──────────┴────┬─────┘
                                                   │ crash → rebalance
```

## 10. Partitioning Strategy
Pinning specific records (i.e. 'EU') exclusively to isolated partitions via custom Hashers.
```text
      key = "eu"   │   key = "us"      key = "apac"
      ▼            ▼            ▼                 ▼
┌───────────┐ ┌───────────┐ ┌────────────────────────┐
│ partition │ │ partition │ │  partitions 2..N       │
│ 0 (EU)    │ │ 1 (US)    │ │  (hashed spread)       │
└───────────┘ └───────────┘ └────────────────────────┘
```

## 11. Saga
Choreography across decoupled states. If sub-states fail, compensating events undo the operation.
```text
        │ order.cancelled                │ inventory.failed                    │
        │ (refund issued)  ◀── COMPENSATE┴──────────────────── out of stock ───┘
```

## 12. Time Windowing
Tumbling windows manually flushed via watermarks based precisely on Time parameters, never processing speed. 
```text
┌────────────────┐      ┌───────────────────────────┐      ┌───────────────────────────┐
│ site.pageviews │ ───▶ │         Windower          │ ───▶ │ site.pageviews.per-window │
└────────────────┘      │ flush when watermark      │      └────────────┬──────────────┘
                        │ passes window end         │                   ▼
                        └───────────────────────────┘   [12:00:05–12:00:10] /home: 7 views
```

## 13. Resilient Consumer (Rx)
A generic, reusable consumer (`KafkaConsumerRx`) separating mechanics of consuming from business logic.

```text
        ┌──────────────────────┐
        │    cdc.customers     │   
        └──────────┬───────────┘
                   │ ConsumeBatch (5s window)
                   ▼
        ┌──────────────────────┐        ┌────────────────────────────────┐
        │   KafkaConsumerRx    │ ─────▶ │ PacketShardMessageProcessor    │
        │  (domain-agnostic)   │        │ Redis fast path ─▶ DB ledger   │
        └──────────┬───────────┘        └───────────────┬────────────────┘
                   │                                    │ bool?
                   ▼                                    ▼
   ┌────────────────────────────────────────────────────────────────────┐
   │ true  (Handled)   → Commit offset and move forward                 │
   │ null  (Transient) → Seek partition back to retry the offset later  │
   │ false (Poison)    → Route to Retry/DLQ topics, then commit         │
   └────────────────────────────────────────────────────────────────────┘
```

- **Skipping a commit isn't a retry:** If processing fails temporarily (`null`), the consumer explicitly calls `Seek` to rewind the partition.
- **Dead Letter Routing:** Persistent failures jump to DLQ topics with `attempts` headers.
- **Two-Tier Idempotency:** Fast-path cache lookup in front of a guaranteed DB immutable ledger.

## 14. Kafka Streams (Streamiz)

Kafka Streams is JVM-only; the community-standard .NET port is **Streamiz.Kafka.Net** — same
DSL, same mechanics: state lives in a local store, the store is backed by a **changelog topic**,
and on restart or rebalance the library restores state by replaying it.

This demo is deliberately the *same job* as pattern 12: tumbling 5s page-view counts. There the
window bookkeeping, watermark and flush are ~60 lines of hand-written state; here they are one
topology declaration, and fault tolerance is not our code's problem anymore.

```text
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

Use when: stateful stream processing (joins, windows, aggregations) where hand-rolling state
management stops being educational and starts being a liability.

## 15. ksqlDB

ksqlDB is SQL over Kafka topics, executed **server-side**: the ksqlDB server compiles each
statement into a Kafka Streams job it runs itself. The app talks to it over REST — here through
`ksqlDB.RestApi.Client`, which adds LINQ for push queries. Four moves make up the pattern:

```text
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

Requires `docker compose up -d ksqldb`; `KSQL_URL` overrides the endpoint. 

Use when: the transformation is expressible in SQL and you'd rather operate one ksqlDB server
than deploy a fleet of small stream-processing apps.

## CDC Variants

One outbox relay pipeline, simulated 4 uniquely modeled `.NET HostedService` ways:

| Command        | Relay Strategy                    | Mechanics                                                                                             |
| -------------- | --------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `cdc-state`    | `StateMachineOutboxRelay`         | Per-row Pending/Processing/Completed/Failed plus a retry budget. Failed publish stays Pending until max retry counts exhaust. |
| `cdc-batch`    | `BatchOutboxRelay`                | Periodic job: wakes up, pulls a set batch utilizing EF Core, pushes to broker, flags bulk as processed. |
| `cdc-poll`     | `PollingOutboxRelay`              | Constant sweeping without complex state mechanics. Scans unsent records every 500ms and ships them out. |
| `cdc-rx`       | `Rx Batch Consumer`               | Full architectural reactive extension lifecycle handling Idempotency natively alongside DLQ mapping. (See #13). |

## Production Best Practices

- `Acks=All` + `EnableIdempotence=true` heavily enforced.
- **Manual commits** after pipeline operations; never before.
- Isolation level `ReadCommitted` handles transaction boundaries seamlessly.
- Error behaviors mapped strictly (`Result` models over simple exception handling). Not committing does **not** signify a downstream broker retry. Real retrying dictates manual partition `Seek()` adjustments natively executed in the Rx consumer setup.
