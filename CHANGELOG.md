# Changelog

Versions follow the Node.js package in their first two numbers: Anarchon for .NET 0.7.x has the
algorithm and the stored data of `anarchon` 0.7.x, so their instances can share jobs. The last number
belongs to each package.

## 0.7.0 (2026-11-21)

The .NET counterpart of `anarchon` 0.7: the inside of the scheduler and of the backends is simpler
to read and to change, and three bugs are fixed. No breaking change: the public API only gains a
member, and the backend contract and the stored data are the same as 0.6.2, so 0.6.2 and 0.7.0
instances, and Node.js instances of 0.6.2 to 0.7.1, can share a store during a rolling deploy.

### Added

- **`Anarchon`**: `RunHistory.PendingStatus()`, the status of an attempt without a result on a
  backend with leases: Running only for the current attempt with a live lease. The Redis, SQL and
  NATS backends use it; a backend of your own can pass it to `RunHistory.Build()`.

### Fixed

- **`Anarchon`**: a start that fails while a stop waits for runs no longer sets the scheduler to
  idle. A later `StartAsync()` is refused with `AnarchonErrorCode.State` until the stop returns;
  before, it started the scheduler and the stop then stopped it without an error.
- **`Anarchon`**: a refused `Jitter` or `MaxLate` of a job names the parameter `options` in its
  `ArgumentOutOfRangeException` (was the internal name `input`).
- **`Anarchon.Nats`**: a `Prefix` that is not empty and does not end with a dot is refused with an
  `ArgumentException` when the backend is made. Before, start passed and every claim failed with
  no reply from the server. The Node.js package makes the same check from 0.7.1.

### Changed

- **`Anarchon`**: `Cron()`, `Every()` and `At()` check the job's name and settings before they check
  that the name is free, so a bad value in a call with a taken name gives an `ArgumentException`,
  not `AnarchonErrorCode.JobExists`.
- **`Anarchon.Sql`**: a claim refused as an overlap writes nothing and reports the earliest live
  run. Before, it could end older dead runs first, in the order the database returned the rows;
  those runs now end at the next claim that starts a run. The Node.js package 0.7.1 has the same
  fix.
- **Internal structure.** No change to what the backends store or answer:
  - `ISchedulerCore` names its lifecycle checks (`CanStart`, `CanWrite`), and the scheduler's parts
    read one `SchedulerSettings` record. `LeaseRunner` builds every lease claim in one place.
  - `Cron()`, `Every()`, `At()` and `Register()` add jobs through one path.
  - Each Redis claim strategy holds its own settings and job checks; `RedisBackend` no longer
    branches on the strategy.
  - The NATS backend builds every subject name in one place, and the lease rules no longer write
    the stored JSON field names.
  - The SQL claim rules are in `ClaimStep.Decide`, and `SqlTables` creates the tables and checks
    the store format.
- **Dependencies.** StackExchange.Redis 3.4.35 (was 3.4.31), Npgsql 10.0.4 (was 10.0.3),
  NATS.Client.KeyValueStore 3.4.1 (was 3.3.2), Microsoft.Data.SqlClient 7.1.2 (was 7.1.1), and
  Microsoft.Data.Sqlite and Microsoft.Extensions.* 10.0.15 (was 10.0.14).

## 0.6.2 (2026-11-10)

### Changed

- **`Anarchon.Redis`**: StackExchange.Redis 3.4.31 (was 3.4.29).

## 0.6.1 (2026-11-04)

### Added

- **`Anarchon.SqlServer`**: SQL Server 2016+ and Azure SQL on Microsoft.Data.SqlClient, with leases,
  backups, catch-up and no overlapping runs, `UseSqlServer()` for the host, `BACKEND=sqlserver`
  in the examples, and the `SqlServer` example (a worker service whose jobs write app tables in the
  same database, keyed by the run's slot). Claims of one job take turns on an application lock; job names compare
  case-sensitively. The Node.js package has no SQL Server backend.
- **`Anarchon.Sql`**: `SqlDialect` members for databases whose SQL differs more: `KeyText` and
  `Text` (column types), `LockHint` (a table hint instead of `FOR UPDATE`), `CreateTable`,
  `CreateIndex`, `InsertIfNew` and `Page`. Their defaults produce the SQL used before, so existing
  dialects and tables are unchanged.

## 0.6.0 (2026-11-02)

The first release of Anarchon for .NET: the scheduler and its backends, with the algorithm and the
stored data of the Node.js package 0.6, so .NET and Node.js instances can share jobs.

### Added

- **Documentation** at [aristoteliss.github.io/anarchon-docs](https://aristoteliss.github.io/anarchon-docs/), shared with the Node.js
  package: every code sample in C# and TypeScript, ASP.NET Core and worker service recipes, and a
  .NET reference. The NuGet packages link to it as their project URL.
- **`Anarchon`**: the `Scheduler` with interval, cron and one-off jobs (`Every()`, `Cron()`, `At()`),
  `RunNowAsync()`, history across instances, events, run data, and the lease runner: backup runners,
  retries, catch-up, no overlapping runs, hand-over on stop and `YieldAfterFailure`.
- **Attributes** `[Cron]`, `[Every]` and `[At]` for jobs declared on methods, added with
  `Register()` or, with a host, `AddJobs<T>()`, which runs each job in its own service scope.
  `Jitter = "0s"` and `CatchUp = "0s"` turn jitter and catch-up off, as `TimeSpan.Zero` does in code.
- **Hosting**: `AddAnarchon()` registers a scheduler that starts and stops with the host;
  `AddAnarchon(name)` registers more as keyed services.
- **The backend contract** (`IBackend`, `IRunStore`, `RunHistory.Build()`, `StoreFormat`) for
  backends in other packages, with `RunText` (the stored names of statuses and reasons) and
  `RunStoreExtensions` (the plain and the manual claim on a lease store).
- **`Anarchon.Redis`**: Redis and Valkey on StackExchange.Redis, with the `Lease` (default),
  `StreamId`, `Idmp` (Redis 8.6+) and `Script` strategies.
- **`Anarchon.Postgres`**: PostgreSQL 9.5+ on Npgsql, with leases, backups, catch-up and no
  overlapping runs; a connection string or your app's `NpgsqlDataSource`; `UsePostgres()`.
- **`Anarchon.Sqlite`**: SQLite 3.24+ on Microsoft.Data.Sqlite, with leases, backups, catch-up and
  no overlapping runs, for the processes of one machine that open the same file; `UseSqlite()`.
- **`Anarchon.RabbitMQ`**: RabbitMQ 3.9+ on RabbitMQ.Client, claim only: a gate queue per run that
  holds one message, with publisher confirms, and history in a stream queue; `UseRabbitMQ()`.
- **`Anarchon.Nats`**: NATS JetStream on NATS.Net: the `MsgId` strategy (claim only, the default)
  and the `Lease` strategy (nats-server 2.10+), whose key-value bucket adds backups, catch-up and no
  overlapping runs; `UseNats()`.
- **`Anarchon.Sql`**: the SQL lease store the SQL backends share, on ADO.NET: an abstract
  `SqlBackend` and a `SqlDialect`, for putting Anarchon on another database.
- **Examples** for every situation the Node.js package shows: three processes stopped and killed for
  real, failover, an ASP.NET Core API, a debugger walkthrough, every option and event, run data,
  attributes, your own connections, and real-life setups (billing, cache refresh, a resumable export,
  tenants, notifications on a broker). Each runs on any backend through `BACKEND`.
