# Changelog

## 0.1.0 (unreleased)

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
