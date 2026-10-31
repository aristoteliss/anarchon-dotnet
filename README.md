# Anarchon for .NET

[![CI](https://github.com/aristoteliss/anarchon-dotnet/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/aristoteliss/anarchon-dotnet/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-AGPL--3.0%20or%20commercial-blue.svg)](#license-and-commercial-use)

[**Documentation**](https://aristoteliss.github.io/anarchon-docs/) · [Quick start](https://aristoteliss.github.io/anarchon-docs/getting-started/quick-start/) · [ASP.NET Core](https://aristoteliss.github.io/anarchon-docs/recipes/aspnet-core/) · [.NET reference](https://aristoteliss.github.io/anarchon-docs/reference/dotnet/scheduler/) · [Guarantees and limits](https://aristoteliss.github.io/anarchon-docs/reference/guarantees/)

Leaderless scheduling for .NET services, coordinated through the datastore you already run. Run the same jobs in every instance; an atomic claim in the store selects one runner for each scheduled slot. On Redis, Valkey, PostgreSQL, SQLite and NATS (`Lease`) a lease lets another instance retry a run whose runner stopped renewing it; RabbitMQ and NATS (`MsgId`) only pick the runner. That gives at-least-once execution after failures, not exactly-once side effects: handlers should be idempotent, and a frozen runner can briefly overlap its replacement.

Anarchon also handles what goes wrong in real clusters:
- jobs that run longer than their interval
- an instance crashing or freezing mid-run
- rolling deploys, and every instance restarting at once

```csharp
using Anarchon;
using Anarchon.Redis;

await using var scheduler = new Scheduler(new SchedulerOptions { Backend = new RedisBackend("localhost:6379") }); // a backend is required: none is assumed

scheduler
    .Every("sync", TimeSpan.FromSeconds(10), ctx => SyncAsync(ctx.CancellationToken))
    .Every("report", TimeSpan.FromMinutes(35), ctx => ReportAsync(ctx.CancellationToken), new() { Timeout = TimeSpan.FromMinutes(20) })
    .Cron("digest", "0 */2 * * *", ctx => DigestAsync(ctx.CancellationToken), new() { TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Athens") })
    .At("migrate", DateTimeOffset.Parse("2026-12-01T03:00:00Z"), ctx => MigrateAsync(ctx.CancellationToken)); // a one-off job: one slot

await scheduler.StartAsync();
```

Put the same code in every instance.

This is the .NET counterpart of the [Node.js package](https://github.com/aristoteliss/anarchon): the same algorithm and the same stored data, with a .NET API. See [Running next to the Node.js package](#running-next-to-the-nodejs-package).

## Install

Not published to NuGet yet. Once it is:

```sh
dotnet add package Anarchon.Redis      # Redis or Valkey; brings in Anarchon, the core
dotnet add package Anarchon.Postgres   # or PostgreSQL
dotnet add package Anarchon.Sqlite     # or a SQLite file, for the processes of one machine
dotnet add package Anarchon.RabbitMQ   # or RabbitMQ: claim only, no leases
dotnet add package Anarchon.Nats       # or NATS JetStream: claim only, or leases with NatsStrategy.Lease
```

Targets .NET 10. Each backend is its own package with its own driver; see [Backends](#backends).

## In an app with a host

In a worker service or an ASP.NET Core app, jobs are methods marked with `[Cron]`, `[Every]` or `[At]`. Each run gets its own service scope, so a job class can take scoped services such as a `DbContext`. The host starts the scheduler, and stops it on shutdown.

```csharp
builder.Services
    .AddAnarchon(options => options.TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Athens"))
    .UseRedis(builder.Configuration.GetConnectionString("redis")!)
    .AddJobs<ReportJobs>();

public sealed class ReportJobs(ReportsDbContext db)
{
    [Cron("0 6 * * *", Name = "daily-report", Timeout = "20m")]
    public Task DailyAsync(JobContext ctx) => db.BuildDailyAsync(ctx.ScheduledAt, ctx.CancellationToken);

    [Every("10s", Name = "sync-customers", Timeout = "8s")]
    public Task SyncAsync(CancellationToken cancellationToken) => db.SyncCustomersAsync(cancellationToken);
}
```

A job method returns `Task`, `ValueTask` or nothing, and takes any of `JobContext` and `CancellationToken`. A job is named `Type-Method` unless the attribute gives a `Name`. Attribute durations are text such as `"30s"` or `"1h30m"` (`Duration.Parse`).

`AddAnarchon()` registers `Scheduler` as a singleton. `AddAnarchon("billing")` registers another one as a keyed service (`[FromKeyedServices("billing")] Scheduler`), for jobs that run on another store.

## How it works

1. **Every instance computes the same run times.** Each scheduled run has a time, called its *slot*. Intervals use a grid counted from 1970 UTC, so "every 10 seconds" means :00, :10, :20…. Cron jobs use the expression. All instances agree on the slots, whenever each one started.
2. **Each instance waits a random delay.** It waits until the slot plus a random 0 to `Jitter` (default 2 s). The delay is what spreads the work across instances.
3. **One atomic claim decides.** Each instance sends the same claim, which records the run unless it's already there: a Lua script in Redis, a short transaction in PostgreSQL and SQLite, a write at a known revision in a NATS key-value bucket. The store decides claims of a run one at a time, so exactly one instance wins.
4. **The winner runs the job under a lease.** It renews the lease every few seconds, and the other instances become its backups.

```
06:00:10.300  B: claim slot 06:00:10 → won      → B runs the job, renewing its lease
06:00:11.400  A: claim slot 06:00:10 → running  → skip, and back B up
06:00:11.900  C: claim slot 06:00:10 → running  → skip, and back B up
```

### How the lease works

- **Claiming.** A claim stores the run with a lease that ends `Lease` (default 30 s) from now, measured on the store's clock (the Redis server's, the database's), so clock differences between instances don't matter. The runner renews the lease every `Heartbeat` (default 5 s).
- **Backups.** Instances that saw the run taken check on it every 1 to 2 heartbeats. If the lease ran out before the run finished, they all try to claim the next attempt, and only one wins. A crashed run is taken over about one lease after the crash. It is started up to `MaxAttempts` times in all (default 3), and only while it's still the latest run of its job.
- **Graceful stop.** On `StopAsync()`, a job that throws once its `CancellationToken` is cancelled gives its run up. A backup takes it over at its next check (within 2 heartbeats), and this doesn't count toward `MaxAttempts`. A job that returns normally counts as finished. A job still running when `StopTimeout` passes stops renewing its lease, so a backup takes it over once the lease runs out.
- **No overlap.** A new run isn't started while an earlier run of the same job still holds a live lease. Set `Overlap = OverlapPolicy.Allow` to run them side by side.
- **Fencing.** Every attempt has its own token. Heartbeats and "finished" only work with the current token, so a runner that was taken over finds out at its next heartbeat: its token is cancelled and the scheduler raises `Lost`.
- **Catch-up.** On start, each instance first picks up unfinished runs whose runner is gone. Then, if the latest run was never claimed and was due less than `CatchUp` ago (default 5 min), it runs it now with reason `CatchUp`.
- **Late claims.** The store checks how late a new claim is on its own clock, so a claim that was delayed or queued can't start a stale run.

A lease isn't a lock you hold and release. Every decision is still a single atomic claim, and the lease only says "the runner is still alive". A frozen runner can't know it was taken over until it wakes up, so two attempts can briefly overlap in that case. Check the cancellation token before side effects, and keep jobs idempotent.

Anarchon is not a queue. A run can be skipped: the previous one is still going, an instance is too late, or every instance was down for longer than `CatchUp`. If the work must get done, have each run work on everything outstanding, not only on its own slot's window.

## API

### Scheduler

| Member | What it does |
|---|---|
| `new Scheduler(SchedulerOptions)` | Creates a scheduler. Nothing connects until `StartAsync()`. |
| `Every(name, TimeSpan, handler, EveryJobOptions?)` | An interval job on the UTC grid; at least 1 second. `Offset` shifts the grid. |
| `Cron(name, expression, handler, CronJobOptions?)` | A cron job: 5 fields, or 6 with seconds first. `TimeZone` follows daylight saving. |
| `At(name, DateTimeOffset, handler, JobOptions?)` | A one-off job. |
| `Register(object, RegisterOptions?)` | Adds the jobs declared with attributes on an object, all or none. |
| `Remove(name)` | Stops scheduling a job on this instance. |
| `StartAsync()` / `StopAsync()` | Starts; stops, handing running jobs over and closing what the backend opened. |
| `RunNowAsync(name, data?)` | Runs a job here, now. Recorded as manual; not deduplicated. |
| `HistoryAsync(name, limit)` / `LastRunAsync(name)` | Runs from every instance, newest first, one record per attempt. |
| `GetNextRun(name)` / `GetJobs()` | The next run, and the registered jobs with their schedule. |
| `SetData(object?)` | Replaces the data every run gets. |

The handler is `Func<JobContext, Task>`. `JobContext` has the job's `Name`, the `Slot` (Unix ms, the same on every instance: use it as an idempotency key), `ScheduledAt`, `Attempt`, `Reason`, `IsManual`, `Data` and the run's `CancellationToken`.

### Options

| Option | Default | |
|---|---|---|
| `Backend` | (required) | Where runs are claimed and recorded. |
| `InstanceId` | `machine:pid` | This instance's name in history and events. |
| `Jitter` | 2 s | Random delay before claiming; capped at a quarter of short intervals. |
| `MaxLate` | 60 s | A run reached later than this is skipped as `Late`. |
| `HistorySize` / `HistoryTtl` | 1000 / none | How much history each job keeps. |
| `TimeZone` | the server's zone | Default zone of cron jobs. |
| `Lease` / `Heartbeat` | 30 s / 5 s | Lease backends only. |
| `StopTimeout` | 30 s | How long `StopAsync()` waits for running jobs. |
| `Data` | none | Data every run gets; a job's own data goes on top. |
| `LoggerFactory`, `TimeProvider` | none, system clock | Set from the services with `AddAnarchon()`. |

Every job option falls back to the scheduler's: `Jitter`, `MaxLate`, `HistorySize`, `HistoryTtl`, `Data`, `Timeout`, and on lease backends `Overlap`, `MaxAttempts` (default 3), `CatchUp` (default 5 min) and `YieldAfterFailure`. A lease option on a backend without leases is refused with `AnarchonErrorCode.Unsupported`, never ignored.

### Events

Events describe this instance; `HistoryAsync()` is the view across all instances. A handler that throws is logged and doesn't affect the run.

| Event | When |
|---|---|
| `Started` | This instance won a run and starts the handler. |
| `Succeeded` | The handler returned. |
| `Failed` | The handler threw, or its `Timeout` passed. |
| `Interrupted` | Stop or a takeover cancelled the token, and the handler then threw. |
| `Skipped` | A run was due and this instance didn't run it: `Taken`, `Late` or `Overlap`. |
| `Lost` | A backup took over a run this instance was executing. |
| `Error` | A backend problem; never a job failure. Without a handler it is logged. |

The scheduler logs under the category `Anarchon.Scheduler`: each run at Debug, failed and lost runs at Warning, problems nobody handles at Error.

### Errors

A value that can't be used throws `ArgumentException` or `ArgumentOutOfRangeException`. Everything else Anarchon refuses is an `AnarchonException` with a stable `Code`: `Unsupported`, `UnsafeJob`, `Server`, `State`, `JobExists`, `UnknownJob`, `Store` or `StoreFormat`.

## Backends

Redis, Valkey, PostgreSQL, SQLite and NATS with `NatsStrategy.Lease` get everything described above: leases, backups, no overlap, catch-up and hand-over on `StopAsync()`. RabbitMQ, and NATS with its default `MsgId` strategy, only make sure one instance wins each run, like the simple `StreamId` claim: they can't hold a lease that a backup could check.

```csharp
new Scheduler(new SchedulerOptions { Backend = new RedisBackend("localhost:6379") });
new Scheduler(new SchedulerOptions { Backend = new PostgresBackend("Host=db;Database=app;Username=app;Password=...") });
new Scheduler(new SchedulerOptions { Backend = new SqliteBackend("Data Source=/var/lib/myapp/jobs.db") });
new Scheduler(new SchedulerOptions { Backend = new RabbitMQBackend("amqp://rabbit.internal") });
new Scheduler(new SchedulerOptions { Backend = new NatsBackend("nats://nats.internal:4222") });
new Scheduler(new SchedulerOptions { Backend = new NatsBackend(new NatsBackendOptions { ConnectionString = "nats://nats.internal:4222", Strategy = NatsStrategy.Lease }) });
```

| Package | How one instance wins | Leases, backups, catch-up | Needs |
|---|---|---|---|
| `Anarchon.Redis` | One Lua script per claim (see [Redis strategies](#redis-strategies)) | Yes (`Lease`, the default) | Redis 5+ or Valkey; StackExchange.Redis |
| `Anarchon.Postgres` | One short transaction per claim. Its first statement writes the job's row in `<table>_jobs`, so claims of one job take turns | Yes | PostgreSQL 9.5+; Npgsql |
| `Anarchon.Sqlite` | One `BEGIN IMMEDIATE` transaction per claim; SQLite has one writer at a time | Yes | SQLite 3.24+ (bundled with Microsoft.Data.Sqlite) |
| `Anarchon.RabbitMQ` | A gate queue per run that holds one message (`x-max-length: 1`, `reject-publish`); publisher confirms say who was first | No | RabbitMQ 3.9+; RabbitMQ.Client |
| `Anarchon.Nats` (`MsgId`, the default) | JetStream publish with `Nats-Msg-Id: <job>:<slot>`; repeats come back as duplicates | No | JetStream enabled; NATS.Net |
| `Anarchon.Nats` (`Lease`) | A write to the job's key in the key-value bucket `<stream>_RUNS`, only at the revision that was read; a refused write is decided again | Yes | nats-server 2.10+ with JetStream; NATS.Net |

With a host, `UseRedis(...)`, `UsePostgres(...)`, `UseSqlite(...)`, `UseRabbitMQ(...)` or `UseNats(...)` picks the backend: a connection string, a lambda that sets the options, or one that also gets the app's services, to share a connection or data source the app registered:

```csharp
builder.Services.AddNpgsqlDataSource(builder.Configuration.GetConnectionString("postgres")!); // Npgsql.DependencyInjection
builder.Services.AddAnarchon().UsePostgres((services, options) => options.DataSource = services.GetRequiredService<NpgsqlDataSource>());
```

### PostgreSQL

`PostgresBackendOptions` takes an Npgsql `ConnectionString` (the backend opens a pool and closes it on stop), or a `DataSource` your app already has (never disposed by Anarchon), and a `Table` (default `anarchon_runs`, may include a schema that exists: `jobs.cron_runs`).

On start the backend creates four tables, if they don't exist: `anarchon_runs` (one row per run: attempt, token, status, lease end), `anarchon_runs_attempts` (one row per attempt: the history), `anarchon_runs_jobs` (one row per job, which claims of that job lock in turn) and `anarchon_runs_meta` (the store format). Services sharing a database need different table names. Leases use the database's clock.

### SQLite

`SqliteBackendOptions` takes a Microsoft.Data.Sqlite `ConnectionString` that names a file (default `Data Source=anarchon.db`) and a `Table` (default `anarchon_runs`, no schema). An in-memory or temporary database is refused: it is private to one connection. A write waits for another process's write lock up to the connection string's `Default Timeout` (30 s unless set).

On start the backend creates `anarchon_runs`, `anarchon_runs_attempts` and `anarchon_runs_meta`, and switches the file to write-ahead logging, so readers don't wait for the writer. Stop closes the backend's pooled connections to the file. Leases use SQLite's clock.

A SQLite file only coordinates the processes that open that same file: one machine, or a volume with working file locks (not NFS). For instances on several machines, use PostgreSQL or Redis.

### RabbitMQ

`RabbitMQBackendOptions` takes an AMQP URI as `ConnectionString` (default `amqp://localhost`; the backend opens the connection and closes it on stop) or a `Connection` your app already has (never closed by Anarchon), a `Prefix` (default `cron.`) and a `SlotTtl` (default 5 minutes).

- **Queues.** Each run has a gate queue `<prefix><job>.slot.<slot>` that deletes itself `SlotTtl` after its last use. Each job's history is a stream queue `<prefix><job>.history`, sized from the job's `HistorySize`.
- **Limits.** RabbitMQ remembers a run for `SlotTtl`, so a job's `Jitter` plus `MaxLate` must be shorter; a job that breaks this is refused with `UnsafeJob`. Job names may only use letters, digits, `_` and `-`, since they become queue names. History is kept by count only, so `HistoryTtl` is refused with `Unsupported`.
- **Failures.** A claim whose answer is lost (the connection dropped mid-publish) fails, so that instance skips the run rather than risk running it twice. A connection or channel that closed is opened again on the next call. The backend's own connection is named `anarchon:<prefix>` in the management UI.

### NATS

`NatsBackendOptions` takes a NATS URL (or several, comma-separated) as `ConnectionString` (default `nats://localhost:4222`; the backend opens the connection and closes it on stop) or a `Connection` your app already has (`INatsConnection`, never closed by Anarchon), the `Strategy` (`MsgId` by default, or `Lease`), a `Stream` (default `CRON`, created on start), a `Prefix` for subjects (default `cron.`), a `DuplicateWindow` (default 5 minutes), a `HistorySize` and `Replicas`.

- **History.** Claims and results go to `<prefix><job>.claims` and `<prefix><job>.results` in the stream, which keeps the largest `HistorySize` of the backend and its jobs, per subject. `HistoryTtl` is refused with `Unsupported`.
- **`MsgId`.** JetStream remembers a claimed run for `DuplicateWindow`, so a job's `Jitter` plus `MaxLate` must be shorter (`UnsafeJob` otherwise).
- **`Lease`.** Each job's runs are one key in the key-value bucket `<stream>_RUNS`; the stream name may then only use letters, digits, `_` and `-`. Leases use the NATS server's clock (nats-server 2.10+). A run is remembered while a late claim could still ask for it, as with the other lease backends.
- **Names.** Job names may only use letters, digits, `_` and `-`, since they become subjects.
- **Clusters.** Set `Replicas = 3` so the stream, and its memory of claimed runs, survives the loss of a server. A new bucket of runs gets the same number.

### The SQL lease store

The SQL lease store is in the package `Anarchon.Sql` (an abstract `SqlBackend` and a `SqlDialect`), which `Anarchon.Postgres` and `Anarchon.Sqlite` bring in. Reference it directly only to put Anarchon on another database.

### Redis strategies

`RedisStrategy.Lease` is the default and the only strategy with backups, no-overlap, retries and catch-up. The simple strategies only decide who runs each slot:

| Strategy | Claim | Why only one wins | Needs |
|---|---|---|---|
| `Lease` (default) | Lua script on `{cron:<job>}:state` | The script runs atomically | Redis 5+ or Valkey |
| `StreamId` | `XADD cron:<job>:runs <slot>-0 …` | A new stream ID must be bigger than the last one | Redis 5+ or Valkey |
| `Idmp` | `XADD … IDMP <job> <slot> * …` | Redis keeps the first entry with that idempotency key | Redis 8.6+ |
| `Script` | Lua: look for the slot in the stream, else `XADD` | The script runs atomically | Redis 5+ or Valkey |

With `Lease`, each job has these keys, all with one hash tag so Redis Cluster keeps them together: `{cron:<job>}:state`, `:active`, `:slots`, `:runs` (one entry per attempt) and `:results` (one result per attempt).

`RedisBackendOptions` takes a StackExchange.Redis `Configuration` string (the backend opens and closes the connection), or a `Connection` your app already has (never closed by Anarchon), a `Prefix` (default `cron:`) and a `Database`.

## Running next to the Node.js package

Each backend writes the same data as the Node.js package (the same Redis keys and Lua scripts, the same SQL tables and columns in PostgreSQL and SQLite, the same RabbitMQ queues, NATS subjects and bucket keys, and the same records), so .NET and Node.js instances can share jobs during a migration, or for good. With the same `Prefix` or `Table`, strategy and job names, each run still has one runner, either side can take over a run the other one left, and `HistoryAsync()` shows the runs of both. Interval jobs compute the same slots on both sides. Cron jobs use Cronos here and croner in Node.js; check unusual expressions on both before you mix them.

Coming from the Node.js API:

| Node.js | .NET |
|---|---|
| `every('sync', '10s', handler)`, values in ms | `Every("sync", TimeSpan.FromSeconds(10), handler)`, `TimeSpan` values |
| `ctx.signal` | `ctx.CancellationToken` |
| `@Cron`, `@Every`, `@At` decorators | `[Cron]`, `[Every]`, `[At]` attributes |
| `Scheduler.configure()`, `autoStart`, named schedulers | `AddAnarchon()`, the host, keyed services |
| `scheduler.on('failure', ...)` | `scheduler.Failed += ...` |
| `log: true`, `logger` | `ILoggerFactory`, filtered by category |
| error codes `ANARCHON_*` | `AnarchonException.Code` |

## Guarantees and limits

- **One winner per attempt.** For one slot, only one instance wins each attempt. A slot gets another attempt only after its runner stopped renewing its lease.
- **At-least-once for crashed runs, by design.** A run whose runner died is started again. Keep jobs idempotent, and check the cancellation token.
- **A hung job blocks its job.** With `OverlapPolicy.Skip`, a hung job keeps renewing its lease and blocks later runs. Set `Timeout`.
- **Keep clocks in sync (NTP).** Run times come from each instance's clock; leases use the store's clock.
- **`Every()` runs on a UTC grid.** For wall-clock times in a time zone, use `Cron()` with `TimeZone`.

## Documentation

**Read the [hosted documentation](https://aristoteliss.github.io/anarchon-docs/)**: guides to every option, with each code sample in C# and
in TypeScript, the claim and lease algorithms with diagrams, one page per backend, recipes
([ASP.NET Core](https://aristoteliss.github.io/anarchon-docs/recipes/aspnet-core/), [worker service](https://aristoteliss.github.io/anarchon-docs/recipes/worker-service/),
Kubernetes, long-running jobs) and a [.NET reference](https://aristoteliss.github.io/anarchon-docs/reference/dotnet/scheduler/). It covers
this package and the Node.js one.

The documentation has its own repository, [anarchon-docs](https://github.com/aristoteliss/anarchon-docs),
checked out here at `docs/` as a git submodule. To read or change it locally (it needs Node.js and pnpm):

```sh
git submodule update --init   # once, or clone with --recursive
cd docs
pnpm install
pnpm dev                      # http://localhost:4321
```

## Examples

Every example runs on a SQLite file in the temp folder unless `BACKEND` names another store (`redis`, `postgres`, `sqlite`, `nats`, `rabbitmq`; see `examples/Shared/Example.cs` for their variables), so most need no server. `.vscode/launch.json` has a debug configuration for each; start with "Example: Walkthrough", which stops before `StartAsync()` and lists where to step.

```sh
dotnet run --project examples/QuickStart        # 3 processes: a graceful stop, a crash (Process.Kill), then downtime and catch-up
dotnet run --project examples/Basic             # run it in 3 terminals, then Ctrl+C the one running "report"
dotnet run --project examples/Hosting           # a worker service: attribute jobs, dependency injection, Redis from configuration
dotnet run --project examples/HttpApi           # ASP.NET Core: GET /jobs, /jobs/{name}/runs, POST /jobs/{name}/run (HttpApi.http has every request)
dotnet run --project examples/Walkthrough       # one run from the timer to history, made for stepping through in a debugger
dotnet run --project examples/Failover          # 3 instances in one process: a graceful hand-over, then a crash and a takeover
dotnet run --project examples/OptionsAndEvents  # every job option and every event
dotnet run --project examples/RunData           # run data: the scheduler's, a job's, a provider read at each run, RunNowAsync and SetData
dotnet run --project examples/Attributes        # [Cron], [Every] and [At] without a host: Register with a prefix, a time zone and defaults
dotnet run --project examples/OwnClients        # your own IConnectionMultiplexer, or NpgsqlDataSource with BACKEND=postgres
```

Real-life setups, each a situation solved with a different set of options (the header of each file explains every choice):

```sh
dotnet run --project examples/Billing              # charge once a day: never twice (MaxAttempts 1), caught up after an outage (CatchUp)
dotnet run --project examples/CacheRefresh         # a cheap refresh every 3 s on 3 instances: jitter, a short MaxLate, no catch-up
dotnet run --project examples/NightlyExport        # a long export interrupted by a deploy, resumed from a checkpoint by another instance
dotnet run --project examples/Tenants              # per-tenant jobs and time zones, a fresh token per run, tenants added and removed live
BACKEND=nats STRATEGY=msg-id dotnet run --project examples/BrokerNotifications   # NATS or RabbitMQ, no leases: each run sends everything outstanding
```

`Hosting` and `OwnClients` need Redis (or Postgres for `OwnClients`), and `BrokerNotifications` a broker; the others need nothing.

## Tests

The tests run against real servers, and a server that isn't running is skipped. `test/docker-compose.test.yml` starts them on ports that leave a Redis on 6379 or a Postgres on 5432 alone:

```sh
docker compose -f test/docker-compose.test.yml up -d --wait   # Redis 8.6, Redis 7.0, Valkey, PostgreSQL 16
docker compose -f test/docker-compose.test.yml --profile brokers up -d --wait   # also RabbitMQ and NATS
dotnet test                                                    # every test
docker compose -f test/docker-compose.test.yml --profile brokers down   # remove them and their data
```

| Server | Default | Variable |
|---|---|---|
| Redis 8.6+ | `localhost:6381` | `REDIS_URL` |
| Redis 7.0 | `localhost:6380` | `REDIS7_URL` |
| Valkey | `localhost:6390` | `VALKEY_URL` |
| PostgreSQL 16 | `Host=localhost;Port=5433;Username=postgres;Password=postgres;Database=anarchon_test` | `POSTGRES_URL` (an Npgsql connection string) |
| SQLite (no server: one file per test) | the temporary folder | `SQLITE_DIR` |
| RabbitMQ 3.13 (profile `brokers`) | `amqp://localhost:5672`, management on `http://localhost:15672` | `RABBITMQ_URL`, `RABBITMQ_MANAGEMENT_URL` |
| NATS 2.11 with JetStream (profile `brokers`) | `nats://localhost:4222` | `NATS_URL` |

To make a run fail when a server is missing, instead of skipping its tests, name the servers in `REQUIRE_SERVERS`: `REQUIRE_SERVERS=redis,redis-7,valkey,postgres,rabbitmq,nats dotnet test`. CI sets it; RabbitMQ and NATS run in a CI job of their own.

This covers, for every backend and each Redis strategy and server: a race where three clients fight over 40 runs, retry-safety and history. For every backend with leases: the lease state machine step by step, and schedulers racing on one store, with hand-over on stop, takeover after a lost lease or a failed hand-over, heartbeats keeping a long run, `YieldAfterFailure`, overlap backups and catch-up. These shared tests live in `test/Anarchon.Specification`, and each backend's test project inherits them. `test/Anarchon.Sql.Tests` and `test/Anarchon.Sqlite.Tests` run them on SQLite files, so they need no server.

Coverage: `dotnet test -- --coverage --coverage-output-format cobertura` writes a Cobertura report per test project.

## Credits

The claim-by-unique-record design comes from Meteor's SyncedCron, as do one-off jobs, run logging and the history time-to-live.

## License and Commercial Use

This software is **Dual-Licensed**.

The project offers the **GNU AGPLv3** (see [`LICENSE`](LICENSE)) and a separate commercial license (see [`COMMERCIAL_LICENSE.txt`](COMMERCIAL_LICENSE.txt)). The AGPLv3 is a copyleft license; review its terms to determine how they apply to your use. The commercial license allows commercial integration without AGPL copyleft obligations and sets an annual fee based on the licensee's gross global revenue. Review the full terms and fee schedule in the linked files before adopting either option.
