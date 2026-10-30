// Copyright (C) 2026-present Aristotelis — see repository license.

// Using connections your app already has. The scheduler never closes a connection you pass in, so
// StopAsync() leaves it open for the rest of the app.
//
//   dotnet run --project examples/OwnClients                                     # your IConnectionMultiplexer (Redis)
//   BACKEND=postgres POSTGRES_URL=... dotnet run --project examples/OwnClients   # your NpgsqlDataSource
//
// Also accepted: a RabbitMQ IConnection (RabbitMQBackendOptions.Connection) and a NATS
// INatsConnection (NatsBackendOptions.Connection). With a host, register the connection as a service
// and hand it over in UseRedis((services, options) => ...): see examples/Hosting.

using Anarchon;
using Anarchon.Backends;
using Anarchon.Postgres;
using Anarchon.Redis;
using Npgsql;
using StackExchange.Redis;
using static Anarchon.Examples.Example;

var usePostgres = Environment.GetEnvironmentVariable("BACKEND") == "postgres";
await using var redis = usePostgres ? null : await ConnectionMultiplexer.ConnectAsync(Environment.GetEnvironmentVariable("REDIS_URL") ?? "localhost:6379");
await using var dataSource = usePostgres ? NpgsqlDataSource.Create(Environment.GetEnvironmentVariable("POSTGRES_URL") ?? "Host=localhost;Database=postgres") : null;

IBackend backend = dataSource is not null
    ? new PostgresBackend(new PostgresBackendOptions { DataSource = dataSource, Table = "example_own_clients" })
    : new RedisBackend(new RedisBackendOptions { Connection = redis, Prefix = "example:own-clients:" });

await using var scheduler = new Scheduler(new SchedulerOptions { Backend = backend });
scheduler.Every("sync", TimeSpan.FromSeconds(2), _ => Task.Delay(100), new() { Jitter = TimeSpan.FromMilliseconds(100), CatchUp = TimeSpan.Zero });
scheduler.Succeeded += (_, run) => Say($"sync ran on {run.InstanceId} in {run.Duration?.TotalMilliseconds:0} ms");

await scheduler.StartAsync();
Say($"started on {scheduler.Backend.Name}");
await Task.Delay(TimeSpan.FromSeconds(7));
await scheduler.StopAsync();

// Still usable after StopAsync().
if (redis is not null) Say($"redis after StopAsync(): ping {(await redis.GetDatabase().PingAsync()).TotalMilliseconds:0.0} ms");
if (dataSource is not null)
{
    await using var command = dataSource.CreateCommand("SELECT 1");
    Say($"data source after StopAsync(): {await command.ExecuteScalarAsync()}");
}
