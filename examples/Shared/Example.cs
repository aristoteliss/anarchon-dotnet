// Copyright (C) 2026-present Aristotelis — see repository license.

// Shared by the examples: the backend comes from the environment, so every example runs on any of them.
//
//   (nothing)          a SQLite file in the temp folder: no server needed, same machine only
//   BACKEND=redis      REDIS_URL (default localhost:6379), STRATEGY (default lease)
//   BACKEND=postgres   POSTGRES_URL (an Npgsql connection string; default Host=localhost;Database=postgres)
//   BACKEND=sqlite     SQLITE_FILE
//   BACKEND=nats       NATS_URL (default nats://localhost:4222), STRATEGY (default lease)
//   BACKEND=rabbitmq   RABBITMQ_URL (default amqp://localhost:5672): claim only, no leases

using System.Diagnostics;
using Anarchon.Backends;
using Anarchon.Nats;
using Anarchon.Postgres;
using Anarchon.RabbitMQ;
using Anarchon.Redis;
using Anarchon.Sqlite;

namespace Anarchon.Examples;

/// <summary>What every example uses: a backend from the environment, and timed console lines.</summary>
public static class Example
{
    private static readonly Stopwatch Clock = Stopwatch.StartNew();

    /// <summary>A new backend for one instance; all of them point at the same store. <paramref name="name"/> keeps examples apart.</summary>
    /// <exception cref="ArgumentException">BACKEND names no backend.</exception>
    public static IBackend Backend(string name) => (Env("BACKEND") ?? "sqlite") switch
    {
        "redis" => new RedisBackend(new RedisBackendOptions
        {
            Configuration = Env("REDIS_URL") ?? "localhost:6379",
            Strategy = Env("STRATEGY") switch
            {
                "stream-id" => RedisStrategy.StreamId,
                "idmp" => RedisStrategy.Idmp,
                "script" => RedisStrategy.Script,
                _ => RedisStrategy.Lease,
            },
            Prefix = $"example:{name}:",
        }),
        "postgres" => new PostgresBackend(new PostgresBackendOptions { ConnectionString = Env("POSTGRES_URL"), Table = $"example_{name}" }),
        "sqlite" => new SqliteBackend(new SqliteBackendOptions { ConnectionString = $"Data Source={SqliteFile()}", Table = $"example_{name}" }),
        "nats" => new NatsBackend(new NatsBackendOptions
        {
            ConnectionString = Env("NATS_URL") ?? "nats://localhost:4222",
            Strategy = Env("STRATEGY") == "msg-id" ? NatsStrategy.MsgId : NatsStrategy.Lease,
            Stream = $"example-{name}",
            Prefix = $"example.{name}.",
        }),
        "rabbitmq" => new RabbitMQBackend(new RabbitMQBackendOptions { ConnectionString = Env("RABBITMQ_URL") ?? "amqp://localhost:5672", Prefix = $"example.{name}." }),
        var other => throw new ArgumentException($"BACKEND must be redis, postgres, sqlite, nats or rabbitmq (got \"{other}\")"),
    };

    /// <summary>The SQLite file the examples share: SQLITE_FILE, or one in the temp folder.</summary>
    public static string SqliteFile() => Env("SQLITE_FILE") ?? Path.Combine(Path.GetTempPath(), "anarchon-examples.db");

    /// <summary>Prints a line with the seconds since the example started.</summary>
    public static void Say(string line) => Console.WriteLine($"{Clock.Elapsed.TotalSeconds,5:0.0}s  {line}");

    /// <summary>A time of day, for log lines.</summary>
    public static string Time(DateTimeOffset at) => at.ToString("HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// Work that stops when its token is cancelled: it throws, so the run counts as unfinished and,
    /// on a backend with leases, is handed over to another instance.
    /// </summary>
    public static async Task WorkAsync(TimeSpan duration, CancellationToken cancellationToken)
    {
        var step = TimeSpan.FromMilliseconds(250);
        for (var done = TimeSpan.Zero; done < duration; done += step)
        {
            await Task.Delay(step, cancellationToken);
        }
    }

    private static string? Env(string name) => Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : null;
}
