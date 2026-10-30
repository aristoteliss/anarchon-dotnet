// Copyright (C) 2026-present Aristotelis — see repository license.

using StackExchange.Redis;

namespace Anarchon.Redis;

/// <summary>
/// How the Redis backend makes sure each run has exactly one runner. Only <see cref="Lease"/> adds
/// backup runners, retries, catch-up and no overlapping runs; the others only decide who runs each time.
/// </summary>
/// <example>
/// <code>
/// new RedisBackend(new RedisBackendOptions { Configuration = redisUrl, Strategy = RedisStrategy.Script, ScriptWindow = TimeSpan.FromMinutes(10) });
/// </code>
/// </example>
public enum RedisStrategy
{
    /// <summary>
    /// The default (Redis 5+, Valkey): one atomic Lua script claims the run and gives the winner a
    /// lease it renews while the job runs.
    /// </summary>
    Lease,

    /// <summary>
    /// Redis 5+, Valkey: XADD with the run time as the entry ID. The server refuses an ID that is not
    /// bigger than the last one, so only the first instance succeeds. Remembers runs forever.
    /// </summary>
    StreamId,

    /// <summary>
    /// Redis 8.6+ only: XADD with <c>IDMP &lt;job&gt; &lt;run time&gt;</c>; the server keeps the first
    /// entry and ignores repeats for <see cref="RedisBackendOptions.IdmpDuration"/>.
    /// </summary>
    Idmp,

    /// <summary>
    /// Redis 5+, Valkey: a Lua script appends the run only if that run time isn't already in the
    /// stream's last <see cref="RedisBackendOptions.ScriptWindow"/>.
    /// </summary>
    Script,
}

/// <summary>Options of <see cref="RedisBackend"/>.</summary>
/// <example>
/// <code>
/// new RedisBackend(new RedisBackendOptions
/// {
///     Configuration = builder.Configuration.GetConnectionString("redis"), // "cache.internal:6379,password=...,ssl=true"
///     Prefix = "billing:cron:",
/// });
/// </code>
/// </example>
public sealed class RedisBackendOptions
{
    /// <summary>
    /// A StackExchange.Redis configuration string, such as <c>localhost:6379</c> or
    /// <c>cache:6380,password=...,ssl=true</c>. The backend connects with it and closes the connection
    /// on stop. Default <c>localhost:6379</c>. Leave it out when you set <see cref="Connection"/>.
    /// </summary>
    public string? Configuration { get; set; }

    /// <summary>
    /// A connection your app already has. It is shared, and never closed by the scheduler. Leave
    /// <see cref="Configuration"/> out when you set it.
    /// </summary>
    public IConnectionMultiplexer? Connection { get; set; }

    /// <summary>The database number. Default -1: the connection's default database.</summary>
    public int Database { get; set; } = -1;

    /// <summary>
    /// How the single runner of each run is chosen. Default <see cref="RedisStrategy.Lease"/>. Every
    /// instance of a service must use the same strategy.
    /// </summary>
    public RedisStrategy Strategy { get; set; } = RedisStrategy.Lease;

    /// <summary>
    /// Prefix of every key. Default <c>cron:</c>. Give each app (or environment) sharing a Redis its
    /// own; instances that share jobs must use the same one.
    /// </summary>
    public string Prefix { get; set; } = "cron:";

    /// <summary>
    /// <see cref="RedisStrategy.Script"/> only: how long a claimed run is remembered. Must be longer
    /// than every job's jitter plus its MaxLate. Default 5 minutes.
    /// </summary>
    public TimeSpan? ScriptWindow { get; set; }

    /// <summary>
    /// <see cref="RedisStrategy.Idmp"/> only: how long Redis remembers a run's ID, 1 second to 1 day.
    /// Must be longer than every job's jitter plus its MaxLate. Default 5 minutes.
    /// </summary>
    public TimeSpan? IdmpDuration { get; set; }

    /// <summary>
    /// <see cref="RedisStrategy.Idmp"/> only: how many run IDs Redis keeps per job, 1 to 10000.
    /// Together with the time between runs it must cover jitter plus MaxLate. Default 1000.
    /// </summary>
    public int? IdmpMaxSize { get; set; }
}
