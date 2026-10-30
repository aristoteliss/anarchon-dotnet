// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;
using Microsoft.Extensions.Logging;

namespace Anarchon;

/// <summary>
/// Options of a <see cref="Scheduler"/>. <see cref="Backend"/> is required: no store is assumed.
/// Give every instance of a service the same options, apart from <see cref="InstanceId"/>.
/// </summary>
/// <example>Production-style setup on Redis
/// <code>
/// var scheduler = new Scheduler(new SchedulerOptions
/// {
///     Backend = new RedisBackend(new RedisBackendOptions { Configuration = redisUrl, Prefix = "billing:cron:" }),
///     InstanceId = Environment.GetEnvironmentVariable("POD_NAME"),
///     TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Athens"),
///     LoggerFactory = loggerFactory,
/// });
/// </code>
/// </example>
public sealed class SchedulerOptions
{
    /// <summary>
    /// Where runs are claimed and recorded. <c>StopAsync()</c> closes only the connections the backend
    /// opened itself, never a client you passed to it. Required.
    /// </summary>
    public IBackend? Backend { get; set; }

    /// <summary>
    /// Name of this instance in history and events. Must differ between instances, or history can't
    /// tell them apart. Default <c>machine:pid</c>; a pod or host name reads better.
    /// </summary>
    public string? InstanceId { get; set; }

    /// <summary>
    /// Data for the runs of every job, handed to each handler as <see cref="JobContext.Data"/>: a
    /// value, or a <see cref="JobDataProvider"/> called when a run starts. A job's own data goes on
    /// top of it, and <c>SetData()</c> replaces it while the scheduler runs.
    /// </summary>
    public object? Data { get; set; }

    /// <summary>
    /// Default jitter for all jobs: each instance waits a random 0 to <c>Jitter</c> after a run is due
    /// before claiming it, which spreads the work. Lowered to a quarter of the interval for short
    /// intervals. Default 2 seconds.
    /// </summary>
    public TimeSpan Jitter { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Default <see cref="JobOptions.MaxLate"/>: an instance that reaches a run later than this skips
    /// it. Default 60 seconds.
    /// </summary>
    public TimeSpan MaxLate { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Default history size per job: about how many runs <c>HistoryAsync()</c> can return. Default 1000.</summary>
    public int HistorySize { get; set; } = 1_000;

    /// <summary>Default history age limit per job: older records are dropped too. Default: none (by count only).</summary>
    public TimeSpan? HistoryTtl { get; set; }

    /// <summary>
    /// Default time zone of cron jobs. Set it: the default is the server's local zone, which can
    /// differ between machines. Interval jobs ignore it (they run on a UTC grid).
    /// </summary>
    public TimeZoneInfo? TimeZone { get; set; }

    /// <summary>
    /// A running job's lease lasts this long and is renewed every <see cref="Heartbeat"/>. A runner
    /// that stops renewing (crash, freeze, lost network) loses the run to a backup about this long
    /// later. Shorter means faster takeover; longer tolerates longer pauses. Default 30 seconds. Lease
    /// backends only: setting it on another backend is refused.
    /// </summary>
    public TimeSpan? Lease { get; set; }

    /// <summary>
    /// How often a running job renews its lease, and how often backups check on it (every 1 to 2
    /// heartbeats). Must be under half of <see cref="Lease"/>. Default 5 seconds, or a sixth of the
    /// lease if smaller. Lease backends only.
    /// </summary>
    public TimeSpan? Heartbeat { get; set; }

    /// <summary>
    /// How long <c>StopAsync()</c> waits for running jobs before it closes the backend. A job still
    /// running then stops renewing its lease, so a backup takes it over. Default 30 seconds.
    /// </summary>
    public TimeSpan StopTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Where the scheduler logs, under the category <c>Anarchon.Scheduler</c>: each run at Debug,
    /// failed and lost runs at Warning, problems nobody handles at Error. Default: no logging.
    /// </summary>
    public ILoggerFactory? LoggerFactory { get; set; }

    /// <summary>The clock and timers the scheduler uses. Default <see cref="System.TimeProvider.System"/>.</summary>
    public TimeProvider? TimeProvider { get; set; }
}
