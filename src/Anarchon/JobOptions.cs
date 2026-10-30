// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon;

/// <summary>
/// Settings of one job. Each falls back to the scheduler's option of the same name, then to the
/// default given here. One-off jobs (<c>At()</c>) take these; recurring ones take
/// <see cref="CronJobOptions"/> or <see cref="EveryJobOptions"/>.
/// </summary>
/// <example>A one-off job that runs once, for at most an hour, and is never retried
/// <code>
/// scheduler.At("migrate-2026-12", DateTimeOffset.Parse("2026-12-01T03:00:00Z"), MigrateAsync,
///     new() { Timeout = TimeSpan.FromHours(1), MaxAttempts = 1 });
/// </code>
/// </example>
public class JobOptions
{
    /// <summary>
    /// Each instance waits a random 0 to <c>Jitter</c> before claiming a run, so a random instance
    /// wins each time and the work is spread. Zero: the instance with the fastest clock wins every
    /// time. Must be under half the time between runs. Default: the scheduler's
    /// <see cref="SchedulerOptions.Jitter"/>, or a quarter of the time between runs if smaller.
    /// </summary>
    public TimeSpan? Jitter { get; set; }

    /// <summary>
    /// An instance that reaches a run later than this after its time skips it, with a
    /// <c>Skipped</c> event of reason <see cref="SkipReason.Late"/>. Stops a process that was frozen
    /// from starting a stale run when it wakes. Must be bigger than <see cref="Jitter"/>. Default:
    /// the scheduler's <see cref="SchedulerOptions.MaxLate"/>.
    /// </summary>
    public TimeSpan? MaxLate { get; set; }

    /// <summary>About how many runs to keep in history. Default: the scheduler's <see cref="SchedulerOptions.HistorySize"/>.</summary>
    public int? HistorySize { get; set; }

    /// <summary>Also drop history older than this. Default: the scheduler's <see cref="SchedulerOptions.HistoryTtl"/>.</summary>
    public TimeSpan? HistoryTtl { get; set; }

    /// <summary>
    /// Data for this job's runs, handed to the handler as <see cref="JobContext.Data"/>: a value, or a
    /// <see cref="JobDataProvider"/> called when each run starts. It goes on top of the scheduler's
    /// <see cref="SchedulerOptions.Data"/>.
    /// </summary>
    public object? Data { get; set; }

    /// <summary>
    /// Cancels the run's token and marks the run Failed with a <see cref="TimeoutException"/> after
    /// this long. The handler is not killed: it should watch its token. Set it on jobs that can hang,
    /// since with <see cref="OverlapPolicy.Skip"/> a hung run blocks every later run of the job.
    /// Default: no limit.
    /// </summary>
    public TimeSpan? Timeout { get; set; }

    /// <summary>
    /// How many times a run may be started when runners die, first try included. 1: never retried.
    /// A hand-over on stop doesn't count, and a run that fails by throwing is not retried either:
    /// retries are for runners that died. 1 to 100. Default 3. Lease backends only.
    /// </summary>
    public int? MaxAttempts { get; set; }

    /// <summary>
    /// On start, run the latest scheduled run if nobody ran it and it was due at most this long ago
    /// (it was missed while every instance was down). Only the latest run is caught up. Zero: never.
    /// Default 5 minutes. Lease backends only.
    /// </summary>
    public TimeSpan? CatchUp { get; set; }

    /// <summary>
    /// After this instance's last run of the job failed, wait before claiming its next run, so a
    /// healthy instance claims it first. The wait ends <c>Jitter + Heartbeat</c> after the run's time,
    /// never later than <see cref="MaxLate"/>; if nobody claimed the run by then, this instance runs
    /// it. Use it when a fault can stay with one instance (a broken connection, a full disk).
    /// Default false. Lease backends only.
    /// </summary>
    public bool? YieldAfterFailure { get; set; }
}

/// <summary>Settings of a recurring job: <see cref="JobOptions"/> and what to do when runs overlap.</summary>
/// <example>
/// <code>
/// scheduler.Every("sample", TimeSpan.FromSeconds(5), SampleAsync, new() { Overlap = OverlapPolicy.Allow });
/// </code>
/// </example>
public class RecurringJobOptions : JobOptions
{
    /// <summary>
    /// What to do when a run is due while the previous one is still running somewhere. Default
    /// <see cref="OverlapPolicy.Skip"/>. Lease backends only.
    /// </summary>
    public OverlapPolicy? Overlap { get; set; }
}

/// <summary>Settings of a cron job.</summary>
/// <example>
/// <code>
/// scheduler.Cron("open-market", "30 9 * * 1-5", OpenAsync,
///     new() { TimeZone = TimeZoneInfo.FindSystemTimeZoneById("America/New_York") });
/// </code>
/// </example>
public sealed class CronJobOptions : RecurringJobOptions
{
    /// <summary>
    /// The time zone of the cron expression. Default: the scheduler's
    /// <see cref="SchedulerOptions.TimeZone"/>, then the server's local zone. Daylight-saving changes
    /// are followed: <c>0 8 * * *</c> stays 08:00 local time all year.
    /// </summary>
    public TimeZoneInfo? TimeZone { get; set; }
}

/// <summary>Settings of an interval job.</summary>
/// <example>Two hourly jobs that should not start together
/// <code>
/// scheduler.Every("a", TimeSpan.FromHours(1), RunAAsync);                                         // 00:00, 01:00, ...
/// scheduler.Every("b", TimeSpan.FromHours(1), RunBAsync, new() { Offset = TimeSpan.FromMinutes(30) }); // 00:30, 01:30, ...
/// </code>
/// </example>
public sealed class EveryJobOptions : RecurringJobOptions
{
    /// <summary>
    /// Shifts the interval grid. With zero, runs fall on multiples of the interval since 1970 UTC:
    /// every 10 seconds runs at :00, :10, :20; every hour on the hour. Must be the same on every
    /// instance. Default zero.
    /// </summary>
    public TimeSpan? Offset { get; set; }
}
