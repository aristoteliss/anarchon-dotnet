// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon;

/// <summary>
/// Why a run started. New values may be added in a minor release: keep a default branch when you
/// switch over them.
/// </summary>
/// <example>
/// <code>
/// scheduler.Started += (_, e) =>
/// {
///     if (e.Reason is RunReason.Takeover or RunReason.CatchUp) alerts.Notify($"{e.Job} recovered ({e.Reason}) on {e.InstanceId}");
/// };
/// </code>
/// </example>
public enum RunReason
{
    /// <summary>It was due.</summary>
    Scheduled,

    /// <summary><c>RunNowAsync()</c> was called.</summary>
    Manual,

    /// <summary>
    /// It was due while every instance was down, and an instance that started within the job's
    /// <c>CatchUp</c> ran it (lease backends).
    /// </summary>
    CatchUp,

    /// <summary>Its runner died, froze or was stopped, and a backup started the next attempt (lease backends).</summary>
    Takeover,
}

/// <summary>
/// How an attempt stands. New values may be added in a minor release: keep a default branch when
/// you switch over them.
/// </summary>
/// <example>
/// <code>
/// var runs = await scheduler.HistoryAsync("nightly-export", 20);
/// var failures = runs.Where(run => run.Status is RunStatus.Failed or RunStatus.Interrupted);
/// </code>
/// </example>
public enum RunStatus
{
    /// <summary>In progress, or its runner died on a backend without leases.</summary>
    Running,

    /// <summary>The handler returned.</summary>
    Success,

    /// <summary>The handler threw, or its timeout passed.</summary>
    Failed,

    /// <summary>
    /// It didn't finish: its runner died or froze, lost the run to a backup, or was stopped with
    /// <c>StopAsync()</c>.
    /// </summary>
    Interrupted,
}

/// <summary>Which method registered a job. New values may be added in a minor release.</summary>
public enum JobType
{
    /// <summary><c>Cron()</c> or <c>[Cron]</c>.</summary>
    Cron,

    /// <summary><c>Every()</c> or <c>[Every]</c>.</summary>
    Every,

    /// <summary><c>At()</c> or <c>[At]</c>: a one-off job.</summary>
    Once,
}

/// <summary>
/// What to do when a run is due while the previous run of the same job is still running somewhere
/// (lease backends).
/// </summary>
/// <example>
/// <code>
/// scheduler.Every("export", TimeSpan.FromMinutes(5), ExportAsync);                                // never two exports at once
/// scheduler.Every("sample", TimeSpan.FromSeconds(5), SampleAsync, new() { Overlap = OverlapPolicy.Allow }); // each run stands alone
/// </code>
/// </example>
public enum OverlapPolicy
{
    /// <summary>Skip the new run, with a <c>Skipped</c> event of reason <see cref="SkipReason.Overlap"/>. Skipped runs are not queued.</summary>
    Skip,

    /// <summary>Run both.</summary>
    Allow,
}

/// <summary>
/// Which run something is about: a job and one of its scheduled times. Every run event and history
/// record carries it.
/// </summary>
/// <example>
/// <code>
/// static string RunId(RunEvent e) => $"{e.Job}@{e.ScheduledAt:O}";
/// scheduler.Started += (_, e) => logger.LogInformation("started {Run}", RunId(e));
/// </code>
/// </example>
public abstract record RunEvent
{
    /// <summary>The job's name, as given to <c>Every()</c>, <c>Cron()</c>, <c>At()</c> or an attribute.</summary>
    public required string Job { get; init; }

    /// <summary>
    /// When the run was due, in Unix milliseconds. Every instance computes the same value, so
    /// <see cref="Job"/> and <see cref="Slot"/> together name one run across the cluster. For a
    /// <c>RunNowAsync()</c> run: when it was asked for.
    /// </summary>
    public required long Slot { get; init; }

    /// <summary><see cref="Slot"/> as a time.</summary>
    public DateTimeOffset ScheduledAt => DateTimeOffset.FromUnixTimeMilliseconds(Slot);

    /// <summary>
    /// The instance: the one the event happened on, or in history the one that ran the attempt.
    /// Set it with <see cref="SchedulerOptions.InstanceId"/>.
    /// </summary>
    public required string InstanceId { get; init; }
}

/// <summary>
/// One attempt of one run of a job: who ran it, when, and how it ended. Returned by
/// <c>HistoryAsync()</c>, <c>LastRunAsync()</c> and <c>RunNowAsync()</c>, and the payload of
/// <c>Succeeded</c>.
/// </summary>
/// <example>
/// <code>
/// var last = await scheduler.LastRunAsync("sync-customers");
/// if (last is null || DateTimeOffset.UtcNow - last.ScheduledAt > TimeSpan.FromMinutes(30)) alerts.Notify("sync-customers has not run for 30 min");
/// else if (last.Status == RunStatus.Failed) alerts.Notify($"sync-customers failed on {last.InstanceId}: {last.Error}");
/// </code>
/// </example>
public record RunRecord : RunEvent
{
    /// <summary>true when started with <c>RunNowAsync()</c>.</summary>
    public required bool IsManual { get; init; }

    /// <summary>1 for the first try, 2 and more when a backup took over. Each attempt is its own record.</summary>
    public required int Attempt { get; init; }

    /// <summary>Why this attempt started. null on backends that don't record it.</summary>
    public RunReason? Reason { get; init; }

    /// <summary>How the attempt stands.</summary>
    public required RunStatus Status { get; init; }

    /// <summary>When the instance claimed and started the run.</summary>
    public required DateTimeOffset StartedAt { get; init; }

    /// <summary>When the attempt ended. null while it runs, and for an attempt whose runner died.</summary>
    public DateTimeOffset? FinishedAt { get; init; }

    /// <summary><see cref="FinishedAt"/> minus <see cref="StartedAt"/>. null with <see cref="FinishedAt"/>.</summary>
    public TimeSpan? Duration { get; init; }

    /// <summary>The error, as <c>Type: message</c>, when <see cref="Status"/> is Failed or Interrupted.</summary>
    public string? Error { get; init; }
}

/// <summary>A registered job, as <c>GetJobs()</c> reports it.</summary>
/// <example>
/// <code>
/// app.MapGet("/health/jobs", (Scheduler scheduler) =>
///     scheduler.GetJobs().Select(job => new { job.Name, job.Schedule, Next = job.NextRunAt }));
/// </code>
/// </example>
public sealed record JobInfo
{
    /// <summary>The job's name.</summary>
    public required string Name { get; init; }

    /// <summary>Which method registered it.</summary>
    public required JobType Type { get; init; }

    /// <summary>The schedule in words, such as <c>every 10s</c> or <c>cron "0 8 * * 1-5" (Europe/Athens)</c>.</summary>
    public required string Schedule { get; init; }

    /// <summary>The next scheduled run, or null when there are no more (a one-off job that already ran).</summary>
    public DateTimeOffset? NextRunAt { get; init; }

    /// <summary>How many runs of this job this instance is executing right now.</summary>
    public required int RunningHere { get; init; }
}
