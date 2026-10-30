// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.Backends;

/// <summary>What an instance sends to claim a run.</summary>
/// <example>
/// <code>
/// new ClaimInput { Job = "sync", Slot = 1791633610000, Token = Guid.NewGuid().ToString(), InstanceId = "api-2", HistorySize = 1000 };
/// </code>
/// </example>
public record ClaimInput
{
    /// <summary>The job's name.</summary>
    public required string Job { get; init; }

    /// <summary>The run's slot, in Unix milliseconds.</summary>
    public required long Slot { get; init; }

    /// <summary>A random value unique to this attempt: lets an instance recognize its own claim after a retry.</summary>
    public required string Token { get; init; }

    /// <summary>Who is claiming. Shown in history as the runner.</summary>
    public required string InstanceId { get; init; }

    /// <summary>About how many runs of the job to keep; the backend trims older ones on a claim.</summary>
    public required int HistorySize { get; init; }

    /// <summary>Drop history older than this. null keeps by count only.</summary>
    public TimeSpan? HistoryTtl { get; init; }
}

/// <summary>Whether a lease claim is for a run that is due, or takes over a run whose runner stopped renewing.</summary>
public enum ClaimMode
{
    /// <summary>A run that is due (scheduled, manual or a catch-up).</summary>
    New,

    /// <summary>A run whose runner stopped renewing its lease.</summary>
    Takeover,
}

/// <summary>
/// A claim on a lease backend: <see cref="ClaimInput"/> plus what the store needs to decide it on
/// its own, atomically.
/// </summary>
/// <example>What the scheduler sends for a due run
/// <code>
/// await runs.ClaimAsync(new RunClaim
/// {
///     Job = "report", Slot = 1791633600000, Token = token, InstanceId = "api-1", HistorySize = 1000,
///     Mode = ClaimMode.New, Reason = RunReason.Scheduled, Lease = TimeSpan.FromSeconds(30),
///     Overlap = OverlapPolicy.Skip, MaxAttempts = 3, MaxLate = TimeSpan.FromSeconds(71), Keep = TimeSpan.FromMinutes(5),
/// }, ct);
/// </code>
/// </example>
public sealed record RunClaim : ClaimInput
{
    /// <summary>A new run, or a takeover of a run whose runner stopped renewing.</summary>
    public required ClaimMode Mode { get; init; }

    /// <summary>Stored with the attempt and shown in history.</summary>
    public required RunReason Reason { get; init; }

    /// <summary>The lease given to the winner: it ends this long after the claim, on the store's clock.</summary>
    public required TimeSpan Lease { get; init; }

    /// <summary>Skip: answer <see cref="ClaimOutcome.Overlap"/> while an earlier run of the job has a live lease.</summary>
    public required OverlapPolicy Overlap { get; init; }

    /// <summary>Stored with the run on its first claim; later attempts are checked against the stored value.</summary>
    public required int MaxAttempts { get; init; }

    /// <summary>
    /// Refuse to start a new run more than this long after its slot, on the store's clock. null: no
    /// limit. Stops claims that were delayed or queued during an outage from starting a stale run.
    /// </summary>
    public TimeSpan? MaxLate { get; init; }

    /// <summary>
    /// Never forget a run younger than this, even beyond the history size. Covers <see cref="MaxLate"/>,
    /// so a forgotten run can never be claimed again.
    /// </summary>
    public TimeSpan? Keep { get; init; }
}

/// <summary>
/// What a lease store answers to a claim. Exactly one instance gets <see cref="Won"/> for each
/// attempt of a run. New kinds may be added in a minor release; the scheduler treats a kind it
/// doesn't know like <see cref="Done"/>.
/// </summary>
/// <example>
/// <code>
/// var text = outcome switch
/// {
///     ClaimOutcome.Won won =&gt; $"won attempt {won.Attempt}",
///     ClaimOutcome.Running running =&gt; $"attempt {running.Attempt} is running elsewhere",
///     _ =&gt; "not ours",
/// };
/// </code>
/// </example>
public abstract record ClaimOutcome
{
    private ClaimOutcome()
    {
    }

    /// <summary>This claim won the attempt.</summary>
    /// <param name="Attempt">The attempt this instance runs.</param>
    public sealed record Won(int Attempt) : ClaimOutcome;

    /// <summary>Someone alive is running it.</summary>
    /// <param name="Attempt">The attempt that is running.</param>
    public sealed record Running(int Attempt) : ClaimOutcome;

    /// <summary>Finished, or given up after its attempts.</summary>
    public sealed record Done : ClaimOutcome;

    /// <summary>Another run of the same job is still running (<see cref="OverlapPolicy.Skip"/>).</summary>
    /// <param name="Slot">The slot of the run that is still running.</param>
    public sealed record Overlap(long Slot) : ClaimOutcome;

    /// <summary>A takeover was asked for a run nobody ever claimed.</summary>
    public sealed record None : ClaimOutcome;

    /// <summary>A new run was asked for more than <see cref="RunClaim.MaxLate"/> after its slot, by the store's clock.</summary>
    /// <param name="By">How long after its slot the claim arrived.</param>
    public sealed record Late(TimeSpan By) : ClaimOutcome;
}

/// <summary>An unfinished run of a job, as <see cref="IRunStore.ActiveAsync"/> reports it.</summary>
/// <param name="Slot">The run's slot.</param>
/// <param name="Attempt">The attempt that is (or was) running.</param>
/// <param name="IsAlive">Whether its lease is still alive on the store's clock.</param>
public readonly record struct ActiveRun(long Slot, int Attempt, bool IsAlive);

/// <summary>What the winner stores when an attempt ends. <see cref="RunHistory.Build"/> joins it with the claim.</summary>
/// <example>
/// <code>
/// new StoredResult { Slot = 1791633610000, By = "api-2", Attempt = 1, Status = RunStatus.Failed,
///     StartedAt = started, FinishedAt = finished, Duration = finished - started, Error = "TimeoutException: upstream" };
/// </code>
/// </example>
public sealed record StoredResult
{
    /// <summary>The run's slot.</summary>
    public required long Slot { get; init; }

    /// <summary>The instance that ran this attempt.</summary>
    public required string By { get; init; }

    /// <summary>Which attempt this result belongs to.</summary>
    public int Attempt { get; init; } = 1;

    /// <summary>Success, Failed or Interrupted; never Running.</summary>
    public required RunStatus Status { get; init; }

    /// <summary>When the handler was called, on the instance's clock.</summary>
    public required DateTimeOffset StartedAt { get; init; }

    /// <summary>When the handler ended, on the instance's clock.</summary>
    public required DateTimeOffset FinishedAt { get; init; }

    /// <summary><see cref="FinishedAt"/> minus <see cref="StartedAt"/>.</summary>
    public required TimeSpan Duration { get; init; }

    /// <summary>What the handler threw, as <c>Type: message</c>, cut at 2000 characters.</summary>
    public string? Error { get; init; }
}

/// <summary>
/// A job's settings that a backend may need to refuse it, passed to <see cref="IBackend.CheckJob"/>
/// when the job is added.
/// </summary>
/// <example>A backend that remembers runs for 5 minutes
/// <code>
/// public void CheckJob(JobLimits job)
/// {
///     if (job.Jitter + job.MaxLate &gt;= TimeSpan.FromMinutes(5)) throw new AnarchonException(AnarchonErrorCode.UnsafeJob, $"Job \"{job.Name}\": claims can come too late");
/// }
/// </code>
/// </example>
public sealed record JobLimits
{
    /// <summary>The job's name.</summary>
    public required string Name { get; init; }

    /// <summary>The job's jitter after defaults were applied.</summary>
    public required TimeSpan Jitter { get; init; }

    /// <summary>The job's lateness limit after defaults were applied. With the jitter: the latest a claim is sent.</summary>
    public required TimeSpan MaxLate { get; init; }

    /// <summary>The smallest gap between two runs; <see cref="TimeSpan.MaxValue"/> for a one-off job.</summary>
    public required TimeSpan MinGap { get; init; }

    /// <summary>How many runs the job keeps.</summary>
    public required int HistorySize { get; init; }

    /// <summary>How long the job keeps history, if limited.</summary>
    public TimeSpan? HistoryTtl { get; init; }
}
