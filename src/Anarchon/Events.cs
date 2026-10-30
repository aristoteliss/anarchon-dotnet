// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Diagnostics.CodeAnalysis;

namespace Anarchon;

/// <summary>Payload of <c>Started</c>: this instance won a run and is about to call the handler.</summary>
/// <example>
/// <code>
/// scheduler.Started += (_, e) =>
/// {
///     metrics.Increment("job.start", ("job", e.Job), ("reason", e.Reason));
///     if (e.Reason == RunReason.Takeover) logger.LogWarning("{Job}: attempt {Attempt} taken over by {Instance}", e.Job, e.Attempt, e.InstanceId);
/// };
/// </code>
/// </example>
public sealed record RunStartEvent : RunEvent
{
    /// <summary>true when started with <c>RunNowAsync()</c>.</summary>
    public required bool IsManual { get; init; }

    /// <summary>1, or 2 and more when this instance is taking the run over.</summary>
    public required int Attempt { get; init; }

    /// <summary>Why the run starts here.</summary>
    public required RunReason Reason { get; init; }
}

/// <summary>Payload of <c>Failed</c>: the handler threw, or its timeout passed.</summary>
/// <example>
/// <code>
/// scheduler.Failed += (_, run) =>
/// {
///     if (run.Cause is RateLimitException) return;
///     pager.Notify($"{run.Job} failed on {run.InstanceId} after {run.Duration}: {run.Error}");
/// };
/// </code>
/// </example>
public sealed record RunFailureEvent : RunRecord
{
    /// <summary>Copies a record and adds what the handler threw.</summary>
    [SetsRequiredMembers]
    internal RunFailureEvent(RunRecord record, Exception cause)
        : base(record)
    {
        Cause = cause;
    }

    /// <summary>What the handler threw; a <see cref="TimeoutException"/> when its timeout passed.</summary>
    public Exception Cause { get; init; }
}

/// <summary>
/// Payload of <c>Interrupted</c>: a run ended without finishing because <c>StopAsync()</c> or a
/// backup's takeover cancelled its token and the handler then threw. Not a job failure.
/// </summary>
/// <example>
/// <code>
/// scheduler.Interrupted += (_, run) =>
///     logger.LogInformation("{Job}: stopped on {Instance}, handed over: {HandedOver}", run.Job, run.InstanceId, run.HandedOver);
/// </code>
/// </example>
public sealed record RunInterruptedEvent : RunRecord
{
    /// <summary>Copies a record and adds what the handler threw and whether the run was handed over.</summary>
    [SetsRequiredMembers]
    internal RunInterruptedEvent(RunRecord record, Exception cause, bool handedOver)
        : base(record)
    {
        Cause = cause;
        HandedOver = handedOver;
    }

    /// <summary>What the handler threw, usually an <see cref="OperationCanceledException"/>.</summary>
    public Exception Cause { get; init; }

    /// <summary>The run was given to another instance to finish (a graceful stop on a lease backend).</summary>
    public bool HandedOver { get; init; }
}

/// <summary>Why this instance did not run a run that was due. New values may be added in a minor release.</summary>
public enum SkipReason
{
    /// <summary>Another instance won this run. With several instances, the normal case.</summary>
    Taken,

    /// <summary>This instance reached the run later than the job's <c>MaxLate</c>.</summary>
    Late,

    /// <summary>The previous run of this job is still running (<see cref="OverlapPolicy.Skip"/>).</summary>
    Overlap,
}

/// <summary>
/// Payload of <c>Skipped</c>: a run was due and this instance did not run it. With several
/// instances this is the normal case: for every run, all instances but one raise it with
/// <see cref="SkipReason.Taken"/>.
/// </summary>
/// <example>
/// <code>
/// scheduler.Skipped += (_, e) =>
/// {
///     if (e.Reason == SkipReason.Late) logger.LogWarning("{Job}: reached {Late} late, skipped", e.Job, e.Late);
/// };
/// </code>
/// </example>
public sealed record SkipEvent : RunEvent
{
    /// <summary>Why the run was skipped here.</summary>
    public required SkipReason Reason { get; init; }

    /// <summary>With <see cref="SkipReason.Late"/>: how long after its time the run was reached.</summary>
    public TimeSpan? Late { get; init; }
}

/// <summary>
/// Payload of <c>Lost</c>: this instance stopped owning a run it was executing, because a backup took
/// it over, or because no renewal of its lease was confirmed within the lease, so a backup may be
/// taking it over. The run's token was cancelled. Only on backends with leases.
/// </summary>
/// <example>
/// <code>
/// scheduler.Lost += (_, e) => logger.LogWarning("{Job} @ {Due}: attempt {Attempt} was taken over", e.Job, e.ScheduledAt, e.Attempt);
/// </code>
/// </example>
public sealed record LostEvent : RunEvent
{
    /// <summary>The attempt this instance was running. The backup runs the next one.</summary>
    public required int Attempt { get; init; }
}

/// <summary>
/// Payload of <c>Error</c>: a backend or setup problem, not a job failure (those are <c>Failed</c>).
/// Without an <c>Error</c> handler these are logged instead.
/// </summary>
/// <example>
/// <code>
/// scheduler.Error += (_, e) => logger.LogError(e.Error, "store problem for {Job}", e.Job ?? "the scheduler");
/// </code>
/// </example>
public sealed record SchedulerErrorEvent
{
    /// <summary>What went wrong: a connection error, a refused write.</summary>
    public required Exception Error { get; init; }

    /// <summary>The job, when the problem belongs to one.</summary>
    public string? Job { get; init; }

    /// <summary>The run's slot, when the problem belongs to one run.</summary>
    public long? Slot { get; init; }
}
