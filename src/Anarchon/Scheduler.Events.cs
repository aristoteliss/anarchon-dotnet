// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;
using Anarchon.Scheduling;

namespace Anarchon;

/// <remarks>
/// Events describe what happens on this instance only; <see cref="HistoryAsync"/> is the view
/// across all instances. Handlers run on the thread that raised the event: keep them short, and
/// don't block. A handler that throws is logged and doesn't affect the run.
/// </remarks>
public sealed partial class Scheduler : ISchedulerCore
{
    /// <summary>This instance won a run and is about to call the handler.</summary>
    /// <example>
    /// <code>
    /// scheduler.Started += (_, e) =&gt; metrics.Increment("job.start", ("job", e.Job));
    /// </code>
    /// </example>
    public event EventHandler<RunStartEvent>? Started;

    /// <summary>The handler returned.</summary>
    /// <example>
    /// <code>
    /// scheduler.Succeeded += (_, run) =&gt; metrics.Record("job.duration", run.Duration!.Value.TotalMilliseconds, ("job", run.Job));
    /// </code>
    /// </example>
    public event EventHandler<RunRecord>? Succeeded;

    /// <summary>The handler threw, or its timeout passed.</summary>
    /// <example>
    /// <code>
    /// scheduler.Failed += (_, run) =&gt; logger.LogError(run.Cause, "{Job} failed on {Instance}", run.Job, run.InstanceId);
    /// </code>
    /// </example>
    public event EventHandler<RunFailureEvent>? Failed;

    /// <summary>Stop or a takeover cancelled the run's token, and the handler then threw.</summary>
    /// <example>
    /// <code>
    /// scheduler.Interrupted += (_, run) =&gt; logger.LogInformation("{Job} stopped mid-run, handed over: {HandedOver}", run.Job, run.HandedOver);
    /// </code>
    /// </example>
    public event EventHandler<RunInterruptedEvent>? Interrupted;

    /// <summary>A run was due and this instance did not run it: taken by another instance, too late, or overlapping.</summary>
    /// <example>
    /// <code>
    /// scheduler.Skipped += (_, e) =&gt; logger.LogDebug("{Job}: {Reason}", e.Job, e.Reason);
    /// </code>
    /// </example>
    public event EventHandler<SkipEvent>? Skipped;

    /// <summary>A backup took over a run this instance was executing, or its lease could not be renewed in time.</summary>
    /// <example>
    /// <code>
    /// scheduler.Lost += (_, e) =&gt; logger.LogWarning("{Job}: a backup took over attempt {Attempt}", e.Job, e.Attempt);
    /// </code>
    /// </example>
    public event EventHandler<LostEvent>? Lost;

    /// <summary>A backend or setup problem; never a job failure. Without a handler it is logged at Error.</summary>
    /// <example>
    /// <code>
    /// scheduler.Error += (_, e) =&gt; logger.LogError(e.Error, "backend problem for {Job}", e.Job);
    /// </code>
    /// </example>
    public event EventHandler<SchedulerErrorEvent>? Error;

    long ISchedulerCore.LeaseMs => LeaseMs;

    long ISchedulerCore.HeartbeatMs => HeartbeatMs;

    TimeProvider ISchedulerCore.Time => Time;

    object? ISchedulerCore.SchedulerData => Volatile.Read(ref _data);

    bool ISchedulerCore.IsRunning => _state == State.Running;

    bool ISchedulerCore.IsIdle => _state == State.Idle;

    CancellationToken ISchedulerCore.StopToken => Volatile.Read(ref _lifecycle).Token;

    long ISchedulerCore.Now() => Now();

    Task ISchedulerCore.EnsureReadyAsync() => EnsureReadyAsync();

    Task ISchedulerCore.PrepareAsync(Job job) => PrepareAsync(job);

    Task ISchedulerCore.Track(Task work) => Track(work);

    Task<T> ISchedulerCore.Track<T>(Task<T> work) => Track(work);

    void ISchedulerCore.OnStarted(RunStartEvent e)
    {
        Log.Started(_logger, e.Job, e.ScheduledAt, e.InstanceId, e.Attempt, e.Reason);
        Raise(Started, e, nameof(Started));
    }

    void ISchedulerCore.OnSucceeded(RunRecord record)
    {
        Log.Succeeded(_logger, record.Job, record.ScheduledAt, Ms(record.Duration));
        Raise(Succeeded, record, nameof(Succeeded));
    }

    void ISchedulerCore.OnFailed(RunFailureEvent e)
    {
        Log.Failed(_logger, e.Job, e.ScheduledAt, Ms(e.Duration), e.Error, e.Cause);
        Raise(Failed, e, nameof(Failed));
    }

    void ISchedulerCore.OnInterrupted(RunInterruptedEvent e)
    {
        Log.Interrupted(_logger, e.Job, e.ScheduledAt, Ms(e.Duration), e.HandedOver);
        Raise(Interrupted, e, nameof(Interrupted));
    }

    void ISchedulerCore.OnSkipped(SkipEvent e) => OnSkipped(e);

    void ISchedulerCore.OnLost(LostEvent e)
    {
        Log.Lost(_logger, e.Job, e.ScheduledAt, e.Attempt);
        Raise(Lost, e, nameof(Lost));
    }

    void ISchedulerCore.OnError(Exception error, string? job, long? slot)
    {
        var handlers = Error;
        if (handlers is null)
        {
            Log.Unhandled(_logger, job is null ? "the scheduler" : $"job \"{job}\"", error.Message, error);
            return;
        }

        Raise(handlers, new SchedulerErrorEvent { Error = error, Job = job, Slot = slot }, nameof(Error));
    }

    private void OnSkipped(SkipEvent e)
    {
        Log.Skipped(_logger, e.Job, e.ScheduledAt, e.Reason);
        Raise(Skipped, e, nameof(Skipped));
    }

    /// <summary>Calls every handler, each on its own: one that throws is logged and the others still run.</summary>
    private void Raise<T>(EventHandler<T>? handlers, T payload, string name)
    {
        if (handlers is null) return;

        foreach (var handler in handlers.GetInvocationList().Cast<EventHandler<T>>())
        {
            try
            {
                handler(this, payload);
            }
            catch (Exception error)
            {
                Log.HandlerThrew(_logger, name, error);
            }
        }
    }

    private static double Ms(TimeSpan? duration) => duration?.TotalMilliseconds ?? 0;

    string ISchedulerCore.InstanceId => InstanceId;

    IBackend ISchedulerCore.Backend => Backend;
}
