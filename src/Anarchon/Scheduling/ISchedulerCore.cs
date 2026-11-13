// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.Scheduling;

/// <summary>
/// What the parts of the scheduler (timers, runners, executor) share. The scheduler owns the state
/// and hands this view of it to each part, so no part reaches into another.
/// </summary>
internal interface ISchedulerCore
{
    /// <summary>The scheduler's options as read at construction: they never change.</summary>
    SchedulerSettings Settings { get; }

    /// <summary>The scheduler's data as it is now: a value, or a provider to call when a run starts.</summary>
    object? SchedulerData { get; }

    /// <summary>
    /// The scheduler is between start and stop and the job is still registered. Timers and background
    /// checks start nothing when this is false.
    /// </summary>
    bool CanStart(Job job);

    /// <summary>
    /// A run that began under <paramref name="stop"/> may still write to the backend: stop has not
    /// returned, and no later start replaced the lifecycle the run belongs to.
    /// </summary>
    bool CanWrite(CancellationToken stop);

    /// <summary>Cancelled when stop is called. Every start makes a new one.</summary>
    CancellationToken StopToken { get; }

    /// <summary>Unix milliseconds, from <see cref="SchedulerSettings.Time"/>.</summary>
    long Now();

    /// <summary>Connects the backend once; later calls wait for the same connection.</summary>
    Task EnsureReadyAsync();

    /// <summary>The backend's one-time setup for a job.</summary>
    Task PrepareAsync(Job job);

    /// <summary>Registers work that stop must wait for, and returns it.</summary>
    Task Track(Task work);

    /// <summary>Registers work that stop must wait for, and returns it.</summary>
    Task<T> Track<T>(Task<T> work);

    /// <summary>Raises <c>Started</c>. Never throws: a handler that throws is logged.</summary>
    void OnStarted(RunStartEvent e);

    void OnSucceeded(RunRecord record);

    void OnFailed(RunFailureEvent e);

    void OnInterrupted(RunInterruptedEvent e);

    void OnSkipped(SkipEvent e);

    void OnLost(LostEvent e);

    /// <summary>Raises <c>Error</c>, or logs the error when nobody handles it.</summary>
    void OnError(Exception error, string? job = null, long? slot = null);
}
