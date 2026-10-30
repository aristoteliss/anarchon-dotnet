// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Scheduling;

/// <summary>
/// What the parts of the scheduler (timers, runners, executor) share. The scheduler owns the state
/// and hands this view of it to each part, so no part reaches into another.
/// </summary>
internal interface ISchedulerCore
{
    /// <summary>This instance's name: the runner in its claims, and the instance in events and history.</summary>
    string InstanceId { get; }

    IBackend Backend { get; }

    /// <summary>Lease backends: how long a lease lasts without a heartbeat, in ms.</summary>
    long LeaseMs { get; }

    /// <summary>Lease backends: how often a runner renews its lease, in ms; backups check every 1 to 2 of these.</summary>
    long HeartbeatMs { get; }

    TimeProvider Time { get; }

    /// <summary>The scheduler's data as it is now: a value, or a provider to call when a run starts.</summary>
    object? SchedulerData { get; }

    /// <summary>Between start and stop. Timers and background checks do nothing outside it.</summary>
    bool IsRunning { get; }

    /// <summary>Stop has returned (or start was never called): nothing is written to the backend any more.</summary>
    bool IsIdle { get; }

    /// <summary>Cancelled when stop is called. Every start makes a new one.</summary>
    CancellationToken StopToken { get; }

    /// <summary>Unix milliseconds, from <see cref="Time"/>.</summary>
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
