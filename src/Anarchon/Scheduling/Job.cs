// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.Scheduling;

/// <summary>A registered job: its settings after validation, and what this instance knows about it right now.</summary>
internal sealed class Job
{
    private int _runningHere;
    private volatile bool _failedHere;
    private volatile bool _removed;

    /// <summary>Unique per scheduler, and the job's identity in the backend: the same on every instance.</summary>
    public required string Name { get; init; }

    public required Schedule Schedule { get; init; }

    public required Func<JobContext, Task> Handler { get; init; }

    /// <summary>The job's data option: a value, or a provider to call when a run starts.</summary>
    public required object? Data { get; init; }

    /// <summary>This instance waits a random 0 to JitterMs after a slot before claiming it.</summary>
    public required long JitterMs { get; init; }

    /// <summary>A run reached later than this after its slot is skipped as late. Always bigger than JitterMs.</summary>
    public required long MaxLateMs { get; init; }

    public required int HistorySize { get; init; }

    public required TimeSpan? HistoryTtl { get; init; }

    public required OverlapPolicy Overlap { get; init; }

    public required long? TimeoutMs { get; init; }

    public required int MaxAttempts { get; init; }

    /// <summary>On start, the latest slot runs if nobody ran it and it was due at most this long ago. 0: never.</summary>
    public required long CatchUpMs { get; init; }

    public required bool YieldAfterFailure { get; init; }

    /// <summary>How long the store must remember a run, so a late claim can't run it twice.</summary>
    public required long KeepMs { get; init; }

    /// <summary>Guards <see cref="Timer"/>, <see cref="TimerGeneration"/>, <see cref="NextSlot"/> and <see cref="Prepared"/>.</summary>
    public Lock Gate { get; } = new();

    /// <summary>The pending timer for <see cref="NextSlot"/>, if any.</summary>
    public ITimer? Timer { get; set; }

    /// <summary>Changes whenever the timer is set or cancelled, so a timer that fires late sees it was replaced.</summary>
    public long TimerGeneration { get; set; }

    /// <summary>The slot the timer waits for; null when the schedule has no more runs.</summary>
    public long? NextSlot { get; set; }

    /// <summary>The backend's one-time setup for this job, started once and shared; reset when the backend is closed.</summary>
    public Task? Prepared { get; set; }

    public int RunningHere => Volatile.Read(ref _runningHere);

    /// <summary>Whether the last run of this job that ended here failed (manual runs aside).</summary>
    public bool FailedHere
    {
        get => _failedHere;
        set => _failedHere = value;
    }

    /// <summary>Set by remove. Timers and backup checks that fire afterwards see it and do nothing.</summary>
    public bool Removed
    {
        get => _removed;
        set => _removed = value;
    }

    public void EnterRun() => Interlocked.Increment(ref _runningHere);

    public void ExitRun() => Interlocked.Decrement(ref _runningHere);

    /// <summary>A random 0 to JitterMs delay: which instance asks first, and so wins, differs from run to run.</summary>
    public long RandomJitterMs() => JitterMs > 0 ? Random.Shared.NextInt64(JitterMs + 1) : 0;
}
