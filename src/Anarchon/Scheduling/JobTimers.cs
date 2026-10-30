// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.Scheduling;

/// <summary>Decides when a job's next run is due on this instance, and calls back when it is.</summary>
internal sealed class JobTimers(ISchedulerCore core, Action<Job, long> onDue)
{
    /// <summary>Long waits are split so the clock is checked again at least once a minute.</summary>
    private const long MaxTimerMs = 60_000;

    /// <summary>Schedules the job's next run on this instance: run time plus a random 0 to jitter delay.</summary>
    public void Arm(Job job)
    {
        if (!core.IsRunning || job.Removed) return;

        var slot = job.Schedule.Next(core.Now());
        lock (job.Gate)
        {
            job.NextSlot = slot;
        }

        if (slot is { } due) SleepUntil(job, due + job.RandomJitterMs(), () => onDue(job, due));
    }

    /// <summary>Cancels the job's pending timer, if any (stop and remove).</summary>
    public static void Disarm(Job job)
    {
        lock (job.Gate)
        {
            job.Timer?.Dispose();
            job.Timer = null;
            job.TimerGeneration++;
        }
    }

    private void SleepUntil(Job job, long at, Action then)
    {
        var waitMs = Math.Clamp(at - core.Now(), 0, MaxTimerMs);

        // Created under the job's lock, which the callback takes first: it always sees its own timer set.
        lock (job.Gate)
        {
            job.Timer?.Dispose();
            var generation = ++job.TimerGeneration;
            job.Timer = core.Time.CreateTimer(
                _ => Wake(job, generation, at, then),
                null,
                TimeSpan.FromMilliseconds(waitMs),
                Timeout.InfiniteTimeSpan);
        }
    }

    private void Wake(Job job, long generation, long at, Action then)
    {
        lock (job.Gate)
        {
            // A timer that was cancelled or replaced may still fire once.
            if (job.TimerGeneration != generation) return;
            job.Timer?.Dispose();
            job.Timer = null;
        }

        if (!core.IsRunning || job.Removed) return;
        if (core.Now() < at) SleepUntil(job, at, then);
        else then();
    }
}
