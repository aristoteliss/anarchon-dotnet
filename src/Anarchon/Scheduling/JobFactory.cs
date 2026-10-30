// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Scheduling;

/// <summary>The scheduler-wide defaults a job falls back to.</summary>
internal sealed record SchedulerDefaults(long JitterMs, long MaxLateMs, int HistorySize, TimeSpan? HistoryTtl, TimeZoneInfo? TimeZone);

/// <summary>What a job is built from: its declaration, the scheduler's defaults and the backend that runs it.</summary>
internal sealed record JobInput(
    string Name,
    Schedule Schedule,
    Func<JobContext, Task> Handler,
    JobOptions Options,
    SchedulerDefaults Defaults,
    IBackend Backend);

/// <summary>A job's history, timeout and retry settings after defaults were applied.</summary>
internal sealed record RunLimits(int HistorySize, TimeSpan? HistoryTtl, long? TimeoutMs, int MaxAttempts, long CatchUpMs);

/// <summary>Checks a job's settings against its schedule and the backend, and builds it.</summary>
internal static class JobFactory
{
    /// <summary>
    /// Lease backends: slack for clock differences and network delays when the store checks how late
    /// a claim is. Its only job is to stop claims that are far too late.
    /// </summary>
    public const long ClockToleranceMs = 10_000;

    private const int MaxAttemptsLimit = 100;

    public static Job Create(JobInput input)
    {
        var (name, schedule, handler, options, defaults, backend) = input;
        RefuseInvalidName(name);
        ArgumentNullException.ThrowIfNull(handler);
        RefuseWithoutLeases(backend, options, $"Job \"{name}\": ");

        var (jitterMs, maxLateMs) = ReadTiming(input);
        var (historySize, historyTtl, timeoutMs, maxAttempts, catchUpMs) = ReadLimits(name, options, defaults);

        backend.CheckJob(new JobLimits
        {
            Name = name,
            Jitter = TimeSpan.FromMilliseconds(jitterMs),
            MaxLate = TimeSpan.FromMilliseconds(maxLateMs),
            MinGap = schedule.MinGapMs == long.MaxValue ? TimeSpan.MaxValue : TimeSpan.FromMilliseconds(schedule.MinGapMs),
            HistorySize = historySize,
            HistoryTtl = historyTtl,
        });

        return new Job
        {
            Name = name,
            Schedule = schedule,
            Handler = handler,
            Data = options.Data,
            JitterMs = jitterMs,
            MaxLateMs = maxLateMs,
            HistorySize = historySize,
            HistoryTtl = historyTtl,
            Overlap = (options as RecurringJobOptions)?.Overlap ?? OverlapPolicy.Skip,
            TimeoutMs = timeoutMs,
            MaxAttempts = maxAttempts,
            CatchUpMs = catchUpMs,
            YieldAfterFailure = options.YieldAfterFailure ?? false,
            // Covers the latest claim this job can make: a scheduled run up to MaxLate late, or a
            // catch-up up to CatchUp + Jitter late.
            KeepMs = Math.Max(maxLateMs, catchUpMs + jitterMs) + ClockToleranceMs,
        };
    }

    /// <summary>Reads the history, timeout and retry settings, each falling back to the scheduler's default or its own.</summary>
    private static RunLimits ReadLimits(string name, JobOptions options, SchedulerDefaults defaults)
    {
        var historySize = options.HistorySize is { } size ? Settings.Positive($"Job \"{name}\": HistorySize", size) : defaults.HistorySize;
        var historyTtl = options.HistoryTtl is { } ttl ? Settings.PositiveTime($"Job \"{name}\": HistoryTtl", ttl) : defaults.HistoryTtl;
        var timeoutMs = options.Timeout is { } timeout ? Settings.PositiveMs($"Job \"{name}\": Timeout", timeout) : (long?)null;
        var maxAttempts = Settings.InRange($"Job \"{name}\": MaxAttempts", options.MaxAttempts ?? 3, 1, MaxAttemptsLimit);
        var catchUpMs = Settings.NonNegativeMs($"Job \"{name}\": CatchUp", options.CatchUp ?? TimeSpan.FromMinutes(5));

        return new RunLimits(historySize, historyTtl, timeoutMs, maxAttempts, catchUpMs);
    }

    /// <summary>Throws when a backend without leases is given options only leases can honour.</summary>
    public static void RefuseWithoutLeases(IBackend backend, JobOptions options, string label)
    {
        if (backend.Runs is not null) return;

        var set = LeaseOptionSet(options);
        if (set is null) return;

        throw new AnarchonException(
            AnarchonErrorCode.Unsupported,
            $"{label}{set} needs a backend with leases (such as Redis with the Lease strategy); {backend.Name} has none");
    }

    private static string? LeaseOptionSet(JobOptions options)
    {
        if ((options as RecurringJobOptions)?.Overlap is not null) return nameof(RecurringJobOptions.Overlap);
        if (options.MaxAttempts is not null) return nameof(JobOptions.MaxAttempts);
        if (options.CatchUp is not null) return nameof(JobOptions.CatchUp);
        return options.YieldAfterFailure is not null ? nameof(JobOptions.YieldAfterFailure) : null;
    }

    private static void RefuseInvalidName(string name)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("A job name must be a non-empty string", nameof(name));
    }

    /// <summary>Reads the jitter and the lateness limit, and refuses timings the schedule or the backend can't keep.</summary>
    private static (long JitterMs, long MaxLateMs) ReadTiming(JobInput input)
    {
        var (name, _, _, options, defaults, backend) = input;
        var jitterMs = ReadJitter(input);

        var maxLateMs = options.MaxLate is { } maxLate
            ? Settings.PositiveMs($"Job \"{name}\": MaxLate", maxLate)
            : Math.Max(defaults.MaxLateMs, jitterMs + 1_000);
        if (maxLateMs <= jitterMs)
        {
            throw new ArgumentOutOfRangeException(
                nameof(input),
                $"Job \"{name}\": MaxLate ({maxLateMs} ms) must be bigger than Jitter ({jitterMs} ms)");
        }

        var windowMs = backend.DedupeWindow == TimeSpan.MaxValue ? long.MaxValue : (long)backend.DedupeWindow.TotalMilliseconds;
        if (jitterMs + maxLateMs >= windowMs)
        {
            throw new AnarchonException(
                AnarchonErrorCode.UnsafeJob,
                $"Job \"{name}\": Jitter + MaxLate ({jitterMs + maxLateMs} ms) must be shorter than how long " +
                $"{backend.Name} remembers a run ({windowMs} ms), or a late instance could run it again");
        }

        return (jitterMs, maxLateMs);
    }

    /// <summary>Reads the jitter, by default capped at a quarter of the time between runs; refuses half or more.</summary>
    private static long ReadJitter(JobInput input)
    {
        var (name, schedule, _, options, defaults, _) = input;
        var jitterMs = options.Jitter is { } jitter
            ? Settings.NonNegativeMs($"Job \"{name}\": Jitter", jitter)
            : Math.Min(defaults.JitterMs, schedule.MinGapMs / 4);

        if (jitterMs * 2 >= schedule.MinGapMs)
        {
            throw new ArgumentOutOfRangeException(
                nameof(input),
                $"Job \"{name}\": Jitter ({jitterMs} ms) must be under half the time between runs ({schedule.MinGapMs} ms)");
        }

        return jitterMs;
    }
}
