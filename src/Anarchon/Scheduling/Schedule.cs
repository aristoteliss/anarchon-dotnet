// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Globalization;
using Cronos;

namespace Anarchon.Scheduling;

/// <summary>
/// Turns "now" into the next and previous run time. Every instance computes the same run times
/// from the same schedule, so a run time ("slot") names one run across the cluster.
/// </summary>
internal abstract class Schedule
{
    public abstract JobType Type { get; }

    /// <summary>For people: <c>every 10s</c>, <c>cron "0 8 * * 1-5" (Europe/Athens)</c>.</summary>
    public abstract string Description { get; }

    /// <summary>The smallest gap between two runs, in ms; <see cref="long.MaxValue"/> when there is no second run.</summary>
    public abstract long MinGapMs { get; }

    /// <summary>The first run time strictly after <paramref name="after"/> (Unix ms), or null when there are no more.</summary>
    public abstract long? Next(long after);

    /// <summary>The latest run time at or before <paramref name="atOrBefore"/> (Unix ms), or null when there is none.</summary>
    public abstract long? Prev(long atOrBefore);

    protected static string ToIso(long ms) =>
        DateTimeOffset.FromUnixTimeMilliseconds(ms).ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture);
}

/// <summary>
/// Runs every interval on a grid counted from 1970-01-01 UTC, plus an offset. All instances share
/// the grid, so they agree on run times whenever each one started.
/// </summary>
internal sealed class EverySchedule : Schedule
{
    private readonly long _intervalMs;
    private readonly long _offsetMs;

    public EverySchedule(long intervalMs, long offsetMs)
    {
        _intervalMs = intervalMs;
        _offsetMs = ((offsetMs % intervalMs) + intervalMs) % intervalMs;
    }

    public override JobType Type => JobType.Every;

    public override string Description
    {
        get
        {
            var every = $"every {Duration.Format(TimeSpan.FromMilliseconds(_intervalMs))}";
            return _offsetMs == 0 ? every : $"{every} (offset {Duration.Format(TimeSpan.FromMilliseconds(_offsetMs))})";
        }
    }

    public override long MinGapMs => _intervalMs;

    public override long? Next(long after) => ((FloorDiv(after - _offsetMs, _intervalMs) + 1) * _intervalMs) + _offsetMs;

    public override long? Prev(long atOrBefore) => (FloorDiv(atOrBefore - _offsetMs, _intervalMs) * _intervalMs) + _offsetMs;

    private static long FloorDiv(long value, long divisor)
    {
        var quotient = value / divisor;
        return value % divisor < 0 ? quotient - 1 : quotient;
    }
}

/// <summary>Runs once, at one time.</summary>
internal sealed class OnceSchedule(long atMs) : Schedule
{
    public override JobType Type => JobType.Once;

    public override string Description => $"once at {ToIso(atMs)}";

    public override long MinGapMs => long.MaxValue;

    public override long? Next(long after) => atMs > after ? atMs : null;

    public override long? Prev(long atOrBefore) => atMs <= atOrBefore ? atMs : null;
}

/// <summary>Standard cron syntax with 5 fields; a 6th leading field adds seconds (<c>*/10 * * * * *</c>).</summary>
internal sealed class CronSchedule : Schedule
{
    /// <summary>How many upcoming runs are sampled to find the smallest gap between runs.</summary>
    private const int GapSamples = 64;

    /// <summary>The first runs are sampled this far ahead at most, enough for a run every leap year.</summary>
    private const int GapSampleYears = 30;

    private readonly CronExpression _cron;
    private readonly TimeZoneInfo _zone;
    private readonly string _description;

    private CronSchedule(CronExpression cron, TimeZoneInfo zone, string description, long minGapMs)
    {
        _cron = cron;
        _zone = zone;
        _description = description;
        MinGapMs = minGapMs;
    }

    public override JobType Type => JobType.Cron;

    public override string Description => _description;

    public override long MinGapMs { get; }

    /// <summary>Parses the expression and measures its smallest gap from <paramref name="nowMs"/>.</summary>
    /// <exception cref="CronFormatException">The expression can't be read.</exception>
    /// <exception cref="ArgumentException">The expression never runs again.</exception>
    public static CronSchedule Create(string expression, TimeZoneInfo? zone, long nowMs)
    {
        var fields = expression.Split(' ', StringSplitOptions.RemoveEmptyEntries).Length;
        var format = fields == 6 ? CronFormat.IncludeSeconds : CronFormat.Standard;
        var cron = CronExpression.Parse(expression, format);
        var effective = zone ?? TimeZoneInfo.Local;

        var from = DateTimeOffset.FromUnixTimeMilliseconds(nowMs);
        var runs = cron.GetOccurrences(from, from.AddYears(GapSampleYears), effective)
            .Take(GapSamples)
            .Select(run => run.ToUnixTimeMilliseconds())
            .ToList();
        if (runs.Count == 0) throw new ArgumentException($"Cron expression \"{expression}\" never runs in the future");

        var minGapMs = runs.Zip(runs.Skip(1), (earlier, later) => later - earlier).DefaultIfEmpty(long.MaxValue).Min();
        var description = zone is null ? $"cron \"{expression}\"" : $"cron \"{expression}\" ({zone.Id})";
        return new CronSchedule(cron, effective, description, minGapMs);
    }

    public override long? Next(long after) =>
        _cron.GetNextOccurrence(DateTimeOffset.FromUnixTimeMilliseconds(after), _zone)?.ToUnixTimeMilliseconds();

    /// <summary>
    /// Cronos only looks forward, so the previous run is the last one in a window that ends at
    /// <paramref name="atOrBefore"/> and widens until it holds a run.
    /// </summary>
    public override long? Prev(long atOrBefore)
    {
        var end = DateTimeOffset.FromUnixTimeMilliseconds(atOrBefore);
        var windowMs = Math.Clamp(MinGapMs, 1_000, 86_400_000) * 2;

        while (true)
        {
            var start = end - DateTimeOffset.UnixEpoch > TimeSpan.FromMilliseconds(windowMs)
                ? end.AddMilliseconds(-windowMs)
                : DateTimeOffset.UnixEpoch;
            var latest = LastIn(start, end);
            if (latest is not null || start == DateTimeOffset.UnixEpoch) return latest;
            windowMs *= 8;
        }
    }

    private long? LastIn(DateTimeOffset start, DateTimeOffset end)
    {
        DateTimeOffset? last = null;
        foreach (var run in _cron.GetOccurrences(start, end, _zone, fromInclusive: true, toInclusive: true))
        {
            last = run;
        }

        return last?.ToUnixTimeMilliseconds();
    }
}
