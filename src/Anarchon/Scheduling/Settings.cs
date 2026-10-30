// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Anarchon.Scheduling;

/// <summary>The options of a scheduler, checked and with their defaults applied.</summary>
internal sealed record SchedulerSettings(
    IBackend Backend,
    string InstanceId,
    object? Data,
    SchedulerDefaults Defaults,
    long LeaseMs,
    long HeartbeatMs,
    TimeSpan StopTimeout,
    ILogger Logger,
    TimeProvider Time);

/// <summary>Checks options and numbers. Every refusal names the option and the value it got.</summary>
internal static class Settings
{
    private static readonly TimeSpan DefaultLease = TimeSpan.FromSeconds(30);
    private const long MaxHeartbeatMs = 5_000;
    private const int HeartbeatsPerLease = 6;

    /// <summary>Checks the options of a scheduler and applies their defaults. Throws on the first problem.</summary>
    public static SchedulerSettings Read(SchedulerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var backend = options.Backend ?? throw new ArgumentException(
            "A scheduler needs a backend, such as Backend = new RedisBackend(...). No store is assumed",
            nameof(options));
        var instanceId = options.InstanceId ?? $"{Environment.MachineName}:{Environment.ProcessId}";
        if (instanceId.Length == 0) throw new ArgumentException("InstanceId must be a non-empty string", nameof(options));
        RefuseLeaseOptions(backend, options);

        var defaults = new SchedulerDefaults(
            NonNegativeMs(nameof(options.Jitter), options.Jitter),
            PositiveMs(nameof(options.MaxLate), options.MaxLate),
            Positive(nameof(options.HistorySize), options.HistorySize),
            options.HistoryTtl is { } ttl ? PositiveTime(nameof(options.HistoryTtl), ttl) : null,
            options.TimeZone);
        var (leaseMs, heartbeatMs) = ReadLeaseTiming(options);
        NonNegativeMs(nameof(options.StopTimeout), options.StopTimeout);

        var logger = options.LoggerFactory?.CreateLogger("Anarchon.Scheduler") ?? NullLogger.Instance;
        var time = options.TimeProvider ?? TimeProvider.System;
        return new SchedulerSettings(backend, instanceId, options.Data, defaults, leaseMs, heartbeatMs, options.StopTimeout, logger, time);
    }

    public static int Positive(string label, int value) =>
        value > 0 ? value : throw new ArgumentOutOfRangeException(label, value, $"{label} must be a positive whole number (got {value})");

    public static int InRange(string label, int value, int min, int max) =>
        value >= min && value <= max
            ? value
            : throw new ArgumentOutOfRangeException(label, value, $"{label} must be a whole number from {min} to {max} (got {value})");

    public static TimeSpan PositiveTime(string label, TimeSpan value)
    {
        PositiveMs(label, value);
        return value;
    }

    public static long PositiveMs(string label, TimeSpan value)
    {
        var ms = (long)value.TotalMilliseconds;
        return ms > 0 ? ms : throw new ArgumentOutOfRangeException(label, value, $"{label} must be at least 1 ms (got {value})");
    }

    public static long NonNegativeMs(string label, TimeSpan value)
    {
        var ms = (long)value.TotalMilliseconds;
        return ms >= 0 ? ms : throw new ArgumentOutOfRangeException(label, value, $"{label} must not be negative (got {value})");
    }

    private static void RefuseLeaseOptions(IBackend backend, SchedulerOptions options)
    {
        if (backend.Runs is not null) return;

        var set = options.Lease is not null ? nameof(options.Lease) : options.Heartbeat is not null ? nameof(options.Heartbeat) : null;
        if (set is null) return;

        throw new AnarchonException(
            AnarchonErrorCode.Unsupported,
            $"{set} needs a backend with leases (such as Redis with the Lease strategy); {backend.Name} has none");
    }

    /// <summary>Reads the lease and the heartbeat, and refuses a heartbeat that would renew a lease fewer than twice.</summary>
    private static (long LeaseMs, long HeartbeatMs) ReadLeaseTiming(SchedulerOptions options)
    {
        var leaseMs = PositiveMs(nameof(options.Lease), options.Lease ?? DefaultLease);
        var heartbeatMs = options.Heartbeat is { } heartbeat
            ? PositiveMs(nameof(options.Heartbeat), heartbeat)
            : Math.Max(1, Math.Min(MaxHeartbeatMs, leaseMs / HeartbeatsPerLease));

        if (heartbeatMs * 2 >= leaseMs)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"Heartbeat ({heartbeatMs} ms) must be under half of Lease ({leaseMs} ms), so a lease is renewed at least twice");
        }

        return (leaseMs, heartbeatMs);
    }
}
