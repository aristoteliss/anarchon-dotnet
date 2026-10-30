// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.Tests;

public sealed class OptionsTests
{
    [Fact]
    public void a_scheduler_needs_a_backend_none_is_assumed()
    {
        var error = Assert.Throws<ArgumentException>(() => new Scheduler(new SchedulerOptions()));
        Assert.Contains("needs a backend", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void lease_options_are_refused_on_a_backend_without_leases()
    {
        Assert.Equal(AnarchonErrorCode.Unsupported, Refused(() => new Scheduler(new SchedulerOptions { Backend = new MemoryBackend(), Lease = TimeSpan.FromSeconds(10) })));
        Assert.Equal(AnarchonErrorCode.Unsupported, Refused(() => new Scheduler(new SchedulerOptions { Backend = new MemoryBackend(), Heartbeat = TimeSpan.FromSeconds(1) })));

        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });
        Assert.Equal(AnarchonErrorCode.Unsupported, Refused(() => scheduler.Every("a", TimeSpan.FromMinutes(1), Nothing, new() { Overlap = OverlapPolicy.Allow })));
        Assert.Equal(AnarchonErrorCode.Unsupported, Refused(() => scheduler.Cron("b", "* * * * *", Nothing, new() { MaxAttempts = 1 })));
        Assert.Equal(AnarchonErrorCode.Unsupported, Refused(() => scheduler.At("c", DateTimeOffset.UtcNow.AddHours(1), Nothing, new() { CatchUp = TimeSpan.Zero })));
        Assert.Equal(AnarchonErrorCode.Unsupported, Refused(() => scheduler.Every("d", TimeSpan.FromMinutes(1), Nothing, new() { YieldAfterFailure = true })));
        Assert.Empty(scheduler.GetJobs());
    }

    [Fact]
    public void lease_options_are_kept_on_a_backend_with_leases()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new LeaseBackendStub(), Lease = TimeSpan.FromSeconds(10), Heartbeat = TimeSpan.FromSeconds(2) });
        scheduler.Every("a", TimeSpan.FromMinutes(1), Nothing, new() { Overlap = OverlapPolicy.Allow, MaxAttempts = 1, CatchUp = TimeSpan.Zero, YieldAfterFailure = true });

        Assert.Single(scheduler.GetJobs());
    }

    [Fact]
    public void a_heartbeat_that_would_renew_a_lease_fewer_than_twice_is_refused() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new Scheduler(new SchedulerOptions
        {
            Backend = new LeaseBackendStub(),
            Lease = TimeSpan.FromSeconds(10),
            Heartbeat = TimeSpan.FromSeconds(5),
        }));

    [Fact]
    public void job_timings_the_schedule_cannot_keep_are_refused_with_the_job_and_the_values()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });

        var interval = Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.Every("fast", TimeSpan.FromMilliseconds(500), Nothing));
        var jitter = Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.Every("spread", TimeSpan.FromSeconds(10), Nothing, new() { Jitter = TimeSpan.FromSeconds(5) }));
        var late = Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.Every("late", TimeSpan.FromMinutes(1), Nothing, new() { Jitter = TimeSpan.FromSeconds(5), MaxLate = TimeSpan.FromSeconds(5) }));
        Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.Every("history", TimeSpan.FromMinutes(1), Nothing, new() { HistorySize = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.Every("timeout", TimeSpan.FromMinutes(1), Nothing, new() { Timeout = TimeSpan.Zero }));
        Assert.Throws<ArgumentException>(() => scheduler.Every(" ", TimeSpan.FromMinutes(1), Nothing));

        Assert.Contains("the interval must be at least 1s (got 500 ms)", interval.Message, StringComparison.Ordinal);
        Assert.Contains("Job \"spread\": Jitter (5000 ms) must be under half the time between runs (10000 ms)", jitter.Message, StringComparison.Ordinal);
        Assert.Contains("Job \"late\": MaxLate (5000 ms) must be bigger than Jitter (5000 ms)", late.Message, StringComparison.Ordinal);
        Assert.Empty(scheduler.GetJobs());
    }

    [Fact]
    public void the_default_jitter_is_lowered_to_a_quarter_of_a_short_interval()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend(), Jitter = TimeSpan.FromSeconds(5) });

        scheduler.Every("short", TimeSpan.FromSeconds(2), Nothing);

        Assert.Single(scheduler.GetJobs());
    }

    [Fact]
    public void names_are_unique_and_unknown_names_are_reported_with_a_code()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });
        scheduler.Every("sync", TimeSpan.FromSeconds(10), Nothing);

        Assert.Equal(AnarchonErrorCode.JobExists, Refused(() => scheduler.Cron("sync", "* * * * *", Nothing)));
        Assert.Equal(AnarchonErrorCode.UnknownJob, Refused(() => scheduler.GetNextRun("missing")));
        Assert.Equal(AnarchonErrorCode.State, Refused(() => scheduler.RunNowAsync("sync")));
        Assert.True(scheduler.Remove("sync"));
        Assert.False(scheduler.Remove("sync"));
    }

    private static AnarchonErrorCode Refused(Func<object?> act) => Assert.Throws<AnarchonException>(act).Code;

    private static Task Nothing(JobContext ctx) => Task.CompletedTask;
}
