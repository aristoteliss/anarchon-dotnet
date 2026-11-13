// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

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
    public void a_refused_job_timing_names_the_options_parameter()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });

        var jitter = Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.Every("spread", TimeSpan.FromSeconds(10), Nothing, new() { Jitter = TimeSpan.FromSeconds(5) }));
        var late = Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.Every("late", TimeSpan.FromMinutes(1), Nothing, new() { Jitter = TimeSpan.FromSeconds(5), MaxLate = TimeSpan.FromSeconds(5) }));

        Assert.Equal("options", jitter.ParamName);
        Assert.Equal("options", late.ParamName);
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

    [Fact]
    public void a_backend_checks_the_job_settings_or_the_scheduler_defaults_where_the_job_sets_none()
    {
        var backend = new MemoryBackend();
        var scheduler = new Scheduler(new SchedulerOptions
        {
            Backend = backend,
            Jitter = TimeSpan.FromSeconds(2),
            MaxLate = TimeSpan.FromSeconds(3),
            HistorySize = 9,
            HistoryTtl = TimeSpan.FromHours(2),
        });

        scheduler.Every("own", TimeSpan.FromMinutes(1), Nothing, new() { Jitter = TimeSpan.FromSeconds(5), MaxLate = TimeSpan.FromSeconds(8), HistorySize = 7, HistoryTtl = TimeSpan.FromHours(1) });
        scheduler.Every("defaults", TimeSpan.FromMinutes(1), Nothing);
        scheduler.Every("wide", TimeSpan.FromMinutes(1), Nothing, new() { Jitter = TimeSpan.FromSeconds(10) });
        scheduler.At("once", DateTimeOffset.UtcNow.AddHours(1), Nothing);

        var limits = backend.Limits.ToDictionary(job => job.Name);
        Assert.Equal((5_000d, 8_000d, 7, TimeSpan.FromHours(1), TimeSpan.FromMinutes(1)), Read(limits["own"]));
        Assert.Equal((2_000d, 3_000d, 9, TimeSpan.FromHours(2), TimeSpan.FromMinutes(1)), Read(limits["defaults"]));
        Assert.Equal(11_000d, limits["wide"].MaxLate.TotalMilliseconds);
        Assert.Equal(TimeSpan.MaxValue, limits["once"].MinGap);
    }

    [Fact]
    public void max_attempts_from_1_to_100_and_a_catch_up_of_zero_or_more_are_kept_and_other_values_are_refused()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new LeaseBackendStub() });

        scheduler.Every("one", TimeSpan.FromMinutes(1), Nothing, new() { MaxAttempts = 1, CatchUp = TimeSpan.Zero });
        scheduler.Every("hundred", TimeSpan.FromMinutes(1), Nothing, new() { MaxAttempts = 100 });
        var none = Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.Every("none", TimeSpan.FromMinutes(1), Nothing, new() { MaxAttempts = 0 }));
        var many = Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.Every("many", TimeSpan.FromMinutes(1), Nothing, new() { MaxAttempts = 101 }));
        var past = Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.Every("past", TimeSpan.FromMinutes(1), Nothing, new() { CatchUp = TimeSpan.FromSeconds(-1) }));

        Assert.Equal(["hundred", "one"], scheduler.GetJobs().Select(job => job.Name).Order());
        Assert.Equal("Job \"none\": MaxAttempts", none.ParamName);
        Assert.Equal("Job \"many\": MaxAttempts", many.ParamName);
        Assert.Equal("Job \"past\": CatchUp", past.ParamName);
    }

    [Fact]
    public void a_negative_stop_timeout_is_refused() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new Scheduler(new SchedulerOptions { Backend = new MemoryBackend(), StopTimeout = TimeSpan.FromSeconds(-1) }));

    [Fact]
    public void jitter_and_max_late_must_end_before_the_backend_forgets_a_run()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend { DedupeWindow = TimeSpan.FromSeconds(10) } });

        scheduler.Every("inside", TimeSpan.FromMinutes(1), Nothing, new() { Jitter = TimeSpan.FromSeconds(4), MaxLate = TimeSpan.FromMilliseconds(5_999) });
        var edge = Assert.Throws<AnarchonException>(() => scheduler.Every("edge", TimeSpan.FromMinutes(1), Nothing, new() { Jitter = TimeSpan.FromSeconds(4), MaxLate = TimeSpan.FromSeconds(6) }));

        Assert.Equal(AnarchonErrorCode.UnsafeJob, edge.Code);
        Assert.Contains("Jitter + MaxLate (10000 ms) must be shorter than how long memory remembers a run (10000 ms)", edge.Message, StringComparison.Ordinal);
        Assert.Equal("inside", Assert.Single(scheduler.GetJobs()).Name);
    }

    private static (double JitterMs, double MaxLateMs, int HistorySize, TimeSpan? HistoryTtl, TimeSpan MinGap) Read(JobLimits job) =>
        (job.Jitter.TotalMilliseconds, job.MaxLate.TotalMilliseconds, job.HistorySize, job.HistoryTtl, job.MinGap);

    private static AnarchonErrorCode Refused(Func<object?> act) => Assert.Throws<AnarchonException>(act).Code;

    private static Task Nothing(JobContext ctx) => Task.CompletedTask;
}
