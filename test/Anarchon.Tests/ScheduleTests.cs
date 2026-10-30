// Copyright (C) 2026-present Aristotelis — see repository license.

using Microsoft.Extensions.Time.Testing;

namespace Anarchon.Tests;

public sealed class ScheduleTests
{
    private static readonly TimeZoneInfo Athens = TimeZoneInfo.FindSystemTimeZoneById("Europe/Athens");

    [Fact]
    public void every_instance_computes_the_same_next_run_whenever_it_started()
    {
        var early = SchedulerAt("2026-10-10T12:00:03.250Z");
        var late = SchedulerAt("2026-10-10T12:00:07.900Z");
        foreach (var scheduler in new[] { early, late })
        {
            scheduler.Every("sync", TimeSpan.FromSeconds(10), Nothing);
            scheduler.Every("shifted", TimeSpan.FromHours(1), Nothing, new() { Offset = TimeSpan.FromMinutes(15) });
        }

        Assert.Equal(At("2026-10-10T12:00:10Z"), early.GetNextRun("sync"));
        Assert.Equal(early.GetNextRun("sync"), late.GetNextRun("sync"));
        Assert.Equal(At("2026-10-10T12:15:00Z"), late.GetNextRun("shifted"));
    }

    [Fact]
    public void a_run_exactly_now_is_not_next_the_next_one_is()
    {
        var scheduler = SchedulerAt("2026-10-10T12:00:10Z");
        scheduler.Every("sync", TimeSpan.FromSeconds(10), Nothing);
        scheduler.Cron("poll", "*/10 * * * * *", Nothing);

        Assert.Equal(At("2026-10-10T12:00:20Z"), scheduler.GetNextRun("sync"));
        Assert.Equal(At("2026-10-10T12:00:20Z"), scheduler.GetNextRun("poll"));
    }

    [Fact]
    public void cron_reads_seconds_and_follows_the_time_zone_through_daylight_saving()
    {
        var summer = SchedulerAt("2026-10-10T12:00:00Z");
        summer.Cron("digest", "0 8 * * *", Nothing, new() { TimeZone = Athens });
        summer.Cron("quarter", "*/15 * * * * *", Nothing);

        var winter = SchedulerAt("2026-10-30T12:00:00Z");
        winter.Cron("digest", "0 8 * * *", Nothing, new() { TimeZone = Athens });

        Assert.Equal(At("2026-10-11T05:00:00Z"), summer.GetNextRun("digest"));
        Assert.Equal(At("2026-10-10T12:00:15Z"), summer.GetNextRun("quarter"));
        Assert.Equal(At("2026-10-31T06:00:00Z"), winter.GetNextRun("digest"));
    }

    [Fact]
    public void the_scheduler_time_zone_is_the_default_of_its_cron_jobs()
    {
        var scheduler = SchedulerAt("2026-10-10T12:00:00Z", new SchedulerOptions { TimeZone = Athens });
        scheduler.Cron("digest", "0 8 * * *", Nothing);

        Assert.Equal(At("2026-10-11T05:00:00Z"), scheduler.GetNextRun("digest"));
        Assert.Equal($"cron \"0 8 * * *\" ({Athens.Id})", scheduler.GetJobs().Single().Schedule);
    }

    [Fact]
    public void jobs_describe_their_schedule()
    {
        var scheduler = SchedulerAt("2026-10-10T12:00:00Z");
        scheduler
            .Every("sync", TimeSpan.FromSeconds(10), Nothing)
            .Every("hourly", TimeSpan.FromHours(1), Nothing, new() { Offset = TimeSpan.FromMinutes(15) })
            .Cron("digest", "0 8 * * 1-5", Nothing, new() { TimeZone = Athens })
            .At("launch", At("2026-12-01T09:00:00Z"), Nothing);

        var jobs = scheduler.GetJobs().ToDictionary(job => job.Name);
        Assert.Equal("every 10s", jobs["sync"].Schedule);
        Assert.Equal("every 1h (offset 15m)", jobs["hourly"].Schedule);
        Assert.Equal($"cron \"0 8 * * 1-5\" ({Athens.Id})", jobs["digest"].Schedule);
        Assert.Equal("once at 2026-12-01T09:00:00.000Z", jobs["launch"].Schedule);
        Assert.Equal(JobType.Once, jobs["launch"].Type);
        Assert.Equal(At("2026-12-01T09:00:00Z"), jobs["launch"].NextRunAt);
    }

    [Fact]
    public void a_one_off_job_in_the_past_has_no_next_run() =>
        Assert.Null(SchedulerAt("2026-10-10T12:00:00Z").At("old", At("2026-01-01T00:00:00Z"), Nothing).GetNextRun("old"));

    [Theory]
    [InlineData("not a cron")]
    [InlineData("61 * * * *")]
    [InlineData("0 0 30 2 *")]
    public void an_invalid_cron_or_one_that_never_runs_is_refused(string expression)
    {
        var error = Assert.Throws<ArgumentException>(() => SchedulerAt("2026-10-10T12:00:00Z").Cron("bad", expression, Nothing));
        Assert.Contains("Job \"bad\"", error.Message, StringComparison.Ordinal);
    }

    private static Scheduler SchedulerAt(string now, SchedulerOptions? options = null)
    {
        options ??= new SchedulerOptions();
        options.Backend = new MemoryBackend();
        options.TimeProvider = new FakeTimeProvider(At(now));
        return new Scheduler(options);
    }

    private static DateTimeOffset At(string iso) => DateTimeOffset.Parse(iso, System.Globalization.CultureInfo.InvariantCulture);

    private static Task Nothing(JobContext ctx) => Task.CompletedTask;
}
