// Copyright (C) 2026-present Aristotelis — see repository license.

// Jobs declared with [Cron], [Every] and [At] on a class, without a host: Register(object) adds
// every job of an object, all or none. With a host, AddJobs<T>() does the same and gives each run its
// own service scope: see examples/Hosting.
//
//   dotnet run --project examples/Attributes                  # a SQLite file in the temp folder
//   BACKEND=redis dotnet run --project examples/Attributes    # see examples/Shared/Example.cs
//
// What it shows:
//   attribute options          Name, Timeout, MaxAttempts, CatchUp, Jitter: text durations ("30s", "1h30m")
//   method shapes              a job method returns Task, ValueTask or nothing, and takes any of
//                              JobContext and CancellationToken
//   RegisterOptions.Prefix     several objects of one class, each with jobs of its own: one per region
//   RegisterOptions.TimeZone   the default zone of the cron jobs of that object
//   RegisterOptions.Defaults   options every job of the object starts from; an attribute sets its own
//   run data                   a job reads ctx.GetData<T>(): here the region the object was made for

using Anarchon;
using static Anarchon.Examples.Example;

await using var scheduler = new Scheduler(new SchedulerOptions { Backend = Backend("attributes"), InstanceId = "worker-1" });

// One object, plain names: "Reports-Daily", "refresh-cache", "warm-up".
scheduler.Register(new Reports());

// One object per region: "eu.export", "us.export". Each gets the region's zone and data.
foreach (var region in new[] { new Region("eu", "Europe/Athens"), new Region("us", "America/New_York") })
{
    scheduler.Register(new RegionalExport(), new RegisterOptions
    {
        Prefix = $"{region.Name}.",
        TimeZone = TimeZoneInfo.FindSystemTimeZoneById(region.TimeZone),
        Defaults = new() { Data = region, CatchUp = TimeSpan.Zero, HistorySize = 100 },
    });
}

scheduler.Started += (_, e) => Say($"{e.Job} starts ({e.Reason})");
scheduler.Failed += (_, run) => Say($"{run.Job} failed: {run.Error}");
await scheduler.StartAsync();
foreach (var job in scheduler.GetJobs())
{
    Say($"  {job.Name,-16} {job.Schedule,-40} next {job.NextRunAt:HH:mm:ss}");
}

await Task.Delay(TimeSpan.FromSeconds(12));
await scheduler.StopAsync();

/// <summary>Plain jobs: the name is Type-Method unless the attribute gives one.</summary>
internal sealed class Reports
{
    private int _refreshes;

    [Cron("0 6 * * *", TimeZone = "Europe/Athens", MaxAttempts = 1, Timeout = "20m")]
    public Task Daily(JobContext ctx) => Task.Delay(100 + _refreshes, ctx.CancellationToken);

    [Every("3s", Name = "refresh-cache", Jitter = "200ms", CatchUp = "0s")]
    public async ValueTask RefreshAsync(CancellationToken cancellationToken)
    {
        await Task.Delay(50, cancellationToken);
        Say($"refresh-cache: done ({Interlocked.Increment(ref _refreshes)} so far on this object)");
    }

    [At("2026-01-01T00:00:00Z", Name = "warm-up")]
    public void WarmUp() => Say($"warm-up: a one-off job that was due long ago never runs again ({_refreshes})");
}

/// <summary>Jobs that run once per region: the object is registered once per region with a prefix.</summary>
internal sealed class RegionalExport
{
    private int _exports;

    [Every("4s", Name = "export", Timeout = "3s")]
    public Task ExportAsync(JobContext ctx)
    {
        var region = ctx.GetData<Region>();
        var local = TimeZoneInfo.ConvertTime(ctx.ScheduledAt, TimeZoneInfo.FindSystemTimeZoneById(region.TimeZone));
        Say($"{ctx.Name}: export {Interlocked.Increment(ref _exports)} for {region.Name}, {local:HH:mm:ss} local time");
        return Task.CompletedTask;
    }
}

/// <summary>What each regional object runs for.</summary>
internal sealed record Region(string Name, string TimeZone);
