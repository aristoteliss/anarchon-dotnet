// Copyright (C) 2026-present Aristotelis — see repository license.

// Every job option and every event, on one instance. Runs for 30 s, then prints what happened.
//
//   dotnet run --project examples/OptionsAndEvents                  # a SQLite file in the temp folder
//   BACKEND=redis dotnet run --project examples/OptionsAndEvents    # see examples/Shared/Example.cs
//
// Jobs:
//   tick      every 5 s, the plain case
//   slow      takes 8 s every 5 s, Overlap Skip (the default): runs due meanwhile are skipped
//   parallel  takes 8 s every 5 s, Overlap Allow: runs side by side
//   stuck     never returns, Timeout 2 s: its token is cancelled and the run is recorded as failed
//   flaky     throws half the time: Failed events, and the error in history
//   digest    a cron expression in a time zone (every 10 s here, so it shows up)
//   welcome   a one-off job, 3 s after start
//   manual    never due on its own in this example: started with RunNowAsync

using System.Collections.Concurrent;
using Anarchon;
using static Anarchon.Examples.Example;

var athens = TimeZoneInfo.FindSystemTimeZoneById("Europe/Athens");
await using var scheduler = new Scheduler(new SchedulerOptions
{
    Backend = Backend("options"),
    InstanceId = "api-1",
    TimeZone = athens,
    // Defaults for every job; each job can set its own.
    Jitter = TimeSpan.FromMilliseconds(200),
    HistorySize = 200,
    HistoryTtl = TimeSpan.FromDays(1),
    Lease = TimeSpan.FromSeconds(6),
    Heartbeat = TimeSpan.FromSeconds(1),
    StopTimeout = TimeSpan.FromSeconds(5),
});

AddJobs(scheduler, athens);
var counts = CountEvents(scheduler);

await scheduler.StartAsync();
Say($"started on {scheduler.Backend.Name}");
foreach (var job in scheduler.GetJobs())
{
    Say($"  {job.Name,-9} {job.Type,-5} {job.Schedule,-40} next {job.NextRunAt:HH:mm:ss}");
}

await Task.Delay(TimeSpan.FromSeconds(6));
var record = await scheduler.RunNowAsync("manual");
Say($"RunNowAsync(\"manual\"): {record.Status} in {record.Duration?.TotalMilliseconds:0} ms, manual {record.IsManual}");

await Task.Delay(TimeSpan.FromSeconds(6));
Say($"Remove(\"parallel\"): {scheduler.Remove("parallel")}; its history stays");
var last = await scheduler.LastRunAsync("flaky");
Say($"next \"tick\" at {scheduler.GetNextRun("tick"):HH:mm:ss}; last \"flaky\" was {last?.Status}");

await Task.Delay(TimeSpan.FromSeconds(18));
// "slow" is mid-run here: it throws when its token is cancelled, so it is recorded as interrupted.
await scheduler.StopAsync();
Say("stopped");
foreach (var (key, count) in counts.OrderBy(pair => pair.Key, StringComparer.Ordinal))
{
    Say($"  {key,-28} {count}");
}

await PrintHistoryAsync("stuck");

// One job per option worth seeing: overlap, timeout, retries, time zone, a one-off and a manual job.
static void AddJobs(Scheduler scheduler, TimeZoneInfo zone) => scheduler
    .Every("tick", TimeSpan.FromSeconds(5), ctx =>
    {
        // What a job gets: its scheduled time, which try this is, and why it runs.
        Say($"tick: scheduled {Time(ctx.ScheduledAt)}, attempt {ctx.Attempt}, reason {ctx.Reason}, on {ctx.InstanceId}");
        return Task.CompletedTask;
    })
    .Every("slow", TimeSpan.FromSeconds(5), ctx => WorkAsync(TimeSpan.FromSeconds(8), ctx.CancellationToken))
    .Every("parallel", TimeSpan.FromSeconds(5), ctx => WorkAsync(TimeSpan.FromSeconds(8), ctx.CancellationToken), new() { Overlap = OverlapPolicy.Allow })
    .Every("stuck", TimeSpan.FromSeconds(10), ctx => WorkAsync(TimeSpan.FromMinutes(1), ctx.CancellationToken), new() { Timeout = TimeSpan.FromSeconds(2), Overlap = OverlapPolicy.Allow })
    .Every("flaky", TimeSpan.FromSeconds(5), _ => Random.Shared.Next(2) == 0 ? throw new HttpRequestException("upstream returned 503") : Task.CompletedTask, new() { MaxLate = TimeSpan.FromSeconds(3), MaxAttempts = 2 })
    .Cron("digest", "*/10 * * * * *", _ => Task.Delay(100), new() { TimeZone = zone })
    .At("welcome", DateTimeOffset.UtcNow.AddSeconds(3), _ =>
    {
        Say("welcome: a one-off job, on one instance");
        return Task.CompletedTask;
    })
    .Every("manual", TimeSpan.FromHours(1), _ => Task.Delay(300), new() { CatchUp = TimeSpan.Zero });

// Counts every event per job, and says the ones worth reading as they happen.
static ConcurrentDictionary<string, int> CountEvents(Scheduler scheduler)
{
    var counts = new ConcurrentDictionary<string, int>();
    void Count(string job, string what) => counts.AddOrUpdate($"{job} {what}", 1, (_, n) => n + 1);

    scheduler.Started += (_, e) => Count(e.Job, "started");
    scheduler.Succeeded += (_, run) => Count(run.Job, "succeeded");
    scheduler.Failed += (_, run) =>
    {
        Count(run.Job, "failed");
        Say($"{run.Job} failed after {run.Duration?.TotalMilliseconds:0} ms: {run.Error}");
    };
    scheduler.Skipped += (_, e) =>
    {
        Count(e.Job, $"skipped ({e.Reason})");
        if (e.Reason == SkipReason.Overlap) Say($"{e.Job}: the {Time(e.ScheduledAt)} run is skipped, the previous one is still going");
    };
    scheduler.Interrupted += (_, run) => Count(run.Job, "interrupted");
    scheduler.Lost += (_, e) => Say($"{e.Job}: attempt {e.Attempt} was taken over by a backup");
    scheduler.Error += (_, e) => Say($"backend error: {e.Error.Message}");
    return counts;
}

// Prints a job's latest runs, read from the store by a scheduler that has no jobs.
static async Task PrintHistoryAsync(string job)
{
    // History is read from the store, so any process can ask, with or without the jobs registered.
    await using var reader = new Scheduler(new SchedulerOptions { Backend = Backend("options") });
    foreach (var run in await reader.HistoryAsync(job, 5))
    {
        Say($"  {job} {Time(run.ScheduledAt)} by {run.InstanceId}: {run.Status} after {run.Duration?.TotalMilliseconds ?? 0:0} ms {run.Error}");
    }
}
