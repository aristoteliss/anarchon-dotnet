// Copyright (C) 2026-present Aristotelis — see repository license.

// Failover, in one process and with no server: three instances share one job, and the instance
// running it is taken away twice.
//
//   dotnet run --project examples/Failover                  # a SQLite file in the temp folder
//   BACKEND=redis dotnet run --project examples/Failover    # see examples/Shared/Example.cs
//
// 1. Rolling deploy: the runner is stopped mid-run. The job throws when its token is cancelled, so
//    the run is handed over and another instance starts attempt 2 within two heartbeats.
// 2. Crash: the runner stops renewing its lease without giving the run up (here: a job that
//    ignores its token, stopped with a short StopTimeout). A backup takes over once the lease runs out.
//
// It takes about 40 s and ends with the history of the job.

using Anarchon;
using static Anarchon.Examples.Example;

var interval = TimeSpan.FromSeconds(10);
var jobTime = TimeSpan.FromSeconds(5);
// Short, so the takeover is quick to watch. The defaults are 30 s and 5 s.
var lease = TimeSpan.FromSeconds(3);
var heartbeat = TimeSpan.FromMilliseconds(500);

var ignoreToken = false;
(string By, long Slot)? running = null;
var instances = new[] { "A", "B", "C" }.ToDictionary(id => id, id =>
{
    var scheduler = new Scheduler(new SchedulerOptions
    {
        Backend = Backend("failover"),
        InstanceId = id,
        Lease = lease,
        Heartbeat = heartbeat,
        StopTimeout = TimeSpan.FromMilliseconds(100), // a job still running after this is left behind, as in a crash
    });
    scheduler.Every("report", interval, async ctx =>
    {
        running = (ctx.InstanceId, ctx.Slot);
        if (ignoreToken && ctx.Attempt == 1) await Task.Delay(jobTime, CancellationToken.None);
        else await WorkAsync(jobTime, ctx.CancellationToken);
    }, new() { Jitter = TimeSpan.FromMilliseconds(300), CatchUp = TimeSpan.Zero }); // the example begins with the next run
    Narrate(scheduler);
    return scheduler;
});

await Task.WhenAll(instances.Values.Select(s => s.StartAsync()));
Say($"3 instances on {instances["A"].Backend.Name}; \"report\" runs every {interval.TotalSeconds} s and takes {jobTime.TotalSeconds} s");

var first = await NextRunnerAsync(after: null);
await Task.Delay(1_500);
Say($"--- stopping {first.By} mid-run, like a rolling deploy");
await instances[first.By].StopAsync(); // cancels the job's token; the job throws, so the run is handed over

ignoreToken = true;
var second = await NextRunnerAsync(after: first.Slot);
await Task.Delay(1_500);
Say($"--- {second.By} stops renewing its lease, like a crash; a backup needs up to {(lease + (2 * heartbeat)).TotalMilliseconds} ms");
await instances[second.By].StopAsync(); // the job ignores its token: after StopTimeout it is left behind, unreleased

await Task.Delay(lease + jobTime + TimeSpan.FromSeconds(2));
var survivor = instances.FirstOrDefault(pair => pair.Key != first.By && pair.Key != second.By).Value
    ?? throw new InvalidOperationException("the same instance ran both runs; run the example again");
foreach (var run in await survivor.HistoryAsync("report", 10))
{
    Say($"  {Time(run.ScheduledAt)}  {run.InstanceId}  attempt {run.Attempt}  {run.Reason,-9}  {run.Status,-11}  {run.Duration?.TotalMilliseconds ?? 0,6:0} ms");
}

await survivor.StopAsync();

// Waits until an instance starts a run other than the one at slot `after`.
async Task<(string By, long Slot)> NextRunnerAsync(long? after)
{
    while (running is not { } run || run.Slot == after)
    {
        await Task.Delay(50);
    }

    return running.Value;
}

// Prints what an instance does with the runs: starts, finishes, interruptions, skips and errors.
static void Narrate(Scheduler scheduler)
{
    scheduler.Started += (_, e) => Say($"{e.InstanceId} started the {Time(e.ScheduledAt)} run (attempt {e.Attempt}, {e.Reason})");
    scheduler.Succeeded += (_, run) => Say($"{run.InstanceId} finished it in {run.Duration?.TotalMilliseconds:0} ms");
    scheduler.Interrupted += (_, run) => Say($"{run.InstanceId} was interrupted{(run.HandedOver ? " and handed the run over" : "")}");
    scheduler.Skipped += (_, e) => Say($"{e.InstanceId} skipped the {Time(e.ScheduledAt)} run ({e.Reason})");
    scheduler.Error += (_, e) => Say($"error: {e.Error.Message}");
}
