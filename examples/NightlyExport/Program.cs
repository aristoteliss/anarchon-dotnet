// Copyright (C) 2026-present Aristotelis — see repository license.

// A long nightly export that survives a deploy: the instance running it is stopped halfway, and
// another instance continues the same run from where it was, not from the start.
//
//   dotnet run --project examples/NightlyExport                  # a SQLite file in the temp folder
//   BACKEND=redis dotnet run --project examples/NightlyExport    # see examples/Shared/Example.cs
//
// Needs a backend with leases (SQLite, PostgreSQL, Redis, NATS Lease): that is what lets a second
// instance take a run over.
//
// The approach, and why:
//   ctx.CancellationToken   checked between pages: when the instance is stopped, the handler throws
//                           there, so the run is handed over instead of counting as finished
//   a checkpoint            the last page written, kept per run (ctx.Slot) in the store the export
//                           writes to; the next attempt reads it and resumes
//   ctx.Attempt             1 for the first try, 2 for the instance that took over
//   Timeout                 a run that hangs is cut off before the next night's run is due
//   Overlap (default)       Skip: if a night's export is still running, the next one doesn't start
//   StopTimeout             on deploy, running jobs get this long to stop before they are left behind
//   Lease, Heartbeat        short here so the takeover is quick to watch; the defaults are 30 s and 5 s
//
// Real life: scheduler.Cron("export-orders", "0 2 * * *", ...). Here the job runs every 30 s and
// the first run is 2 s after start.

using System.Collections.Concurrent;
using Anarchon;
using static Anarchon.Examples.Example;

const int Pages = 10;
var every = TimeSpan.FromSeconds(30);
// Stands in for a table in the export's own database: the last page written, per run.
var checkpoints = new ConcurrentDictionary<long, int>();
var startedAt = DateTimeOffset.UtcNow;
var firstRunAt = startedAt.AddSeconds(2).ToUnixTimeMilliseconds();

Scheduler? runner = null;
Scheduler[] workers = [Worker("worker-a"), Worker("worker-b")];
foreach (var worker in workers)
{
    worker.Started += (_, _) => Interlocked.CompareExchange(ref runner, worker, null);
}

await Task.WhenAll(workers.Select(w => w.StartAsync()));
Say("two workers up; the export starts in 2 s");

await WaitForPageAsync(4);
Say($"deploy: {runner?.InstanceId} is stopped, with 1 s for its job to stop");
await runner!.StopAsync();

await WaitForPageAsync(Pages);
var survivor = runner == workers[0] ? workers[1] : workers[0];
var history = (await survivor.HistoryAsync("export-orders", 10)).Where(run => run.StartedAt >= startedAt).Reverse().ToList();
await survivor.StopAsync();
foreach (var run in history)
{
    Say($"  attempt {run.Attempt} on {run.InstanceId}: {run.Status} ({run.Reason})");
}

// One worker, with its first export at firstRunAt and every 30 s after.
Scheduler Worker(string id)
{
    var scheduler = new Scheduler(new SchedulerOptions
    {
        Backend = Backend("export"),
        InstanceId = id,
        Lease = TimeSpan.FromSeconds(3),
        Heartbeat = TimeSpan.FromMilliseconds(500),
        StopTimeout = TimeSpan.FromSeconds(1),
    });
    scheduler.Every("export-orders", every, ExportOrdersAsync, new()
    {
        Offset = TimeSpan.FromMilliseconds(firstRunAt % (long)every.TotalMilliseconds),
        Timeout = TimeSpan.FromSeconds(25),
        CatchUp = TimeSpan.Zero,
    });
    scheduler.Interrupted += (_, run) => Say($"{run.InstanceId}: stopped mid-run, {(run.HandedOver ? "handed over" : "not handed over")}");
    return scheduler;
}

async Task ExportOrdersAsync(JobContext ctx)
{
    var from = checkpoints.GetValueOrDefault(ctx.Slot);
    Say($"{ctx.InstanceId}: attempt {ctx.Attempt} ({ctx.Reason}) starts at page {from + 1}");
    for (var page = from + 1; page <= Pages; page++)
    {
        await WorkAsync(TimeSpan.FromSeconds(1), ctx.CancellationToken); // fetch the page and write it; both take the token
        checkpoints[ctx.Slot] = page;
        Say($"{ctx.InstanceId}: page {page}/{Pages} written");
    }
}

async Task WaitForPageAsync(int page)
{
    while (checkpoints.Values.DefaultIfEmpty(0).Max() < page)
    {
        await Task.Delay(100);
    }
}
