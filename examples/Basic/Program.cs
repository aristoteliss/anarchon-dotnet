// Copyright (C) 2026-present Aristotelis — see repository license.

// Start this in three terminals: each run of "sync" runs on one of them. Stop the one running
// "report" with Ctrl+C mid-run: it hands the run over, and another one finishes it as attempt 2.
//
//   dotnet run --project examples/Basic                    # a SQLite file in the temp folder
//   BACKEND=redis dotnet run --project examples/Basic      # see examples/Shared/Example.cs

using Anarchon;
using Microsoft.Extensions.Logging;
using static Anarchon.Examples.Example;

using var logging = LoggerFactory.Create(builder => builder.AddSimpleConsole(o => o.SingleLine = true).AddFilter("Anarchon", LogLevel.Debug));

await using var scheduler = new Scheduler(new SchedulerOptions
{
    Backend = Backend("basic"),
    InstanceId = $"{Environment.MachineName}:{Environment.ProcessId}",
    Lease = TimeSpan.FromSeconds(6),
    Heartbeat = TimeSpan.FromSeconds(1),
    LoggerFactory = logging,
});

scheduler
    .Every("sync", TimeSpan.FromSeconds(5), async ctx =>
    {
        Console.WriteLine($"sync for {ctx.ScheduledAt:HH:mm:ss} on {ctx.InstanceId}");
        await Task.Delay(500, ctx.CancellationToken);
    })
    .Every("report", TimeSpan.FromMinutes(1), async ctx =>
    {
        Console.WriteLine($"report for {ctx.ScheduledAt:HH:mm}: attempt {ctx.Attempt} ({ctx.Reason}), 20 s of work; press Ctrl+C to hand it over");
        for (var step = 1; step <= 20; step++)
        {
            await Task.Delay(1_000, ctx.CancellationToken); // throws when stopped: the run is handed over
        }

        Console.WriteLine("report done");
    }, new() { Timeout = TimeSpan.FromMinutes(2) });

scheduler.Skipped += (_, e) => Console.WriteLine($"{e.Job} for {e.ScheduledAt:HH:mm:ss}: skipped here ({e.Reason})");
scheduler.Interrupted += (_, e) => Console.WriteLine($"{e.Job}: stopped mid-run, handed over: {e.HandedOver}");

using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stop.Cancel();
};

await scheduler.StartAsync();
Console.WriteLine($"{scheduler.InstanceId} runs {string.Join(", ", scheduler.GetJobs().Select(job => $"{job.Name} ({job.Schedule})"))}. Ctrl+C stops it.");
await Task.Delay(Timeout.Infinite, stop.Token).ContinueWith(_ => { }, TaskScheduler.Default);
await scheduler.StopAsync();

foreach (var run in await scheduler.HistoryAsync("report", 5))
{
    Console.WriteLine($"report {run.ScheduledAt:HH:mm} {run.InstanceId} attempt {run.Attempt}: {run.Status}");
}
