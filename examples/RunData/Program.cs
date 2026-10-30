// Copyright (C) 2026-present Aristotelis — see repository license.

// Run data: what you give a scheduler or a job, which every run reads as ctx.Data. It is for what a
// handler can't take from its surroundings: the tenant it runs for, the permission it runs with, a
// token that must be fresh.
//
//   dotnet run --project examples/RunData        # Ctrl+C stops it
//
// Where the data comes from, from the most general to the most specific:
//
//   1. SchedulerOptions.Data             for every job
//   2. a job's Data                      on top of it; two dictionaries are merged, anything else replaces
//   3. a JobDataProvider                 called when each run starts, so the value is current
//   4. RunNowAsync(name, data)           for one manual run
//
// and SetData, which replaces the scheduler's data while it runs. A job that wants a type of its own
// reads it with ctx.GetData<T>().

using Anarchon;
using static Anarchon.Examples.Example;

var issued = 0;

// 1. For every job of this scheduler.
await using var scheduler = new Scheduler(new SchedulerOptions
{
    Backend = Backend("run_data"),
    Data = Values(("tenant", "acme"), ("plan", "free")),
});

// No data of its own: it gets the scheduler's.
scheduler.Every("usage", TimeSpan.FromSeconds(5), ctx =>
{
    var data = Read(ctx);
    Say($"usage   for {data["tenant"]} on the {data["plan"]} plan");
    return Task.CompletedTask;
});

// 2. Its own keys go on top of the scheduler's: tenant and plan stay, permission is added.
scheduler.Every("export", TimeSpan.FromSeconds(5), ctx =>
{
    var data = Read(ctx);
    Say($"export  for {data["tenant"]}, allowed to {data["permission"]}{(data.TryGetValue("requestedBy", out var by) ? $", asked by {by}" : "")}");
    return Task.CompletedTask;
}, new() { Data = Values(("permission", "reports:export")) });

// 3. A provider is called when each run starts. It gets the run, and may be async. If it throws, the
//    run fails with that error and the handler isn't called.
scheduler.Every("sync", TimeSpan.FromSeconds(5), ctx =>
{
    var data = Read(ctx);
    Say($"sync    with {data["token"]}, started by {data["startedBy"]}");
    return Task.CompletedTask;
}, new()
{
    Data = new JobDataProvider(async run => Values(("token", await FreshTokenAsync(run.Name)), ("startedBy", run.IsManual ? "an operator" : "the schedule"))),
});

await scheduler.StartAsync();
Say($"started on {scheduler.Backend.Name}");

// 4. For this run only, on top of everything else: who asked for it.
await scheduler.RunNowAsync("export", Values(("permission", "admin"), ("requestedBy", "maria")));
await scheduler.RunNowAsync("sync");

using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stop.Cancel();
};

await Task.Delay(TimeSpan.FromSeconds(7));
// The scheduler's data can be replaced while it runs: later runs see the new plan.
Say("the tenant upgraded: SetData(plan = pro)");
scheduler.SetData(Values(("tenant", "acme"), ("plan", "pro")));
await Task.Delay(Timeout.Infinite, stop.Token).ContinueWith(_ => { }, TaskScheduler.Default);
await scheduler.StopAsync();

// Stands for a call to an auth service: a token that is only valid for a short time.
async Task<string> FreshTokenAsync(string job)
{
    await Task.Delay(20);
    return $"{job}-token-{Interlocked.Increment(ref issued)}";
}

static Dictionary<string, object?> Values(params (string Key, object? Value)[] pairs) => pairs.ToDictionary(pair => pair.Key, pair => pair.Value);

static IReadOnlyDictionary<string, object?> Read(JobContext ctx) => ctx.GetData<IReadOnlyDictionary<string, object?>>();
