// Copyright (C) 2026-present Aristotelis — see repository license.

// Refreshing a cache every few seconds on three instances: a cheap job where a missed run doesn't
// matter, because the next one comes soon, but a stale one does.
//
//   dotnet run --project examples/CacheRefresh                  # a SQLite file in the temp folder
//   BACKEND=redis dotnet run --project examples/CacheRefresh    # see examples/Shared/Example.cs
//
// The options, and why:
//   Jitter = 1 s       each instance waits a random 0-1 s before claiming, so the work spreads over
//                      the instances instead of always landing on the fastest one
//   MaxLate = 2 s      a refresh that can't start within 2 s of its time is dropped: a process that
//                      was frozen (GC pause, suspended VM) must not refresh with old inputs
//   CatchUp = 0        nothing to catch up after a restart: the next refresh is 3 s away
//   HistorySize = 50   a frequent job would otherwise keep 1000 runs nobody reads
//   Overlap (default)  a refresh that runs long makes the next one skip (Overlap), so two refreshes
//                      never write the cache at the same time
//
// It runs for 30 s and prints which instance refreshed how often, and what was skipped and why.
// Every 5th refresh is slow (4 s) to show the overlap skip.

using System.Collections.Concurrent;
using Anarchon;
using static Anarchon.Examples.Example;

var refreshes = 0;
var prices = new ConcurrentDictionary<string, decimal>();
var ran = new ConcurrentDictionary<string, int>();
var skipped = new ConcurrentDictionary<SkipReason, int>();

var instances = new[] { "web-1", "web-2", "web-3" }.Select(id =>
{
    var scheduler = new Scheduler(new SchedulerOptions { Backend = Backend("cache"), InstanceId = id });
    scheduler.Every("refresh-prices", TimeSpan.FromSeconds(3), RefreshPricesAsync, new()
    {
        Jitter = TimeSpan.FromSeconds(1),
        MaxLate = TimeSpan.FromSeconds(2),
        CatchUp = TimeSpan.Zero,
        HistorySize = 50,
    });
    scheduler.Started += (_, e) => ran.AddOrUpdate(e.InstanceId, 1, (_, n) => n + 1);
    scheduler.Skipped += (_, e) =>
    {
        skipped.AddOrUpdate(e.Reason, 1, (_, n) => n + 1);
        if (e.Reason != SkipReason.Taken) Say($"{e.InstanceId}: skipped {Time(e.ScheduledAt)} ({e.Reason})");
    };
    return scheduler;
}).ToList();

await Task.WhenAll(instances.Select(s => s.StartAsync()));
Say("three instances, one refresh every 3 s");
await Task.Delay(TimeSpan.FromSeconds(30));
await Task.WhenAll(instances.Select(s => s.StopAsync()));

Say($"refreshes per instance: {string.Join(", ", ran.OrderBy(p => p.Key).Select(p => $"{p.Key} {p.Value}"))}");
Say($"skips: {string.Join(", ", skipped.Select(p => $"{p.Key} {p.Value}"))}");
Say("Taken is the normal case: for every refresh, the two instances that didn't win skip it");

async Task RefreshPricesAsync(JobContext ctx)
{
    var slow = Interlocked.Increment(ref refreshes) % 5 == 0;
    await Task.Delay(slow ? 4_000 : 150, ctx.CancellationToken); // the upstream call
    prices["EURUSD"] = 1.08m + (Random.Shared.Next(100) / 10_000m);
    Say($"{ctx.InstanceId}: prices refreshed{(slow ? " (slow upstream: 4 s)" : "")}");
}
