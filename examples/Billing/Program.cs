// Copyright (C) 2026-present Aristotelis — see repository license.

// Charging customers once a day: a job that must never charge twice, and must still charge when
// every instance was down at the time it was due.
//
//   dotnet run --project examples/Billing                                   # a SQLite file in the temp folder
//   BACKEND=postgres POSTGRES_URL=... dotnet run --project examples/Billing
//
// Needs a backend with leases (SQLite, PostgreSQL, Redis, NATS Lease): MaxAttempts and CatchUp
// don't exist without one. The day is compressed to 10 s so it can be watched.
//
// The options, and why:
//   Cron + TimeZone   charges at 06:00 Athens time all year, whatever zone the servers are in
//   MaxAttempts = 1   a charge whose runner died is NOT started again by a backup: a half-done run
//                     needs a person, not a retry. (The default, 3, suits jobs safe to redo.)
//   CatchUp           after a full outage, the day's run still happens when an instance starts, if
//                     it is at most this late. It covers the latest missed day only: if a longer
//                     outage must not skip a day, have each run charge every day not charged yet
//   Timeout           a run that hangs is cut off and recorded as failed, so tomorrow's can start
//   ctx.Slot          the key of each charge: the same on every instance and every attempt, so the
//                     ledger refuses a second charge for the same day even if one slipped through

using System.Collections.Concurrent;
using Anarchon;
using static Anarchon.Examples.Example;

var day = TimeSpan.FromSeconds(10);
string[] customers = ["acme", "globex", "initech"];
// Stands in for a table with a unique key on (customer, day): INSERT ... ON CONFLICT DO NOTHING.
var ledger = new ConcurrentDictionary<(string Customer, long Day), string>();
var startedAt = DateTimeOffset.UtcNow;

var a = Instance("billing-a");
var b = Instance("billing-b");
await Task.WhenAll(a.StartAsync(), b.StartAsync());
Say("two instances up; each \"day\" is charged by one of them");
await NextSuccessAsync(a, b);
await NextSuccessAsync(a, b);

await Task.WhenAll(a.StopAsync(), b.StopAsync());
var missed = DateTimeOffset.FromUnixTimeMilliseconds(((DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / (long)day.TotalMilliseconds) + 1) * (long)day.TotalMilliseconds);
Say($"outage: both instances down while {Time(missed)} is due");
await Task.Delay(missed - DateTimeOffset.UtcNow + TimeSpan.FromSeconds(3));

await Task.WhenAll(a.StartAsync(), b.StartAsync());
Say("back up: the missed day is caught up by one instance");
await NextSuccessAsync(a, b);
await NextSuccessAsync(a, b);
var history = (await a.HistoryAsync("charge-cards", 20)).Where(run => run.StartedAt >= startedAt).Reverse().ToList();
await Task.WhenAll(a.StopAsync(), b.StopAsync());

var days = ledger.Keys.GroupBy(key => key.Day).ToList();
var even = days.All(charged => charged.Count() == customers.Length);
Say($"ledger: {ledger.Count} charges over {days.Count} days, {(even ? "each customer once a day" : "UNEVEN")}");
foreach (var run in history)
{
    Say($"  {Time(run.ScheduledAt)}  {run.Status,-8} on {run.InstanceId}  ({run.Reason})");
}

Scheduler Instance(string id)
{
    var scheduler = new Scheduler(new SchedulerOptions
    {
        Backend = Backend("billing"),
        InstanceId = id,
        TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Athens"),
    });

    // Real life: "0 6 * * *". Here: every 10 s (six fields, seconds first).
    scheduler.Cron("charge-cards", "*/10 * * * * *", ChargeCardsAsync, new()
    {
        MaxAttempts = 1,
        CatchUp = TimeSpan.FromMinutes(1),
        Timeout = TimeSpan.FromSeconds(8),
    });
    scheduler.Failed += (_, run) => Say($"{run.InstanceId}: charge run failed, needs a look: {run.Error}");
    return scheduler;
}

// Charges every customer for the run's day; the ledger refuses a day charged before.
Task ChargeCardsAsync(JobContext ctx)
{
    var late = ctx.Reason == RunReason.CatchUp
        ? $" (caught up {(DateTimeOffset.UtcNow - ctx.ScheduledAt).TotalMilliseconds:0} ms late: nobody ran it when it was due)"
        : "";
    foreach (var customer in customers)
    {
        ctx.CancellationToken.ThrowIfCancellationRequested();
        var charged = ledger.TryAdd((customer, ctx.Slot), ctx.InstanceId);
        Say($"{ctx.InstanceId}: {customer} for {Time(ctx.ScheduledAt)} {(charged ? "charged" : "already charged")}{late}");
    }

    return Task.CompletedTask;
}

// Waits for the next run that ends well, on either instance.
static Task NextSuccessAsync(params Scheduler[] schedulers)
{
    var done = new TaskCompletionSource();
    foreach (var scheduler in schedulers)
    {
        scheduler.Succeeded += OnSuccess;
    }

    return done.Task.ContinueWith(_ =>
    {
        foreach (var scheduler in schedulers)
        {
            scheduler.Succeeded -= OnSuccess;
        }
    }, TaskScheduler.Default);

    void OnSuccess(object? sender, RunRecord run) => done.TrySetResult();
}
