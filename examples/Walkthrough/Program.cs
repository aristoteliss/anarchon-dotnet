// Copyright (C) 2026-present Aristotelis — see repository license.

// For the debugger: one run from the timer to the history record, slowly enough to follow.
//
// In VS Code pick "Example: Walkthrough (step through one run)" in Run and Debug and press F5. It
// stops on Debugger.Break() below. From there use F11 (step into) to enter the library, or set
// breakpoints on the methods listed here and press F5 to jump from one to the next.
//
// The flow of one run, with the file under src/Anarchon/ each step lives in:
//
//   Scheduler.cs                StartAsync()          connects (Backend.InitAsync), then for each job: Schedule()
//   Scheduling/JobTimers.cs     Arm()                 computes the next run time and sets a timer for it, plus a random jitter
//   Scheduling/LeaseRunner.cs   RecoverAsync()        on start: takes over dead runs, catches up a missed run
//   Scheduler.cs                FireAsync()           the timer fired: the run is due
//   Scheduling/LeaseRunner.cs   RunAsync()            asks the backend for the run; exactly one instance gets Won
//                                 the claim           src/Anarchon.Sql/SqlRunStore.cs ClaimAsync(), or the CLAIM script in src/Anarchon.Redis/LeaseScripts.cs
//                               Follow()              Won -> ExecuteLeasedAsync(); Running -> Watch(): this instance is now a backup of that run
//   Scheduling/LeaseRunner.cs   ExecuteLeasedAsync()  starts the heartbeat (Scheduling/HeldLease.cs) that renews the lease, then the executor
//   Scheduling/Executor.cs      ExecuteAsync()        raises Started, calls your handler, then stores the result (FinishAsync, SaveResultAsync)
//   Scheduling/LeaseRunner.cs   Wake()                a backup's timer: every 1 to 2 heartbeats it calls RunAsync(..., Takeover)
//   Scheduler.cs                StopAsync()           DoStopAsync(): cancels the run tokens, waits for running jobs, closes the backend
//
// Two instances (A and B) run in this process, so you see both sides of one claim: the winner in
// ExecuteLeasedAsync() and the other one in Watch(). core.InstanceId tells you which one you are in.
//
// Pausing in a debugger stops the clock for the process but not for the store, so the timings here
// are long on purpose: a 10 minute lease and lateness limit, so a run is not lost or skipped while
// you read. No jitter on A, a fixed one on B, so A always wins.
//
//   dotnet run --project examples/Walkthrough     # without a debugger it just prints the same flow

using System.Diagnostics;
using Anarchon;
using static Anarchon.Examples.Example;

var runs = 0;
var schedulers = new[] { "A", "B" }.Select(id =>
{
    var scheduler = new Scheduler(new SchedulerOptions
    {
        Backend = Backend("walkthrough"),
        InstanceId = id,
        Lease = TimeSpan.FromMinutes(10),
        Heartbeat = TimeSpan.FromMinutes(1),
        MaxLate = TimeSpan.FromMinutes(10),
    });
    scheduler.Every("job", TimeSpan.FromSeconds(10), async ctx =>
    {
        // A breakpoint here stops inside your job. The call stack shows ExecuteAsync <- ExecuteLeasedAsync <- RunAsync <- FireAsync.
        Say($"{ctx.InstanceId}: the job runs (scheduled {Time(ctx.ScheduledAt)}, attempt {ctx.Attempt}, {ctx.Reason})");
        await Task.Delay(500);
        Interlocked.Increment(ref runs);
    }, new() { Jitter = id == "A" ? TimeSpan.Zero : TimeSpan.FromMilliseconds(400), CatchUp = TimeSpan.Zero });
    scheduler.Started += (_, e) => Say($"{e.InstanceId}: event Started");
    scheduler.Succeeded += (_, run) => Say($"{run.InstanceId}: event Succeeded after {run.Duration?.TotalMilliseconds:0} ms");
    scheduler.Skipped += (_, e) => Say($"{e.InstanceId}: event Skipped ({e.Reason}): it is now the backup of that run");
    scheduler.Error += (_, e) => Say($"error: {e.Error.Message}");
    return scheduler;
}).ToList();
var (a, b) = (schedulers[0], schedulers[1]);

// The starting point of the walkthrough; does nothing without a debugger.
if (Debugger.IsAttached) Debugger.Break();
await a.StartAsync(); // F11 here: StartAsync() -> EnsureReadyAsync() -> Schedule() -> Arm(), RecoverAsync()
await b.StartAsync();
Say($"both started on {a.Backend.Name}; the next run is at {a.GetNextRun("job"):HH:mm:ss}");

while (Volatile.Read(ref runs) < 2)
{
    await Task.Delay(100);
}

foreach (var run in await b.HistoryAsync("job", 5))
{
    Say($"  {Time(run.ScheduledAt)} by {run.InstanceId}, attempt {run.Attempt}: {run.Status} in {run.Duration?.TotalMilliseconds ?? 0:0} ms");
}

await Task.WhenAll(a.StopAsync(), b.StopAsync()); // F11 here: StopAsync() -> DoStopAsync()
Say("stopped");
