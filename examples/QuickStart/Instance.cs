// Copyright (C) 2026-present Aristotelis — see repository license.

using static Anarchon.Examples.Example;

namespace Anarchon.Examples;

/// <summary>
/// One instance of the service: a process of its own with the job. It reports its events on
/// standard output, one per line, and stops gracefully when it reads "stop" on standard input.
/// </summary>
internal static class Instance
{
    public static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);
    public static readonly TimeSpan JobTime = TimeSpan.FromSeconds(4);
    public static readonly TimeSpan Lease = TimeSpan.FromSeconds(2);
    public static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(500);

    public static async Task RunAsync(string id, string job, long offsetMs)
    {
        await using var scheduler = new Scheduler(new SchedulerOptions
        {
            Backend = Backend("quickstart"),
            InstanceId = id,
            Lease = Lease,
            Heartbeat = Heartbeat,
        });
        // The job stops when its token is cancelled, so a stopped runner hands its run over. The
        // first instances don't catch up the run before they started; a late one does (CatchUp default).
        var catchUp = id.EndsWith("-late", StringComparison.Ordinal) ? (TimeSpan?)null : TimeSpan.Zero;
        scheduler.Every(job, Interval, ctx => WorkAsync(JobTime, ctx.CancellationToken), new()
        {
            Offset = TimeSpan.FromMilliseconds(offsetMs),
            Jitter = TimeSpan.FromMilliseconds(300),
            CatchUp = catchUp,
        });
        Report(scheduler);

        await scheduler.StartAsync();
        while (await Console.In.ReadLineAsync() is { } line && line != "stop")
        {
        }

        await scheduler.StopAsync();
    }

    /// <summary>Writes each event as a line the demo reads: kind|value|value.</summary>
    private static void Report(Scheduler scheduler)
    {
        scheduler.Started += (_, e) => Console.WriteLine($"start|{e.Slot}|{e.Attempt}|{e.Reason}");
        scheduler.Skipped += (_, e) => Console.WriteLine($"skip|{e.Reason}");
        scheduler.Interrupted += (_, run) => Console.WriteLine($"interrupted|{run.HandedOver}");
        scheduler.Succeeded += (_, run) => Console.WriteLine($"success|{run.Duration?.TotalMilliseconds:0}");
        scheduler.Error += (_, e) => Console.WriteLine($"error|{e.Error.Message}");
    }
}
