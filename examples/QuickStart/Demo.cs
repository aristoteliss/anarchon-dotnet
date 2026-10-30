// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Diagnostics;
using static Anarchon.Examples.Example;

namespace Anarchon.Examples;

/// <summary>
/// Starts the instances as processes of their own, takes the runner away three times (a graceful
/// stop, a crash, then every process at once), and narrates what the instances report.
/// </summary>
internal sealed class Demo
{
    /// <summary>How long a run is left alone before its runner is stopped or killed.</summary>
    private static readonly TimeSpan MidRun = TimeSpan.FromSeconds(1.5);

    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(60);

    private readonly Lock _gate = new();
    private readonly Dictionary<string, Process> _children = [];
    private readonly TaskCompletionSource _takenOver = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<string> _caughtUp = new(TaskCreationOptions.RunContinuationsAsynchronously);
    // Each run of the demo has a job of its own, so what an earlier run left in the store doesn't show up.
    private readonly string _job = $"report-{Environment.ProcessId}";
    // Every process computes the same grid; this shift puts the first run 2.5 s from now.
    private readonly long _offsetMs = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 2_500) % (long)Instance.Interval.TotalMilliseconds;
    private long? _gracefulSlot;
    private long? _crashSlot;
    private DateTimeOffset _killedAt;
    private string? _takenOverBy;

    public async Task RunAsync()
    {
        Say($"3 processes, one job, {Backend("quickstart").Name}. Lease {Instance.Lease.TotalSeconds}s, heartbeat {Instance.Heartbeat.TotalSeconds}s (defaults: 30s, 5s)");
        foreach (var id in new[] { "instance-1", "instance-2", "instance-3" })
        {
            Start(id);
        }

        try
        {
            await _takenOver.Task.WaitAsync(Patience);
            await DowntimeAsync();
            await PrintHistoryAsync();
        }
        finally
        {
            KillAll();
        }
    }

    /// <summary>Kills every process, waits until the next run is due, and starts a new one that catches it up.</summary>
    private async Task DowntimeAsync()
    {
        Say("--- Downtime: every process is killed, and the next run falls due while none is up");
        KillAll();
        var interval = (long)Instance.Interval.TotalMilliseconds;
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var next = ((now - _offsetMs) / interval * interval) + interval + _offsetMs;
        await Task.Delay(TimeSpan.FromMilliseconds(next - now) + TimeSpan.FromSeconds(1.5));

        Say("a new process starts, with the default CatchUp (5 min)");
        Start("instance-4-late");
        var reason = await _caughtUp.Task.WaitAsync(Patience);
        await Task.Delay(Instance.JobTime + TimeSpan.FromSeconds(1));
        Say($"the run nobody was up for ran once, reason {reason}");
    }

    private async Task PrintHistoryAsync()
    {
        await using var reader = new Scheduler(new SchedulerOptions { Backend = Backend("quickstart") });
        foreach (var run in (await reader.HistoryAsync(_job, 10)).Reverse())
        {
            Say($"  {Time(run.ScheduledAt)}  {run.InstanceId,-16} attempt {run.Attempt}  {run.Reason,-9}  {run.Status}");
        }
    }

    /// <summary>Starts this program again as an instance, and listens to the events it reports.</summary>
    private void Start(string id)
    {
        var self = Environment.ProcessPath!;
        var info = new ProcessStartInfo(self) { RedirectStandardInput = true, RedirectStandardOutput = true, UseShellExecute = false };
        // Run as "dotnet QuickStart.dll": the program is the dll, after the host.
        if (Path.GetFileNameWithoutExtension(self) == "dotnet") info.ArgumentList.Add(typeof(Demo).Assembly.Location);
        foreach (var arg in new[] { "instance", id, _job, _offsetMs.ToString(System.Globalization.CultureInfo.InvariantCulture) })
        {
            info.ArgumentList.Add(arg);
        }

        var process = Process.Start(info) ?? throw new InvalidOperationException($"could not start {id}");
        process.OutputDataReceived += (_, line) => OnReport(id, line.Data);
        process.BeginOutputReadLine();
        lock (_gate)
        {
            _children[id] = process;
        }
    }

    /// <summary>Narrates one event an instance reported, and injects the faults.</summary>
    private void OnReport(string id, string? line)
    {
        var parts = line?.Split('|') ?? [];
        switch (parts)
        {
            case ["start", var slot, var attempt, var reason]:
                OnStart(id, long.Parse(slot, System.Globalization.CultureInfo.InvariantCulture), int.Parse(attempt, System.Globalization.CultureInfo.InvariantCulture), reason);
                break;
            case ["skip", var reason]:
                Say($"  - {id} skipped ({(reason == "Taken" ? "the run is taken by another instance" : reason)})");
                break;
            case ["interrupted", var handedOver]:
                Say($"  x {id} interrupted mid-run{(handedOver == "True" ? " (handed over)" : "")}");
                break;
            case ["success", var ms]:
                Say($"  ok {id} finished in {ms} ms");
                if (id == _takenOverBy) _takenOver.TrySetResult();
                break;
            case ["error", var message]:
                Say($"  error on {id}: {message}");
                break;
        }
    }

    private void OnStart(string id, long slot, int attempt, string reason)
    {
        var afterKill = slot == _crashSlot && attempt > 1 ? $", {(DateTimeOffset.UtcNow - _killedAt).TotalSeconds:0.0}s after the kill" : "";
        Say($"  ok {id} won the {Time(DateTimeOffset.FromUnixTimeMilliseconds(slot))} run (attempt {attempt}, {reason}){afterKill}");
        if (afterKill.Length > 0) _takenOverBy = id;
        if (id.EndsWith("-late", StringComparison.Ordinal)) _caughtUp.TrySetResult(reason);
        else if (attempt == 1) InjectFault(id, slot);
    }

    /// <summary>Stops the first runner gracefully mid-run, and kills the runner of the next new run.</summary>
    private void InjectFault(string id, long slot)
    {
        lock (_gate)
        {
            if (_gracefulSlot is null)
            {
                _gracefulSlot = slot;
                _ = StopLaterAsync(id); // the demo waits on the events, not on this task
                return;
            }

            if (_crashSlot is not null || slot == _gracefulSlot) return;
            _crashSlot = slot;
            _ = KillLaterAsync(id);
        }
    }

    private async Task StopLaterAsync(string id)
    {
        await Task.Delay(MidRun);
        Say($"--- Graceful stop: {id} is told to stop, as on a deploy");
        await Child(id).StandardInput.WriteLineAsync("stop");
    }

    private async Task KillLaterAsync(string id)
    {
        await Task.Delay(MidRun);
        Say($"--- Crash: {id} is killed (like kill -9), waiting for the {Instance.Lease.TotalSeconds}s lease");
        _killedAt = DateTimeOffset.UtcNow;
        Child(id).Kill(entireProcessTree: true);
    }

    private Process Child(string id)
    {
        lock (_gate)
        {
            return _children[id];
        }
    }

    private void KillAll()
    {
        lock (_gate)
        {
            foreach (var process in _children.Values.Where(process => !process.HasExited))
            {
                process.Kill(entireProcessTree: true);
            }
        }
    }
}
