// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Collections.Concurrent;
using Anarchon.Backends;

namespace Anarchon.Specification;

/// <summary>
/// Several schedulers on one store, as instances of one service: failover, hand-over and catch-up
/// end to end. Every backend with leases derives from it once.
/// </summary>
public abstract class SchedulerLeaseTests(TestStore store)
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task stop_hands_a_running_job_to_another_instance_when_the_job_throws_on_cancel()
    {
        var scope = await ScopeAsync("handover");
        var a = Create(scope, "A");
        var b = Create(scope, "B");
        var handedOver = new TaskCompletionSource<RunInterruptedEvent>();
        var takenOver = new TaskCompletionSource<RunStartEvent>();
        var due = DateTimeOffset.UtcNow.AddMilliseconds(500);
        a.Interrupted += (_, e) => handedOver.TrySetResult(e);
        b.Started += (_, e) => takenOver.TrySetResult(e);
        var startedOnA = new TaskCompletionSource();
        a.At("export", due, async ctx =>
        {
            startedOnA.TrySetResult();
            await Task.Delay(Timeout.Infinite, ctx.CancellationToken);
        }, new() { Jitter = TimeSpan.Zero });
        b.At("export", due, _ => Task.CompletedTask, new() { Jitter = TimeSpan.Zero });

        await a.StartAsync();
        await startedOnA.Task.WaitAsync(Patience);
        await b.StartAsync();
        await a.StopAsync();
        var takeover = await takenOver.Task.WaitAsync(Patience);
        await Task.Delay(300);
        var history = await b.HistoryAsync("export");
        await b.StopAsync();

        Assert.True((await handedOver.Task).HandedOver);
        Assert.Equal(("B", 2, RunReason.Takeover), (takeover.InstanceId, takeover.Attempt, takeover.Reason));
        Assert.Equal([("B", 2, RunStatus.Success), ("A", 1, RunStatus.Interrupted)], history.Select(r => (r.InstanceId, r.Attempt, r.Status)));
    }

    [Fact]
    public async Task a_job_still_running_when_the_stop_timeout_passes_is_taken_over_once_its_lease_runs_out()
    {
        var scope = await ScopeAsync("timeout");
        var a = Create(scope, "A", stopTimeout: TimeSpan.FromMilliseconds(100));
        var b = Create(scope, "B");
        var startedOnA = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        var takenOver = new TaskCompletionSource<RunStartEvent>();
        b.Started += (_, e) => takenOver.TrySetResult(e);
        var due = DateTimeOffset.UtcNow.AddMilliseconds(500);
        a.At("stubborn", due, async _ =>
        {
            startedOnA.TrySetResult();
            await release.Task; // ignores its token
        }, new() { Jitter = TimeSpan.Zero });
        b.At("stubborn", due, _ => Task.CompletedTask, new() { Jitter = TimeSpan.Zero });

        await a.StartAsync();
        await startedOnA.Task.WaitAsync(Patience);
        await b.StartAsync();
        var stoppedAt = DateTimeOffset.UtcNow;
        await a.StopAsync();
        var takeover = await takenOver.Task.WaitAsync(Patience);
        var waited = DateTimeOffset.UtcNow - stoppedAt;
        release.SetResult();
        await b.StopAsync();

        Assert.Equal((2, RunReason.Takeover), (takeover.Attempt, takeover.Reason));
        Assert.True(waited >= TimeSpan.FromMilliseconds(800), $"taken over after {waited.TotalMilliseconds} ms, before the lease ran out");
    }

    [Fact]
    public async Task an_instance_whose_heartbeats_never_answer_loses_its_run_by_the_time_a_backup_takes_it_over()
    {
        var scope = await ScopeAsync("lost");
        var a = new Scheduler(Options(new SilentHeartbeats(store.Create(scope)), "A"));
        var b = Create(scope, "B");
        var lost = new TaskCompletionSource<LostEvent>();
        var startedOnA = new TaskCompletionSource();
        var cancelledOnA = new TaskCompletionSource();
        var takenOver = new TaskCompletionSource<RunStartEvent>();
        a.Lost += (_, e) => lost.TrySetResult(e);
        b.Started += (_, e) => takenOver.TrySetResult(e);
        var due = DateTimeOffset.UtcNow.AddMilliseconds(500);
        a.At("frozen", due, async ctx =>
        {
            ctx.CancellationToken.Register(() => cancelledOnA.TrySetResult());
            startedOnA.TrySetResult();
            await Task.Delay(Timeout.Infinite, ctx.CancellationToken);
        }, new() { Jitter = TimeSpan.Zero });
        b.At("frozen", due, _ => Task.CompletedTask, new() { Jitter = TimeSpan.Zero });

        await a.StartAsync();
        await startedOnA.Task.WaitAsync(Patience);
        await b.StartAsync();
        var lostEvent = await lost.Task.WaitAsync(Patience);
        await cancelledOnA.Task.WaitAsync(Patience);
        var takeover = await takenOver.Task.WaitAsync(Patience);
        await Task.WhenAll(a.StopAsync(), b.StopAsync());

        Assert.Equal(("frozen", 1, "A"), (lostEvent.Job, lostEvent.Attempt, lostEvent.InstanceId));
        Assert.Equal((2, RunReason.Takeover), (takeover.Attempt, takeover.Reason));
    }

    [Fact]
    public async Task a_run_missed_while_every_instance_was_down_is_caught_up_once_on_start()
    {
        var scope = await ScopeAsync("catchup");
        var runs = new ConcurrentBag<(string Instance, RunReason Reason)>();
        var skips = new ConcurrentBag<SkipEvent>();
        var missed = DateTimeOffset.UtcNow.AddSeconds(-2);
        var schedulers = new[] { Create(scope, "A"), Create(scope, "B") };
        foreach (var scheduler in schedulers)
        {
            scheduler.Skipped += (_, e) => skips.Add(e);
            scheduler.At("report", missed, ctx =>
            {
                runs.Add((ctx.InstanceId, ctx.Reason));
                return Task.CompletedTask;
            }, new() { Jitter = TimeSpan.FromMilliseconds(100) });
        }

        await Task.WhenAll(schedulers.Select(s => s.StartAsync()));
        await Task.Delay(1_000);
        await Task.WhenAll(schedulers.Select(s => s.StopAsync()));

        var run = Assert.Single(runs);
        Assert.Equal(RunReason.CatchUp, run.Reason);
        Assert.Empty(skips);
    }

    [Fact]
    public async Task every_second_three_instances_run_each_slot_once()
    {
        var scope = await ScopeAsync("cluster");
        var starts = new ConcurrentBag<RunStartEvent>();
        var schedulers = new[] { "A", "B", "C" }.Select(id => Create(scope, id)).ToList();
        foreach (var scheduler in schedulers)
        {
            scheduler.Started += (_, e) => starts.Add(e);
            scheduler.Every("tick", TimeSpan.FromSeconds(1), _ => Task.Delay(50), new() { Jitter = TimeSpan.FromMilliseconds(200), CatchUp = TimeSpan.Zero });
        }

        await Task.WhenAll(schedulers.Select(s => s.StartAsync()));
        await Task.Delay(3_500);
        await Task.WhenAll(schedulers.Select(s => s.StopAsync()));

        var slots = starts.GroupBy(e => e.Slot).ToList();
        Assert.InRange(slots.Count, 3, 4);
        Assert.All(slots, slot => Assert.Single(slot));
        Assert.All(starts, e => Assert.Equal((1, RunReason.Scheduled), (e.Attempt, e.Reason)));
    }

    [Fact]
    public async Task a_run_in_progress_shows_as_running_then_as_success()
    {
        var scope = await ScopeAsync("progress");
        await using var scheduler = Create(scope, "A");
        var started = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        scheduler.Every("slow", TimeSpan.FromHours(1), async _ =>
        {
            started.SetResult();
            await release.Task;
        }, new() { CatchUp = TimeSpan.Zero });
        await scheduler.StartAsync();

        var run = scheduler.RunNowAsync("slow");
        await started.Task.WaitAsync(Patience);
        var during = await scheduler.LastRunAsync("slow");
        release.SetResult();
        await run;
        var after = await scheduler.LastRunAsync("slow");

        Assert.Equal((RunStatus.Running, true), (during?.Status, during?.IsManual));
        Assert.Equal(RunStatus.Success, after?.Status);
        Assert.Equal(during?.Slot, after?.Slot);
    }

    [Fact]
    public async Task history_keeps_one_record_per_attempt_and_a_small_history_size_is_fine()
    {
        var scope = await ScopeAsync("small");
        await using var scheduler = Create(scope, "api-1");
        // With leases, history may be shorter than the time a late claim can take.
        scheduler.Every("job", TimeSpan.FromSeconds(1), _ => Task.CompletedTask, new() { HistorySize = 3, Jitter = TimeSpan.Zero, CatchUp = TimeSpan.Zero });
        await scheduler.StartAsync();

        for (var i = 0; i < 5; i++)
        {
            await scheduler.RunNowAsync("job");
        }

        var history = await scheduler.HistoryAsync("job", 20);
        await scheduler.StopAsync();

        // The job is also due every second, so a scheduled run can be among them, and still running.
        Assert.InRange(history.Count, 3, 6);
        Assert.All(history, record => Assert.Equal("api-1", record.InstanceId));
        Assert.All(history.Where(record => record.IsManual), record => Assert.Equal(RunStatus.Success, record.Status));
    }

    /// <summary>Skips the test when the store is down, and gives it a scope of its own.</summary>
    private async Task<string> ScopeAsync(string test)
    {
        await store.RequireAsync();
        return TestStore.NewScope(test);
    }

    private Scheduler Create(string scope, string instanceId, TimeSpan? stopTimeout = null)
    {
        var options = Options(store.Create(scope), instanceId);
        options.StopTimeout = stopTimeout ?? options.StopTimeout;
        return new Scheduler(options);
    }

    /// <summary>A lease short enough for a test to wait out: a dead runner is replaced within about 1.5 seconds.</summary>
    private static SchedulerOptions Options(IBackend backend, string instanceId) => new()
    {
        Backend = backend,
        InstanceId = instanceId,
        Lease = TimeSpan.FromSeconds(1),
        Heartbeat = TimeSpan.FromMilliseconds(200),
    };
}
