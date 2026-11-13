// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Collections.Concurrent;

namespace Anarchon.Tests;

public sealed class SchedulerTests
{
    [Fact]
    public async Task a_manual_run_runs_here_and_shows_in_history_as_manual()
    {
        var backend = new MemoryBackend();
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = backend, InstanceId = "api-1" });
        JobContext? seen = null;
        scheduler.Cron("digest", "0 8 * * *", ctx =>
        {
            seen = ctx;
            return Task.CompletedTask;
        });
        await scheduler.StartAsync();

        var run = await scheduler.RunNowAsync("digest");
        var history = await scheduler.HistoryAsync("digest");

        Assert.Equal(RunStatus.Success, run.Status);
        Assert.True(run.IsManual);
        Assert.Equal(RunReason.Manual, seen?.Reason);
        Assert.Equal("api-1", seen?.InstanceId);
        Assert.Equal((run.Slot, "api-1", true, RunStatus.Success), (history[0].Slot, history[0].InstanceId, history[0].IsManual, history[0].Status));
    }

    [Fact]
    public async Task run_data_layers_the_scheduler_job_and_manual_data_and_providers_are_read_when_a_run_starts()
    {
        var calls = 0;
        JobDataProvider fresh = _ => ValueTask.FromResult<object?>(new Dictionary<string, object?> { ["token"] = $"t{++calls}" });
        await using var scheduler = new Scheduler(new SchedulerOptions
        {
            Backend = new MemoryBackend(),
            Data = new Dictionary<string, object?> { ["tenant"] = "acme", ["format"] = "csv" },
        });
        var seen = new List<IReadOnlyDictionary<string, object?>>();
        scheduler.Every("export", TimeSpan.FromHours(1), ctx =>
        {
            seen.Add(ctx.GetData<IReadOnlyDictionary<string, object?>>());
            return Task.CompletedTask;
        }, new() { Data = fresh });
        await scheduler.StartAsync();

        await scheduler.RunNowAsync("export");
        await scheduler.RunNowAsync("export", new Dictionary<string, object?> { ["format"] = "json" });

        Assert.Equal(["acme", "csv", "t1"], new[] { seen[0]["tenant"], seen[0]["format"], seen[0]["token"] });
        Assert.Equal(["acme", "json", "t2"], new[] { seen[1]["tenant"], seen[1]["format"], seen[1]["token"] });
    }

    [Fact]
    public async Task a_handler_that_throws_fails_the_run_and_a_timeout_cancels_its_token()
    {
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });
        var failures = new ConcurrentBag<RunFailureEvent>();
        scheduler.Failed += (_, e) => failures.Add(e);
        var cancelled = new TaskCompletionSource();
        scheduler.Every("broken", TimeSpan.FromHours(1), _ => throw new InvalidOperationException("upstream is down"));
        scheduler.Every("hung", TimeSpan.FromHours(1), async ctx =>
        {
            ctx.CancellationToken.Register(() => cancelled.TrySetResult());
            await Task.Delay(Timeout.Infinite, CancellationToken.None);
        }, new() { Timeout = TimeSpan.FromMilliseconds(200) });
        await scheduler.StartAsync();

        var broken = await scheduler.RunNowAsync("broken");
        var hung = await scheduler.RunNowAsync("hung");
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal((RunStatus.Failed, "InvalidOperationException: upstream is down"), (broken.Status, broken.Error));
        Assert.Equal((RunStatus.Failed, "TimeoutException: Timed out after 200 ms"), (hung.Status, hung.Error));
        Assert.Contains(failures, e => e.Job == "broken" && e.Cause is InvalidOperationException);
        Assert.Contains(failures, e => e.Job == "hung" && e.Cause is TimeoutException);
    }

    [Fact]
    public async Task stop_cancels_running_jobs_and_waits_for_them()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });
        var started = new TaskCompletionSource();
        var interrupted = new TaskCompletionSource<RunInterruptedEvent>();
        scheduler.Interrupted += (_, e) => interrupted.TrySetResult(e);
        scheduler.Every("export", TimeSpan.FromHours(1), async ctx =>
        {
            started.SetResult();
            await Task.Delay(Timeout.Infinite, ctx.CancellationToken);
        });
        await scheduler.StartAsync();

        var run = scheduler.RunNowAsync("export");
        await started.Task;
        await scheduler.StopAsync();

        Assert.True(run.IsCompleted);
        Assert.Equal(RunStatus.Interrupted, (await run).Status);
        Assert.False((await interrupted.Task).HandedOver);
    }

    [Fact]
    public async Task a_handler_of_an_event_that_throws_is_logged_and_neither_the_run_nor_other_handlers_notice()
    {
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });
        var reached = false;
        scheduler.Succeeded += (_, _) => throw new InvalidOperationException("listener bug");
        scheduler.Succeeded += (_, _) => reached = true;
        scheduler.Every("sync", TimeSpan.FromHours(1), _ => Task.CompletedTask);
        await scheduler.StartAsync();

        var run = await scheduler.RunNowAsync("sync");

        Assert.Equal(RunStatus.Success, run.Status);
        Assert.True(reached);
    }

    [Fact]
    public async Task every_second_three_instances_on_one_store_run_each_slot_once()
    {
        var store = new MemoryBackend.Store();
        var runs = new ConcurrentBag<(string Instance, long Slot)>();
        var skips = new ConcurrentBag<SkipEvent>();
        var schedulers = Enumerable.Range(1, 3).Select(n => new Scheduler(new SchedulerOptions
        {
            Backend = new MemoryBackend(store),
            InstanceId = $"i{n}",
            Jitter = TimeSpan.FromMilliseconds(200),
        })).ToList();
        foreach (var scheduler in schedulers)
        {
            scheduler.Skipped += (_, e) => skips.Add(e);
            scheduler.Every("tick", TimeSpan.FromSeconds(1), ctx =>
            {
                runs.Add((ctx.InstanceId, ctx.Slot));
                return Task.CompletedTask;
            });
        }

        await Task.WhenAll(schedulers.Select(s => s.StartAsync()));
        await Task.Delay(3_500);
        await Task.WhenAll(schedulers.Select(s => s.StopAsync()));

        var slots = runs.GroupBy(run => run.Slot).ToList();
        Assert.InRange(slots.Count, 3, 4);
        Assert.All(slots, slot => Assert.Single(slot));
        // An instance whose claim was still on its way when stop began raises no skip for that run.
        Assert.All(skips, skip => Assert.Equal(SkipReason.Taken, skip.Reason));
        Assert.All(skips, skip => Assert.Contains(skip.Slot, slots.Select(slot => slot.Key)));
        Assert.InRange(skips.Count, (runs.Count - 1) * 2, runs.Count * 2);
    }

    [Fact]
    public async Task a_start_that_fails_while_stop_waits_for_runs_does_not_end_the_stop()
    {
        var backend = new MemoryBackend();
        var scheduler = new Scheduler(new SchedulerOptions { Backend = backend, StopTimeout = TimeSpan.FromMinutes(1) });
        var entered = new TaskCompletionSource();
        var stuck = new TaskCompletionSource();
        scheduler.Cron("slow", "0 8 * * *", async _ =>
        {
            entered.TrySetResult();
            await stuck.Task;
        });
        await scheduler.StartAsync();
        var run = scheduler.RunNowAsync("slow");
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await scheduler.StopAsync(new CancellationToken(canceled: true));

        var connect = new TaskCompletionSource();
        backend.InitGate = connect.Task;
        var start = scheduler.StartAsync();
        using var release = new CancellationTokenSource();
        var stop = scheduler.StopAsync(release.Token);
        await UntilStoppingAsync(scheduler);
        backend.InitFailure = new InvalidOperationException("unreachable");
        connect.SetResult();
        await Assert.ThrowsAsync<InvalidOperationException>(() => start);
        backend.InitGate = null;
        backend.InitFailure = null;

        var refused = await Assert.ThrowsAsync<AnarchonException>(() => scheduler.StartAsync());

        Assert.Equal(AnarchonErrorCode.State, refused.Code);
        await release.CancelAsync();
        await stop;
        stuck.SetResult();
        await run;
    }

    private static async Task UntilStoppingAsync(Scheduler scheduler)
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (true)
        {
            var refused = await Assert.ThrowsAsync<AnarchonException>(() => scheduler.StartAsync());
            if (refused.Message.Contains("stopping", StringComparison.Ordinal)) return;

            await Task.Delay(TimeSpan.FromMilliseconds(10), limit.Token);
        }
    }

    [Fact]
    public async Task a_manual_run_starts_as_manual_and_lasts_from_the_handler_start_to_its_end()
    {
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });
        var started = new TaskCompletionSource<RunStartEvent>();
        scheduler.Started += (_, e) => started.TrySetResult(e);
        scheduler.Every("export", TimeSpan.FromHours(1), _ => Task.Delay(TimeSpan.FromMilliseconds(100)));
        await scheduler.StartAsync();

        var run = await scheduler.RunNowAsync("export");

        Assert.True((await started.Task.WaitAsync(TimeSpan.FromSeconds(5))).IsManual);
        Assert.InRange(run.Duration ?? TimeSpan.Zero, TimeSpan.FromMilliseconds(100), TimeSpan.FromMinutes(1));
    }

    [Fact]
    public async Task an_error_is_kept_as_type_and_message_cut_after_2000_characters()
    {
        const string type = "InvalidOperationException: ";
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });
        scheduler.Every("full", TimeSpan.FromHours(1), _ => throw new InvalidOperationException(new string('a', 2_000 - type.Length)));
        scheduler.Every("over", TimeSpan.FromHours(1), _ => throw new InvalidOperationException(new string('a', 2_001 - type.Length)));
        await scheduler.StartAsync();

        var full = await scheduler.RunNowAsync("full");
        var over = await scheduler.RunNowAsync("over");

        Assert.Equal(type + new string('a', 2_000 - type.Length), full.Error);
        Assert.Equal(type + new string('a', 2_000 - type.Length) + "…", over.Error);
    }

    [Fact]
    public async Task a_job_added_while_the_scheduler_runs_is_scheduled_at_once()
    {
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });
        await scheduler.StartAsync();

        scheduler.Every("late", TimeSpan.FromHours(1), _ => Task.CompletedTask);

        Assert.NotNull(scheduler.GetNextRun("late"));
    }

    [Fact]
    public async Task history_reads_from_1_to_10000_runs_and_refuses_other_limits()
    {
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });

        await scheduler.HistoryAsync("sync", 1);
        await scheduler.HistoryAsync("sync", 10_000);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => scheduler.HistoryAsync("sync", 0));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => scheduler.HistoryAsync("sync", 10_001));
    }

    [Fact]
    public async Task a_read_after_stop_opens_the_backend_for_itself_and_closes_it_again()
    {
        var backend = new MemoryBackend();
        var scheduler = new Scheduler(new SchedulerOptions { Backend = backend });
        await scheduler.StartAsync();
        await scheduler.StopAsync();
        var closes = backend.Closes;

        await scheduler.HistoryAsync("anything");

        Assert.Equal(closes + 1, backend.Closes);
    }
}
