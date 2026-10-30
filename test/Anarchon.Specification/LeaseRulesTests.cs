// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Collections.Concurrent;
using Anarchon.Backends;

namespace Anarchon.Specification;

/// <summary>
/// The lease rules, step by step, straight on a backend's run store. Every backend with leases
/// derives from it once per store; the scheduler on top of the rules is in <see cref="SchedulerLeaseTests"/>.
/// </summary>
public abstract class LeaseRulesTests(TestStore store)
{
    private const long T = 1_000_000;

    /// <summary>A lease short enough to wait out, long enough to outlive the claims a test sends while it must stay live.</summary>
    private static readonly TimeSpan ShortLease = TimeSpan.FromMilliseconds(400);

    [Fact]
    public async Task state_machine_claim_resend_backup_takeover_fencing_give_up()
    {
        await using var fresh = await FreshAsync("lease");
        var runs = fresh.Runs;
        Task<ClaimOutcome> Claim(long slot, string token, string by, ClaimMode mode = ClaimMode.New) =>
            runs.ClaimAsync(Run(slot, token, by, ShortLease, mode, maxAttempts: 2), CancellationToken.None);

        Assert.Equal(new ClaimOutcome.Won(1), await Claim(T, "a1", "A"));
        Assert.Equal(new ClaimOutcome.Won(1), await Claim(T, "a1", "A"));
        Assert.Equal(new ClaimOutcome.Running(1), await Claim(T, "b1", "B"));
        Assert.Equal(new ClaimOutcome.Overlap(T), await Claim(T + 1, "b2", "B"));
        Assert.True(await runs.HeartbeatAsync("j", T, "a1", ShortLease, CancellationToken.None));

        await Task.Delay(ShortLease + TimeSpan.FromMilliseconds(100)); // A stops renewing: it died
        Assert.Equal([new ActiveRun(T, 1, false)], await runs.ActiveAsync("j", CancellationToken.None));
        Assert.Equal(new ClaimOutcome.Won(2), await Claim(T, "b3", "B", ClaimMode.Takeover));
        Assert.Equal(new ClaimOutcome.Running(2), await Claim(T, "c0", "C", ClaimMode.Takeover));
        Assert.False(await runs.HeartbeatAsync("j", T, "a1", ShortLease, CancellationToken.None));
        Assert.False(await runs.FinishAsync("j", T, "a1", RunStatus.Success, CancellationToken.None));

        await Task.Delay(ShortLease + TimeSpan.FromMilliseconds(100)); // B dies too; two attempts at most
        Assert.Equal(new ClaimOutcome.Done(), await Claim(T, "c1", "C", ClaimMode.Takeover));
        Assert.Equal(new ClaimOutcome.Won(1), await Claim(T + 5, "c2", "C"));
        Assert.True(await runs.FinishAsync("j", T + 5, "c2", RunStatus.Success, CancellationToken.None));
        Assert.Empty(await runs.ActiveAsync("j", CancellationToken.None));
        Assert.Equal(new ClaimOutcome.None(), await Claim(T + 9, "x", "C", ClaimMode.Takeover));

        await fresh.Backend.SaveResultAsync("j", Result(T + 5, "C", 1), 100, null, CancellationToken.None);
        var history = await fresh.Backend.HistoryAsync("j", 10, CancellationToken.None);
        Assert.Equal(
            [(5L, "C", 1, RunStatus.Success), (0L, "B", 2, RunStatus.Interrupted), (0L, "A", 1, RunStatus.Interrupted)],
            history.Select(r => (r.Slot - T, r.InstanceId, r.Attempt, r.Status)));
    }

    [Fact]
    public async Task overlap_allow_lets_runs_of_the_same_job_run_side_by_side()
    {
        await using var fresh = await FreshAsync("allow");

        var first = await fresh.Runs.ClaimAsync(Run(T, "t1", "A", TimeSpan.FromSeconds(5), overlap: OverlapPolicy.Allow), CancellationToken.None);
        var second = await fresh.Runs.ClaimAsync(Run(T + 1, "t2", "A", TimeSpan.FromSeconds(5), overlap: OverlapPolicy.Allow), CancellationToken.None);

        Assert.IsType<ClaimOutcome.Won>(first);
        Assert.IsType<ClaimOutcome.Won>(second);
    }

    [Fact]
    public async Task a_crashed_manual_run_is_never_retried_by_a_backup()
    {
        await using var fresh = await FreshAsync("manual");
        var manual = Run(T, "m", "A", TimeSpan.FromMilliseconds(300), maxAttempts: 1, overlap: OverlapPolicy.Allow) with { Reason = RunReason.Manual };

        Assert.IsType<ClaimOutcome.Won>(await fresh.Runs.ClaimAsync(manual, CancellationToken.None));
        await Task.Delay(400); // the instance running it died
        var takeover = await fresh.Runs.ClaimAsync(Run(T, "b", "B", TimeSpan.FromMilliseconds(300), ClaimMode.Takeover, maxAttempts: 3), CancellationToken.None);

        Assert.Equal(new ClaimOutcome.Done(), takeover);
    }

    [Fact]
    public async Task a_run_handed_over_on_stop_is_taken_over_at_once_and_a_hand_over_is_not_a_failed_attempt()
    {
        await using var fresh = await FreshAsync("handover");
        var runs = fresh.Runs;
        var lease = TimeSpan.FromMinutes(1);
        Task<ClaimOutcome> Takeover(string token) => runs.ClaimAsync(Run(T, token, "B", lease, ClaimMode.Takeover, maxAttempts: 1), CancellationToken.None);

        Assert.IsType<ClaimOutcome.Won>(await runs.ClaimAsync(Run(T, "a", "A", lease, maxAttempts: 1), CancellationToken.None));
        Assert.Equal(new ClaimOutcome.Running(1), await Takeover("b"));
        Assert.True(await runs.ReleaseAsync("j", T, "a", CancellationToken.None));
        Assert.False(await runs.ReleaseAsync("j", T, "a", CancellationToken.None));
        Assert.Equal(new ClaimOutcome.Won(2), await Takeover("b"));
        Assert.False(await runs.HeartbeatAsync("j", T, "a", lease, CancellationToken.None));
        Assert.True(await runs.FinishAsync("j", T, "b", RunStatus.Success, CancellationToken.None));
    }

    [Fact]
    public async Task a_heartbeat_that_arrives_after_a_release_does_not_renew_the_released_lease()
    {
        await using var fresh = await FreshAsync("released");
        var lease = TimeSpan.FromMinutes(1);

        Assert.IsType<ClaimOutcome.Won>(await fresh.Runs.ClaimAsync(Run(T, "a", "A", lease, maxAttempts: 1), CancellationToken.None));
        Assert.True(await fresh.Runs.ReleaseAsync("j", T, "a", CancellationToken.None));

        Assert.False(await fresh.Runs.HeartbeatAsync("j", T, "a", lease, CancellationToken.None));
        Assert.Equal([new ActiveRun(T, 1, false)], await fresh.Runs.ActiveAsync("j", CancellationToken.None));
    }

    [Fact]
    public async Task history_keeps_the_result_of_an_attempt_that_follows_more_than_100_hand_overs()
    {
        await using var fresh = await FreshAsync("handovers");
        var lease = TimeSpan.FromMinutes(1);
        var token = "t1";
        Assert.IsType<ClaimOutcome.Won>(await fresh.Runs.ClaimAsync(Run(T, token, "A", lease, maxAttempts: 1), CancellationToken.None));

        for (var attempt = 2; attempt <= 101; attempt++)
        {
            Assert.True(await fresh.Runs.ReleaseAsync("j", T, token, CancellationToken.None));
            token = $"t{attempt}";
            Assert.Equal(new ClaimOutcome.Won(attempt), await fresh.Runs.ClaimAsync(Run(T, token, "A", lease, ClaimMode.Takeover, maxAttempts: 1), CancellationToken.None));
        }

        Assert.True(await fresh.Runs.FinishAsync("j", T, token, RunStatus.Success, CancellationToken.None));
        await fresh.Backend.SaveResultAsync("j", Result(T, "A", 101), 100, null, CancellationToken.None);
        var last = (await fresh.Backend.HistoryAsync("j", 1, CancellationToken.None)).Single();

        Assert.Equal((101, RunStatus.Success), (last.Attempt, last.Status));
    }

    [Fact]
    public async Task a_new_run_is_refused_when_its_claim_arrives_later_than_max_late_by_the_server_clock()
    {
        await using var fresh = await FreshAsync("late");
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var maxLate = TimeSpan.FromSeconds(10);

        var stale = await fresh.Runs.ClaimAsync(Run(now - 30_000, "x", "A", TimeSpan.FromSeconds(5), maxAttempts: 1) with { MaxLate = maxLate }, CancellationToken.None);
        var recent = await fresh.Runs.ClaimAsync(Run(now - 1_000, "y", "A", TimeSpan.FromSeconds(5), maxAttempts: 1, overlap: OverlapPolicy.Allow) with { MaxLate = maxLate }, CancellationToken.None);

        Assert.True(stale is ClaimOutcome.Late { By.TotalMilliseconds: >= 30_000 }, $"{stale}");
        Assert.IsType<ClaimOutcome.Won>(recent);
    }

    [Fact]
    public async Task on_start_a_dead_run_is_retried_only_if_it_is_still_the_latest_run_of_its_job()
    {
        const long IntervalMs = 10_000;
        // Stay inside one interval for the whole test, so "the latest run" doesn't change under it.
        var toNext = IntervalMs - (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() % IntervalMs);
        if (toNext < 3_000) await Task.Delay(TimeSpan.FromMilliseconds(toNext + 50));
        var latest = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / IntervalMs * IntervalMs;
        var scope = TestStore.NewScope("recover");
        await using (var setup = await FreshAsync("recover", scope))
        {
            foreach (var (slot, token) in new[] { (latest - (3 * IntervalMs), "old"), (latest, "latest") })
            {
                await setup.Runs.ClaimAsync(Run(slot, token, "dead", TimeSpan.FromMilliseconds(200), overlap: OverlapPolicy.Allow), CancellationToken.None);
            }

            await Task.Delay(300); // both runners are gone
        }

        var started = new ConcurrentQueue<(long Slot, int Attempt, RunReason Reason)>();
        await using var scheduler = new Scheduler(new SchedulerOptions
        {
            Backend = store.Create(scope),
            InstanceId = "new",
            Lease = TimeSpan.FromSeconds(1),
            Heartbeat = TimeSpan.FromMilliseconds(200),
        });
        scheduler.Every("j", TimeSpan.FromMilliseconds(IntervalMs), ctx =>
        {
            started.Enqueue((ctx.Slot, ctx.Attempt, ctx.Reason));
            return Task.CompletedTask;
        }, new() { Jitter = TimeSpan.FromMilliseconds(100), CatchUp = TimeSpan.Zero });
        await scheduler.StartAsync();
        await Task.Delay(500);
        await scheduler.StopAsync();

        Assert.Equal([(latest, 2, RunReason.Takeover)], started.Where(run => run.Slot <= latest));
    }

    [Fact]
    public async Task trimming_never_drops_a_run_in_progress_or_one_a_late_claim_could_still_ask_for()
    {
        await using var fresh = await FreshAsync("trim");
        var runs = fresh.Runs;
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var lease = TimeSpan.FromMinutes(1);
        RunClaim Trimmed(long slot, string token, RunReason reason = RunReason.Scheduled) =>
            Run(slot, token, "A", lease, maxAttempts: 1, overlap: OverlapPolicy.Allow) with { HistorySize = 2, Keep = lease, Reason = reason };
        async Task<ClaimOutcome> RunToEnd(long slot, string token, RunReason reason = RunReason.Scheduled)
        {
            var outcome = await runs.ClaimAsync(Trimmed(slot, token, reason), CancellationToken.None);
            if (outcome is ClaimOutcome.Won) await runs.FinishAsync("j", slot, token, RunStatus.Success, CancellationToken.None);
            return outcome;
        }

        Assert.IsType<ClaimOutcome.Won>(await runs.ClaimAsync(Trimmed(now - 400_000, "long"), CancellationToken.None)); // a long run
        await RunToEnd(now - 300_000, "old1");
        await RunToEnd(now - 200_000, "old2");
        Assert.IsType<ClaimOutcome.Won>(await RunToEnd(now, "due"));
        for (var i = 1; i <= 5; i++)
        {
            Assert.IsType<ClaimOutcome.Won>(await RunToEnd(now + i, $"m{i}", RunReason.Manual));
        }

        Assert.True(await runs.HeartbeatAsync("j", now - 400_000, "long", lease, CancellationToken.None), "the long run still owns its lease");
        // Another instance's claim for the due run arrives late, but within MaxLate: it must not run it again.
        var late = await runs.ClaimAsync(Trimmed(now, "late") with { MaxLate = lease }, CancellationToken.None);
        Assert.Equal(new ClaimOutcome.Done(), late);
    }

    private static RunClaim Run(
        long slot,
        string token,
        string by,
        TimeSpan lease,
        ClaimMode mode = ClaimMode.New,
        int maxAttempts = 3,
        OverlapPolicy overlap = OverlapPolicy.Skip) => new()
        {
            Job = "j",
            Slot = slot,
            Token = token,
            InstanceId = by,
            HistorySize = 100,
            Mode = mode,
            Reason = mode == ClaimMode.Takeover ? RunReason.Takeover : RunReason.Scheduled,
            Lease = lease,
            Overlap = overlap,
            MaxAttempts = maxAttempts,
        };

    private static StoredResult Result(long slot, string by, int attempt) => new()
    {
        Slot = slot,
        By = by,
        Attempt = attempt,
        Status = RunStatus.Success,
        StartedAt = DateTimeOffset.FromUnixTimeMilliseconds(1),
        FinishedAt = DateTimeOffset.FromUnixTimeMilliseconds(2),
        Duration = TimeSpan.FromMilliseconds(1),
    };

    /// <summary>A started backend on a scope of its own (or the one given), with its run store.</summary>
    private async Task<LeaseBackend> FreshAsync(string test, string? scope = null)
    {
        await store.RequireAsync();
        var backend = store.Create(scope ?? TestStore.NewScope(test));
        await backend.InitAsync(CancellationToken.None);
        return new LeaseBackend(backend);
    }

    private sealed class LeaseBackend(IBackend backend) : IAsyncDisposable
    {
        public IBackend Backend => backend;

        public IRunStore Runs => backend.Runs ?? throw new InvalidOperationException($"{backend.Name} has no leases");

        public async ValueTask DisposeAsync() => await backend.CloseAsync();
    }
}
