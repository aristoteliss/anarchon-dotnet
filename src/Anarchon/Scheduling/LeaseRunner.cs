// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Scheduling;

/// <summary>A run this instance claimed and now executes.</summary>
internal sealed record LeasedRun(
    Job Job,
    long Slot,
    string Token,
    long ClaimedAt,
    int Attempt,
    RunReason Reason,
    object? Data,
    CancellationToken Stop);

/// <summary>
/// Runs on backends with leases. The winner of a run renews a lease while the job runs; every other
/// instance becomes its backup and takes the run over when the lease runs out. Also: no overlapping
/// runs, catch-up on start, and hand-over on stop.
/// </summary>
internal sealed class LeaseRunner(ISchedulerCore core, Executor executor, IRunStore runs)
{
    private const int MaxManualTries = 10;

    private readonly Lock _gate = new();

    /// <summary>Runs another instance is executing, that this instance is ready to take over.</summary>
    private readonly Dictionary<(string Job, long Slot), Backup> _watchers = [];

    /// <summary>Leased runs this instance is executing right now; never watched, since they are known to be alive.</summary>
    private readonly Dictionary<(string Job, long Slot), HeldLease> _localRuns = [];

    /// <summary>
    /// Called on start and when a job is added: picks up what other instances left behind. Unfinished
    /// runs whose runner died are taken over, live ones are watched, and the latest run is caught up
    /// if it was due while every instance was down.
    /// </summary>
    public async Task RecoverAsync(Job job)
    {
        var stop = core.StopToken;

        try
        {
            await core.EnsureReadyAsync().ConfigureAwait(false);
            var latest = job.Schedule.Prev(core.Now());
            var active = await runs.ActiveAsync(job.Name, CancellationToken.None).ConfigureAwait(false);
            foreach (var run in active)
            {
                if (stop.IsCancellationRequested || !core.IsRunning || job.Removed) return;
                if (run.IsAlive) Watch(job, run.Slot);
                // A dead run is retried only while it is the job's latest run; an older one is
                // superseded (it is marked interrupted when the next run starts).
                else if (latest is null || run.Slot >= latest) _ = core.Track(RunAsync(job, run.Slot, RunReason.Takeover, stop));
            }
        }
        catch (Exception error)
        {
            if (!stop.IsCancellationRequested) core.OnError(error, job.Name);
        }

        await CatchUpAsync(job, stop).ConfigureAwait(false);
    }

    /// <summary>Runs the latest run if it was due at most the job's CatchUp ago, at a random moment so instances don't all try at once.</summary>
    private async Task CatchUpAsync(Job job, CancellationToken stop)
    {
        if (job.CatchUpMs == 0) return;

        var now = core.Now();
        var previous = job.Schedule.Prev(now);
        if (previous is not { } slot || now - slot > job.CatchUpMs) return;

        await Delay(job.RandomJitterMs(), stop).ConfigureAwait(false);
        if (!stop.IsCancellationRequested && core.IsRunning && !job.Removed)
        {
            await RunAsync(job, slot, RunReason.CatchUp, stop).ConfigureAwait(false);
        }
    }

    /// <summary>Claims a run (or the next attempt of a dead one) and runs it if this instance wins. Never throws.</summary>
    public async Task RunAsync(Job job, long slot, RunReason reason, CancellationToken stop)
    {
        if (reason == RunReason.Scheduled) await YieldIfFailedAsync(job, slot, stop).ConfigureAwait(false);
        var input = Claims.Input(core.InstanceId, job, slot);
        var claimedAt = core.Now();
        var outcome = await ClaimAsync(job, input, reason, stop).ConfigureAwait(false);
        if (outcome is null) return;

        if (stop.IsCancellationRequested)
        {
            if (outcome is ClaimOutcome.Won && !core.IsIdle && stop == core.StopToken) await ReleaseQuietlyAsync(input).ConfigureAwait(false);
            return;
        }

        if (outcome is not ClaimOutcome.Won won)
        {
            Follow(job, slot, reason, outcome);
            return;
        }

        Unwatch(job, slot);
        var startedAs = won.Attempt > 1 ? RunReason.Takeover : reason;
        await ExecuteLeasedAsync(new LeasedRun(job, slot, input.Token, claimedAt, won.Attempt, startedAs, null, stop)).ConfigureAwait(false);
    }

    /// <summary>RunNowAsync(): a run of its own, never retried by a backup and allowed to overlap.</summary>
    public async Task<RunRecord> RunManualAsync(Job job, object? data, CancellationToken stop)
    {
        var token = Guid.NewGuid().ToString();
        var first = core.Now();

        for (var slot = first; slot < first + MaxManualTries; slot++)
        {
            if (stop.IsCancellationRequested)
            {
                var stopped = new Execution(1, RunReason.Manual, stop) { Data = data };
                return await executor.ExecuteAsync(job, slot, stopped, claimed: false).ConfigureAwait(false);
            }

            var claimedAt = core.Now();
            var outcome = await ClaimManualAsync(job, slot, token).ConfigureAwait(false);
            if (outcome is ClaimOutcome.Won)
            {
                return await ExecuteLeasedAsync(new LeasedRun(job, slot, token, claimedAt, 1, RunReason.Manual, data, stop)).ConfigureAwait(false);
            }
        }

        throw new AnarchonException(AnarchonErrorCode.Store, $"Could not start a manual run of \"{job.Name}\"");
    }

    /// <summary>Stops backing up every run, or the runs of one job.</summary>
    public void StopWatching(string? jobName = null)
    {
        lock (_gate)
        {
            var keys = _watchers.Keys.Where(key => jobName is null || key.Job == jobName).ToList();
            foreach (var key in keys)
            {
                _watchers[key].Timer?.Dispose();
                _watchers.Remove(key);
            }
        }
    }

    /// <summary>Runs still going here are left to their lease: it runs out, and a backup takes them over.</summary>
    public void StopRenewing()
    {
        List<HeldLease> held;
        lock (_gate)
        {
            held = [.. _localRuns.Values];
        }

        foreach (var lease in held)
        {
            lease.Stop();
        }
    }

    /// <summary>With YieldAfterFailure, after a run failed here: waits so that healthy instances claim this run first.</summary>
    private Task YieldIfFailedAsync(Job job, long slot, CancellationToken stop)
    {
        if (!job.YieldAfterFailure || !job.FailedHere) return Task.CompletedTask;

        // Healthy instances claim by slot + jitter; the wait ends a heartbeat later, capped at MaxLate
        // so the claim is never refused as late.
        var end = slot + Math.Min(job.JitterMs + core.HeartbeatMs, job.MaxLateMs);
        return Delay(end - core.Now(), stop);
    }

    /// <summary>
    /// Sends the claim. null when the scheduler stopped first, or when the claim failed: the failure is
    /// reported as an Error event, and a failed takeover is tried again later.
    /// </summary>
    private async Task<ClaimOutcome?> ClaimAsync(Job job, ClaimInput input, RunReason reason, CancellationToken stop)
    {
        var lateMs = reason == RunReason.CatchUp ? job.CatchUpMs + job.JitterMs : job.MaxLateMs;

        try
        {
            await core.EnsureReadyAsync().ConfigureAwait(false);
            if (stop.IsCancellationRequested) return null;

            var claim = new RunClaim
            {
                Job = input.Job,
                Slot = input.Slot,
                Token = input.Token,
                InstanceId = input.InstanceId,
                HistorySize = input.HistorySize,
                HistoryTtl = input.HistoryTtl,
                Mode = reason == RunReason.Takeover ? ClaimMode.Takeover : ClaimMode.New,
                Reason = reason,
                Lease = TimeSpan.FromMilliseconds(core.LeaseMs),
                Overlap = job.Overlap,
                MaxAttempts = job.MaxAttempts,
                // Checked again on the store's clock, in case this claim was delayed on its way.
                MaxLate = TimeSpan.FromMilliseconds(lateMs + JobFactory.ClockToleranceMs),
                Keep = TimeSpan.FromMilliseconds(job.KeepMs),
            };
            return await runs.ClaimAsync(claim, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            if (stop.IsCancellationRequested) return null;

            core.OnError(error, job.Name, input.Slot);
            if (reason == RunReason.Takeover) Watch(job, input.Slot); // try again later
            return null;
        }
    }

    /// <summary>Claims one slot for RunNowAsync(): a new run with its own token, allowed to overlap, never retried.</summary>
    private Task<ClaimOutcome> ClaimManualAsync(Job job, long slot, string token)
    {
        var input = Claims.Input(core.InstanceId, job, slot);
        var claim = new RunClaim
        {
            Job = input.Job,
            Slot = slot,
            Token = token,
            InstanceId = input.InstanceId,
            HistorySize = input.HistorySize,
            HistoryTtl = input.HistoryTtl,
            Mode = ClaimMode.New,
            Reason = RunReason.Manual,
            Lease = TimeSpan.FromMilliseconds(core.LeaseMs),
            Overlap = OverlapPolicy.Allow,
            MaxAttempts = 1,
            Keep = TimeSpan.FromMilliseconds(job.KeepMs),
        };

        return runs.ClaimAsync(claim, CancellationToken.None);
    }

    /// <summary>Reports a claim this instance lost as a skip, and backs up the run that someone else holds.</summary>
    private void Follow(Job job, long slot, RunReason reason, ClaimOutcome outcome)
    {
        // Background checks don't report skips.
        var quiet = reason is RunReason.Takeover or RunReason.CatchUp;
        void Skip(SkipReason why, TimeSpan? late = null)
        {
            if (!quiet) core.OnSkipped(new SkipEvent { Job = job.Name, Slot = slot, InstanceId = core.InstanceId, Reason = why, Late = late });
        }

        switch (outcome)
        {
            case ClaimOutcome.Running: // someone alive runs it: be its backup
                Skip(SkipReason.Taken);
                Watch(job, slot);
                return;
            case ClaimOutcome.Overlap overlap: // the previous run is still going: back it up, and this one too
                Skip(SkipReason.Overlap);
                Watch(job, overlap.Slot);
                Watch(job, slot);
                return;
            case ClaimOutcome.Late late:
                Skip(SkipReason.Late, late.By);
                return;
            case ClaimOutcome.None: // skipped here as an overlap, and not started by anyone yet
                if (CouldStillStart(job, slot, core.Now())) Watch(job, slot);
                return;
            default: // done, given up, or a kind this version doesn't know
                Unwatch(job, slot);
                Skip(SkipReason.Taken);
                return;
        }
    }

    /// <summary>
    /// Checks a run another instance is executing every 1 to 2 heartbeats, until it ends or dies. So a
    /// crashed run is taken over soon after its lease runs out, and a released one within 2 heartbeats.
    /// </summary>
    private void Watch(Job job, long slot)
    {
        var key = (job.Name, slot);
        lock (_gate)
        {
            var watched = _watchers.ContainsKey(key) || _localRuns.ContainsKey(key);
            if (watched || !core.IsRunning || job.Removed) return;

            // At a random moment, so backups don't all try at once.
            var delayMs = core.HeartbeatMs + Random.Shared.NextInt64(core.HeartbeatMs);
            var watch = new Backup();
            watch.Timer = core.Time.CreateTimer(_ => Wake(job, slot, watch), null, TimeSpan.FromMilliseconds(delayMs), Timeout.InfiniteTimeSpan);
            _watchers[key] = watch;
        }
    }

    private void Wake(Job job, long slot, Backup watch)
    {
        lock (_gate)
        {
            // Created under the same lock, so a watch that is still current is always found here.
            if (!_watchers.TryGetValue((job.Name, slot), out var current) || current != watch) return;
            _watchers.Remove((job.Name, slot));
            watch.Timer?.Dispose();
        }

        if (core.IsRunning && !job.Removed) _ = core.Track(RunAsync(job, slot, RunReason.Takeover, core.StopToken));
    }

    private void Unwatch(Job job, long slot)
    {
        lock (_gate)
        {
            if (!_watchers.Remove((job.Name, slot), out var watch)) return;
            watch.Timer?.Dispose();
        }
    }

    /// <summary>Runs a job this instance claimed, renewing its lease until it ends.</summary>
    private async Task<RunRecord> ExecuteLeasedAsync(LeasedRun leased)
    {
        var (job, slot, _, _, attempt, reason, data, stop) = leased;
        var run = new Execution(attempt, reason, stop) { Data = data };
        var lease = new HeldLease(core, runs, leased, () => LoseRun(run, leased));
        run.Finish = lease.FinishAsync;
        run.Release = lease.ReleaseAsync;
        lock (_gate)
        {
            _localRuns[(job.Name, slot)] = lease;
        }

        try
        {
            var record = await executor.ExecuteAsync(job, slot, run).ConfigureAwait(false);
            if (reason != RunReason.Manual) job.FailedHere = record.Status == RunStatus.Failed;
            return record;
        }
        finally
        {
            lease.Stop();
            lock (_gate)
            {
                if (_localRuns.TryGetValue((job.Name, slot), out var current) && current == lease) _localRuns.Remove((job.Name, slot));
            }
        }
    }

    /// <summary>Cancels a run whose lease another instance holds now, or that could not be renewed in time, and raises Lost.</summary>
    private void LoseRun(Execution run, LeasedRun leased)
    {
        run.Abort(AbortCause.Lost);
        core.OnLost(new LostEvent { Job = leased.Job.Name, Slot = leased.Slot, InstanceId = core.InstanceId, Attempt = leased.Attempt });
    }

    /// <summary>Gives back a run won after stop began; when that fails the lease runs out on its own.</summary>
    private async Task ReleaseQuietlyAsync(ClaimInput input)
    {
        try
        {
            await runs.ReleaseAsync(input.Job, input.Slot, input.Token, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The lease runs out on its own, then a backup takes the run over.
        }
    }

    /// <summary>Waits, unless stop cancels the wait first.</summary>
    private async Task Delay(long ms, CancellationToken stop)
    {
        if (ms <= 0) return;

        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(ms), core.Time, stop).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Stop cancelled the wait: the caller sees the cancelled token and claims nothing.
        }
    }

    /// <summary>A run skipped as an overlap can still be started by another instance while it is the job's latest run and not too late.</summary>
    private static bool CouldStillStart(Job job, long slot, long now)
    {
        var latest = job.Schedule.Prev(now);
        var isLatest = latest is null || slot >= latest;
        return isLatest && now - slot <= job.MaxLateMs + JobFactory.ClockToleranceMs;
    }

    private sealed class Backup
    {
        public ITimer? Timer { get; set; }
    }
}
