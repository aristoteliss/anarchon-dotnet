// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Scheduling;

/// <summary>
/// Runs on backends without leases: one atomic claim per run, and the instance that wins runs the
/// job. No backups and no retries.
/// </summary>
internal sealed class ClaimOnlyRunner(ISchedulerCore core, Executor executor)
{
    /// <summary>A scheduled run is due: claims it, and runs it if this instance won. Never throws.</summary>
    public async Task RunAsync(Job job, long slot, CancellationToken stop)
    {
        var input = Claims.Input(core.InstanceId, job, slot);
        var won = await ClaimAsync(job, input, stop).ConfigureAwait(false);
        if (won is null) return;

        if (won == false)
        {
            if (!stop.IsCancellationRequested) EmitTaken(job, slot);
            return;
        }

        try
        {
            await core.Backend.RecordClaimAsync(input, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            // The run is ours either way; a failed history write must not cancel it.
            core.OnError(error, job.Name, slot);
        }

        // Won after stop began: the executor records it as interrupted without starting the handler.
        await executor.ExecuteAsync(job, slot, new Execution(1, RunReason.Scheduled, stop)).ConfigureAwait(false);
    }

    /// <summary>RunNowAsync(): recorded in history, not deduplicated.</summary>
    public async Task<RunRecord> RunManualAsync(Job job, object? data, CancellationToken stop)
    {
        var requested = core.Now();
        var run = new Execution(1, RunReason.Manual, stop) { Data = data };
        if (stop.IsCancellationRequested) return await executor.ExecuteAsync(job, requested, run, claimed: false).ConfigureAwait(false);

        var input = Claims.Input(core.InstanceId, job, requested);
        var recorded = await core.Backend.RecordManualRunAsync(input, CancellationToken.None).ConfigureAwait(false);
        return await executor.ExecuteAsync(job, recorded ?? requested, run).ConfigureAwait(false);
    }

    /// <summary>
    /// Sends the claim. null when the scheduler stopped first, or when the claim failed: the failure
    /// is reported as an Error event.
    /// </summary>
    private async Task<bool?> ClaimAsync(Job job, ClaimInput input, CancellationToken stop)
    {
        try
        {
            await core.EnsureReadyAsync().ConfigureAwait(false);
            await core.PrepareAsync(job).ConfigureAwait(false);
            if (stop.IsCancellationRequested) return null;

            return await core.Backend.ClaimAsync(input, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            if (!stop.IsCancellationRequested) core.OnError(error, job.Name, input.Slot);
            return null;
        }
    }

    private void EmitTaken(Job job, long slot) =>
        core.OnSkipped(new SkipEvent { Job = job.Name, Slot = slot, InstanceId = core.InstanceId, Reason = SkipReason.Taken });
}

/// <summary>What an instance sends to claim a run.</summary>
internal static class Claims
{
    /// <summary>The claim of one run. The token is new for every attempt.</summary>
    public static ClaimInput Input(string instanceId, Job job, long slot) => new()
    {
        Job = job.Name,
        Slot = slot,
        Token = Guid.NewGuid().ToString(),
        InstanceId = instanceId,
        HistorySize = job.HistorySize,
        HistoryTtl = job.HistoryTtl,
    };
}
