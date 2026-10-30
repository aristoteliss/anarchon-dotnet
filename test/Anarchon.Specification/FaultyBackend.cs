// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Specification;

/// <summary>What goes wrong in a <see cref="FaultyBackend"/>'s run store.</summary>
public sealed record Faults
{
    /// <summary>Heartbeats never answer: the network to the store dropped them, or the store froze.</summary>
    public bool HeartbeatsHang { get; init; }

    /// <summary>Every heartbeat throws, as a store that refuses connections does.</summary>
    public bool HeartbeatsThrow { get; init; }

    /// <summary>How many times in a row "finished" throws before it reaches the store.</summary>
    public int FinishFailures { get; init; }

    /// <summary>Giving a run up on stop throws.</summary>
    public bool ReleaseThrows { get; init; }

    /// <summary>Claims of new runs reach the store this much later, so other instances decide first.</summary>
    public TimeSpan NewClaimDelay { get; init; }
}

/// <summary>A lease backend whose run store fails the way <see cref="Faults"/> says; everything else reaches the real store.</summary>
public sealed class FaultyBackend(IBackend inner, Faults faults) : IBackend
{
    private readonly FaultyRuns _runs = new(inner.Runs ?? throw new ArgumentException($"{inner.Name} has no leases", nameof(inner)), faults);

    public string Name => inner.Name;

    public TimeSpan DedupeWindow => inner.DedupeWindow;

    public IRunStore? Runs => _runs;

    public void CheckJob(JobLimits job) => inner.CheckJob(job);

    public Task InitAsync(CancellationToken cancellationToken) => inner.InitAsync(cancellationToken);

    public Task PrepareJobAsync(string job, int historySize, CancellationToken cancellationToken) => inner.PrepareJobAsync(job, historySize, cancellationToken);

    public Task<bool> ClaimAsync(ClaimInput input, CancellationToken cancellationToken) => inner.ClaimAsync(input, cancellationToken);

    public Task<long?> RecordManualRunAsync(ClaimInput input, CancellationToken cancellationToken) => inner.RecordManualRunAsync(input, cancellationToken);

    public Task SaveResultAsync(string job, StoredResult result, int historySize, TimeSpan? historyTtl, CancellationToken cancellationToken) =>
        inner.SaveResultAsync(job, result, historySize, historyTtl, cancellationToken);

    public Task<IReadOnlyList<RunRecord>> HistoryAsync(string job, int limit, CancellationToken cancellationToken) => inner.HistoryAsync(job, limit, cancellationToken);

    public Task CloseAsync() => inner.CloseAsync();

    private sealed class FaultyRuns(IRunStore runs, Faults faults) : IRunStore
    {
        private int _finishFailures = faults.FinishFailures;

        public async Task<ClaimOutcome> ClaimAsync(RunClaim claim, CancellationToken cancellationToken)
        {
            if (claim.Mode == ClaimMode.New) await Task.Delay(faults.NewClaimDelay, CancellationToken.None);
            return await runs.ClaimAsync(claim, cancellationToken);
        }

        public Task<bool> HeartbeatAsync(string job, long slot, string token, TimeSpan lease, CancellationToken cancellationToken)
        {
            if (faults.HeartbeatsHang) return new TaskCompletionSource<bool>().Task;
            if (faults.HeartbeatsThrow) throw new IOException("the store refused the connection");
            return runs.HeartbeatAsync(job, slot, token, lease, cancellationToken);
        }

        public Task<bool> FinishAsync(string job, long slot, string token, RunStatus status, CancellationToken cancellationToken)
        {
            if (Interlocked.Decrement(ref _finishFailures) >= 0) throw new IOException("the store dropped the connection");
            return runs.FinishAsync(job, slot, token, status, cancellationToken);
        }

        public Task<bool> ReleaseAsync(string job, long slot, string token, CancellationToken cancellationToken)
        {
            if (faults.ReleaseThrows) throw new IOException("the store dropped the connection");
            return runs.ReleaseAsync(job, slot, token, cancellationToken);
        }

        public Task<IReadOnlyList<ActiveRun>> ActiveAsync(string job, CancellationToken cancellationToken) => runs.ActiveAsync(job, cancellationToken);
    }
}
