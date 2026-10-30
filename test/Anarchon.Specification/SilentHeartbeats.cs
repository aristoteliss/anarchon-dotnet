// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Specification;

/// <summary>A backend whose store never answers heartbeats: the network to it dropped them, or it froze.</summary>
public sealed class SilentHeartbeats(IBackend inner) : IBackend
{
    private readonly SilentRuns _runs = new(inner.Runs ?? throw new ArgumentException($"{inner.Name} has no leases", nameof(inner)));

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

    private sealed class SilentRuns(IRunStore runs) : IRunStore
    {
        public Task<ClaimOutcome> ClaimAsync(RunClaim claim, CancellationToken cancellationToken) => runs.ClaimAsync(claim, cancellationToken);

        public Task<bool> HeartbeatAsync(string job, long slot, string token, TimeSpan lease, CancellationToken cancellationToken) =>
            new TaskCompletionSource<bool>().Task;

        public Task<bool> FinishAsync(string job, long slot, string token, RunStatus status, CancellationToken cancellationToken) =>
            runs.FinishAsync(job, slot, token, status, cancellationToken);

        public Task<bool> ReleaseAsync(string job, long slot, string token, CancellationToken cancellationToken) => runs.ReleaseAsync(job, slot, token, cancellationToken);

        public Task<IReadOnlyList<ActiveRun>> ActiveAsync(string job, CancellationToken cancellationToken) => runs.ActiveAsync(job, cancellationToken);
    }
}
