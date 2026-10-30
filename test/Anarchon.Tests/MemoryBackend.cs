// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Tests;

/// <summary>A claim-only backend in one process: the instances that share one store share its lock.</summary>
internal sealed class MemoryBackend(MemoryBackend.Store store) : IBackend
{
    public MemoryBackend()
        : this(new Store())
    {
    }

    public string Name => "memory";

    public TimeSpan DedupeWindow => TimeSpan.MaxValue;

    public int Inits { get; private set; }

    public int Closes { get; private set; }

    /// <summary>When set, every claim throws it, as a store that went away does.</summary>
    public Exception? ClaimFailure { get; set; }

    /// <summary>When set, connecting throws it, as an unreachable store does.</summary>
    public Exception? InitFailure { get; set; }

    /// <summary>When set, closing throws it.</summary>
    public Exception? CloseFailure { get; set; }

    public Task InitAsync(CancellationToken cancellationToken)
    {
        Inits++;
        return InitFailure is { } failure ? Task.FromException(failure) : Task.CompletedTask;
    }

    public Task PrepareJobAsync(string job, int historySize, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task<bool> ClaimAsync(ClaimInput input, CancellationToken cancellationToken)
    {
        if (ClaimFailure is { } failure) return Task.FromException<bool>(failure);
        lock (store)
        {
            var first = store.Claims.TryAdd((input.Job, input.Slot), input) ? input : store.Claims[(input.Job, input.Slot)];
            return Task.FromResult(first.Token == input.Token);
        }
    }

    public Task<long?> RecordManualRunAsync(ClaimInput input, CancellationToken cancellationToken)
    {
        lock (store)
        {
            store.Manual.Add(input);
        }

        return Task.FromResult<long?>(null);
    }

    public Task SaveResultAsync(string job, StoredResult result, int historySize, TimeSpan? historyTtl, CancellationToken cancellationToken)
    {
        lock (store)
        {
            store.Results.Add((job, result));
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<RunRecord>> HistoryAsync(string job, int limit, CancellationToken cancellationToken)
    {
        lock (store)
        {
            var claims = store.Claims.Values.Where(c => c.Job == job).Select(c => ToClaim(c, manual: false))
                .Concat(store.Manual.Where(c => c.Job == job).Select(c => ToClaim(c, manual: true)))
                .ToList();
            var results = store.Results.Where(r => r.Job == job).Select(r => r.Result).ToList();
            return Task.FromResult(RunHistory.Build(job, claims, results, limit));
        }
    }

    public Task CloseAsync()
    {
        Closes++;
        return CloseFailure is { } failure ? Task.FromException(failure) : Task.CompletedTask;
    }

    private static ClaimRecord ToClaim(ClaimInput input, bool manual) =>
        new() { Slot = input.Slot, By = input.InstanceId, At = DateTimeOffset.FromUnixTimeMilliseconds(input.Slot), IsManual = manual };

    internal sealed class Store
    {
        public Dictionary<(string Job, long Slot), ClaimInput> Claims { get; } = [];

        public List<ClaimInput> Manual { get; } = [];

        public List<(string Job, StoredResult Result)> Results { get; } = [];
    }
}

/// <summary>A backend that says it has leases, for checks of the options that need them; it is never started.</summary>
internal sealed class LeaseBackendStub : IBackend
{
    public string Name => "lease-stub";

    public TimeSpan DedupeWindow => TimeSpan.MaxValue;

    public IRunStore? Runs { get; } = new NoRuns();

    public Task InitAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task PrepareJobAsync(string job, int historySize, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task<bool> ClaimAsync(ClaimInput input, CancellationToken cancellationToken) => Task.FromResult(false);

    public Task<long?> RecordManualRunAsync(ClaimInput input, CancellationToken cancellationToken) => Task.FromResult<long?>(null);

    public Task SaveResultAsync(string job, StoredResult result, int historySize, TimeSpan? historyTtl, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task<IReadOnlyList<RunRecord>> HistoryAsync(string job, int limit, CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<RunRecord>>([]);

    public Task CloseAsync() => Task.CompletedTask;

    private sealed class NoRuns : IRunStore
    {
        public Task<ClaimOutcome> ClaimAsync(RunClaim claim, CancellationToken cancellationToken) => Task.FromResult<ClaimOutcome>(new ClaimOutcome.Done());

        public Task<bool> HeartbeatAsync(string job, long slot, string token, TimeSpan lease, CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<bool> FinishAsync(string job, long slot, string token, RunStatus status, CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<bool> ReleaseAsync(string job, long slot, string token, CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<IReadOnlyList<ActiveRun>> ActiveAsync(string job, CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<ActiveRun>>([]);
    }
}
