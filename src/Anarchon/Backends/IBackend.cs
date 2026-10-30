// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.Backends;

/// <summary>
/// Where runs are claimed and recorded. Every backend gives the same guarantee: for one slot,
/// <see cref="ClaimAsync"/> returns true for exactly one instance. A backend with leases also
/// implements <see cref="Runs"/>.
/// </summary>
/// <remarks>
/// Members added in later versions get a default implementation, so backends written against an
/// older version keep compiling.
/// </remarks>
/// <example>The idea, as a backend for tests: one process, so a lock is atomic enough
/// <code>
/// sealed class MemoryBackend : IBackend
/// {
///     private readonly Dictionary&lt;(string, long), ClaimInput&gt; _claims = [];
///     public string Name =&gt; "memory";
///     public TimeSpan DedupeWindow =&gt; TimeSpan.MaxValue;
///     public Task&lt;bool&gt; ClaimAsync(ClaimInput input, CancellationToken ct)
///     {
///         lock (_claims)
///         {
///             var first = _claims.TryAdd((input.Job, input.Slot), input) ? input : _claims[(input.Job, input.Slot)];
///             return Task.FromResult(first.Token == input.Token);
///         }
///     }
///     // ...the other members
/// }
/// </code>
/// </example>
public interface IBackend
{
    /// <summary>Which backend and strategy, for logs and error messages, such as <c>redis:lease</c>.</summary>
    string Name { get; }

    /// <summary>
    /// How long the backend remembers that a run was claimed. <see cref="TimeSpan.MaxValue"/>:
    /// forever. A job's jitter plus its <c>MaxLate</c> must be shorter, which the scheduler checks
    /// when the job is added.
    /// </summary>
    TimeSpan DedupeWindow { get; }

    /// <summary>
    /// The run store when the backend has leases; null otherwise. A backend with leases gets backup
    /// runners, no overlapping runs, catch-up, retries and the lease options.
    /// </summary>
    IRunStore? Runs => null;

    /// <summary>
    /// Throws when a job can't run safely on this backend (its name, its limits, an option the backend
    /// can't honour). Called when the job is added, so a bad job fails at once.
    /// </summary>
    /// <param name="job">The job's settings after defaults were applied.</param>
    void CheckJob(JobLimits job)
    {
    }

    /// <summary>Connects and checks the server. Called by <c>StartAsync()</c> and before reads.</summary>
    /// <param name="cancellationToken">Stops waiting for the connection.</param>
    /// <returns>A task that completes once the backend is ready.</returns>
    Task InitAsync(CancellationToken cancellationToken);

    /// <summary>One-time setup for a job, such as a stream or a queue.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="historySize">About how many runs the job keeps.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the job is set up.</returns>
    Task PrepareJobAsync(string job, int historySize, CancellationToken cancellationToken);

    /// <summary>true for exactly one instance per slot. Safe to repeat with the same token.</summary>
    /// <param name="input">The run and who claims it.</param>
    /// <param name="cancellationToken">Stops waiting; the claim may still reach the store.</param>
    /// <returns>Whether this claim won the run.</returns>
    Task<bool> ClaimAsync(ClaimInput input, CancellationToken cancellationToken);

    /// <summary>Called after a won claim, for backends whose claim isn't already a history record.</summary>
    /// <param name="input">The claim that won.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the claim is recorded.</returns>
    Task RecordClaimAsync(ClaimInput input, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>Records a manual run (<c>RunNowAsync()</c>), which is not deduplicated.</summary>
    /// <param name="input">The manual run.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The slot the backend actually used, or null when it used <c>input.Slot</c>.</returns>
    Task<long?> RecordManualRunAsync(ClaimInput input, CancellationToken cancellationToken);

    /// <summary>Stores how an attempt ended, and trims the job's history to its size and age.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="result">How the attempt ended.</param>
    /// <param name="historySize">About how many runs to keep.</param>
    /// <param name="historyTtl">Drop records older than this; null keeps by count only.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the result is stored.</returns>
    Task SaveResultAsync(
        string job,
        StoredResult result,
        int historySize,
        TimeSpan? historyTtl,
        CancellationToken cancellationToken);

    /// <summary>
    /// The latest runs of a job from all instances, newest first, at most <paramref name="limit"/>
    /// attempts. Build it with <see cref="RunHistory.Build"/> so it matches the other backends.
    /// </summary>
    /// <param name="job">The job's name.</param>
    /// <param name="limit">How many attempts to return at most.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The attempts, newest first.</returns>
    Task<IReadOnlyList<RunRecord>> HistoryAsync(string job, int limit, CancellationToken cancellationToken);

    /// <summary>
    /// Closes the connections the backend opened itself. A client passed in stays open. The backend
    /// may be initialized again afterwards (the scheduler can be started again).
    /// </summary>
    /// <returns>A task that completes once the connections are closed.</returns>
    Task CloseAsync();
}

/// <summary>
/// A backend's run store: runs with leases. The winner renews its lease while the job runs; when a
/// lease runs out before the run is finished, a backup can claim the next attempt.
/// </summary>
/// <example>How a store decides a claim, in outline
/// <code>
/// var run = await LoadAsync(claim.Job, claim.Slot);
/// if (run is null) return claim.Mode == ClaimMode.Takeover ? new ClaimOutcome.None() : await StartAsync(claim, attempt: 1);
/// if (run.Finished) return new ClaimOutcome.Done();
/// if (run.LeaseEnd > now) return new ClaimOutcome.Running(run.Attempt);
/// return run.Attempt &lt; run.MaxAttempts ? await StartAsync(claim, run.Attempt + 1) : new ClaimOutcome.Done();
/// </code>
/// </example>
public interface IRunStore
{
    /// <summary>Decides one claim atomically. Exactly one caller gets <see cref="ClaimOutcome.Won"/> for each attempt of a run.</summary>
    /// <param name="claim">The claim and the rules the store applies to it.</param>
    /// <param name="cancellationToken">Stops waiting; the claim may still reach the store.</param>
    /// <returns>What the store decided.</returns>
    Task<ClaimOutcome> ClaimAsync(RunClaim claim, CancellationToken cancellationToken);

    /// <summary>Renews the lease to <paramref name="lease"/> from now, on the store's clock.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="slot">The run's slot.</param>
    /// <param name="token">The token of the winning claim.</param>
    /// <param name="lease">How long the renewed lease lasts.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>false when the run is no longer ours (a backup took it over).</returns>
    Task<bool> HeartbeatAsync(string job, long slot, string token, TimeSpan lease, CancellationToken cancellationToken);

    /// <summary>Marks the run ended.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="slot">The run's slot.</param>
    /// <param name="token">The token of the winning claim.</param>
    /// <param name="status">Success, Failed or Interrupted.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>false when the run is no longer ours.</returns>
    Task<bool> FinishAsync(string job, long slot, string token, RunStatus status, CancellationToken cancellationToken);

    /// <summary>
    /// Gives an unfinished run up so a backup can take it over now (a graceful stop). This doesn't
    /// count against the run's attempts.
    /// </summary>
    /// <param name="job">The job's name.</param>
    /// <param name="slot">The run's slot.</param>
    /// <param name="token">The token of the winning claim.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>false when the run is no longer ours.</returns>
    Task<bool> ReleaseAsync(string job, long slot, string token, CancellationToken cancellationToken);

    /// <summary>The runs of this job not finished yet, and whether their lease is still alive.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The unfinished runs.</returns>
    Task<IReadOnlyList<ActiveRun>> ActiveAsync(string job, CancellationToken cancellationToken);
}
