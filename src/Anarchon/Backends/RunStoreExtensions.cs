// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.Backends;

/// <summary>Claims that every lease store answers the same way, built on <see cref="IRunStore.ClaimAsync"/>.</summary>
public static class RunStoreExtensions
{
    private const int ManualTries = 10;

    /// <summary>Long enough for any run that has no backups: nobody takes it over while it runs.</summary>
    private static readonly TimeSpan LongLease = TimeSpan.FromDays(1);

    /// <summary>
    /// The plain claim of <see cref="IBackend.ClaimAsync"/> on a lease store: one winner, a lease of a
    /// day, no backups and no retries. For callers that don't use <see cref="IBackend.Runs"/>.
    /// </summary>
    /// <param name="runs">The lease store.</param>
    /// <param name="input">The run and who claims it.</param>
    /// <param name="cancellationToken">Stops waiting; the claim may still reach the store.</param>
    /// <returns>Whether this claim won the run.</returns>
    /// <example>
    /// <code>
    /// public Task&lt;bool&gt; ClaimAsync(ClaimInput input, CancellationToken ct) =&gt; _runs.ClaimWithoutBackupsAsync(input, ct);
    /// </code>
    /// </example>
    public static async Task<bool> ClaimWithoutBackupsAsync(this IRunStore runs, ClaimInput input, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(runs);
        ArgumentNullException.ThrowIfNull(input);

        var outcome = await runs.ClaimAsync(OneAttempt(input, input.Slot, RunReason.Scheduled), cancellationToken).ConfigureAwait(false);
        return outcome is ClaimOutcome.Won;
    }

    /// <summary>
    /// Claims a manual run on a lease store, at the first free millisecond from its slot. The run is
    /// never retried and may overlap other runs of its job.
    /// </summary>
    /// <param name="runs">The lease store.</param>
    /// <param name="input">The manual run.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The slot that was claimed.</returns>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.Store"/>: no free slot was found.</exception>
    /// <example>
    /// <code>
    /// public async Task&lt;long?&gt; RecordManualRunAsync(ClaimInput input, CancellationToken ct) =&gt; await _runs.ClaimManualRunAsync(input, ct);
    /// </code>
    /// </example>
    public static async Task<long> ClaimManualRunAsync(this IRunStore runs, ClaimInput input, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(runs);
        ArgumentNullException.ThrowIfNull(input);

        for (var slot = input.Slot; slot < input.Slot + ManualTries; slot++)
        {
            var outcome = await runs.ClaimAsync(OneAttempt(input, slot, RunReason.Manual), cancellationToken).ConfigureAwait(false);
            if (outcome is ClaimOutcome.Won) return slot;
        }

        throw new AnarchonException(AnarchonErrorCode.Store, $"Could not record a manual run of \"{input.Job}\"");
    }

    private static RunClaim OneAttempt(ClaimInput input, long slot, RunReason reason) => new()
    {
        Job = input.Job,
        Slot = slot,
        Token = input.Token,
        InstanceId = input.InstanceId,
        HistorySize = input.HistorySize,
        HistoryTtl = input.HistoryTtl,
        Mode = ClaimMode.New,
        Reason = reason,
        Lease = LongLease,
        Overlap = OverlapPolicy.Allow,
        MaxAttempts = 1,
    };
}
