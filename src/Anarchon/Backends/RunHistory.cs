// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.Backends;

/// <summary>What a backend stores when a run is claimed, in the shape <see cref="RunHistory.Build"/> reads.</summary>
/// <example>
/// <code>
/// var claim = new ClaimRecord { Slot = input.Slot, By = input.InstanceId, At = DateTimeOffset.UtcNow };
/// </code>
/// </example>
public sealed record ClaimRecord
{
    /// <summary>The run's slot.</summary>
    public required long Slot { get; init; }

    /// <summary>The instance that claimed it.</summary>
    public required string By { get; init; }

    /// <summary>When it was claimed: the record's start until a result says otherwise.</summary>
    public required DateTimeOffset At { get; init; }

    /// <summary>true for a <c>RunNowAsync()</c> run.</summary>
    public bool IsManual { get; init; }

    /// <summary>Which attempt this claim is (lease backends).</summary>
    public int Attempt { get; init; } = 1;

    /// <summary>Why the attempt started (lease backends).</summary>
    public RunReason? Reason { get; init; }
}

/// <summary>Builds the records <c>HistoryAsync()</c> returns, the same way for every backend.</summary>
public static class RunHistory
{
    /// <summary>
    /// Joins a job's claims with their results: newest first, one record per attempt, at most
    /// <paramref name="limit"/>. A result belongs to the claim of the same slot, instance and attempt.
    /// </summary>
    /// <param name="job">The job's name.</param>
    /// <param name="claims">The job's claims, in any order.</param>
    /// <param name="results">The results the winners saved, in any order.</param>
    /// <param name="limit">How many records to return.</param>
    /// <param name="pendingStatus">
    /// The status of a claim without a result. Default Running; a backend that knows the attempt
    /// ended without a result (its lease ran out) answers Interrupted.
    /// </param>
    /// <returns>The records, newest first.</returns>
    /// <example>The history of a custom backend
    /// <code>
    /// public async Task&lt;IReadOnlyList&lt;RunRecord&gt;&gt; HistoryAsync(string job, int limit, CancellationToken ct)
    /// {
    ///     var claims = await _db.ClaimsOfAsync(job, ct);
    ///     var results = await _db.ResultsOfAsync(job, ct);
    ///     return RunHistory.Build(job, claims, results, limit);
    /// }
    /// </code>
    /// </example>
    public static IReadOnlyList<RunRecord> Build(
        string job,
        IEnumerable<ClaimRecord> claims,
        IEnumerable<StoredResult> results,
        int limit,
        Func<ClaimRecord, RunStatus>? pendingStatus = null)
    {
        var byAttempt = new Dictionary<(long, string, int), StoredResult>();
        foreach (var result in results)
        {
            byAttempt[(result.Slot, result.By, result.Attempt)] = result;
        }

        return claims
            .OrderByDescending(claim => claim.Slot)
            .ThenByDescending(claim => claim.Attempt)
            .ThenByDescending(claim => claim.At)
            .Take(limit)
            .Select(claim => ToRecord(job, claim, byAttempt.GetValueOrDefault((claim.Slot, claim.By, claim.Attempt)), pendingStatus))
            .ToList();
    }

    private static RunRecord ToRecord(
        string job,
        ClaimRecord claim,
        StoredResult? result,
        Func<ClaimRecord, RunStatus>? pendingStatus)
    {
        var status = result?.Status ?? pendingStatus?.Invoke(claim) ?? RunStatus.Running;

        return new RunRecord
        {
            Job = job,
            Slot = claim.Slot,
            InstanceId = claim.By,
            IsManual = claim.IsManual,
            Attempt = claim.Attempt,
            Reason = claim.Reason,
            Status = status,
            StartedAt = result?.StartedAt ?? claim.At,
            FinishedAt = result?.FinishedAt,
            Duration = result?.Duration,
            Error = result?.Error,
        };
    }
}

/// <summary>The version of the data layout the built-in backends write.</summary>
/// <remarks>
/// It goes up only for a change an older version of Anarchon can't read or would damage; added
/// fields don't raise it, since every version keeps the fields it doesn't know.
/// </remarks>
public static class StoreFormat
{
    /// <summary>The format this version writes and reads.</summary>
    public const int Current = 1;

    /// <summary>Throws when the store holds data in a newer format than this version reads.</summary>
    /// <param name="found">The format recorded in the store.</param>
    /// <param name="where">The store, for the message, such as <c>The keys under "cron:"</c>.</param>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.StoreFormat"/>: the format is newer.</exception>
    /// <example>
    /// <code>
    /// StoreFormat.Check(await ReadMarkerAsync(), $"The table {_table}");
    /// </code>
    /// </example>
    public static void Check(string? found, string where)
    {
        if (!int.TryParse(found, out var version) || version <= Current) return;

        throw new AnarchonException(
            AnarchonErrorCode.StoreFormat,
            $"{where} holds data in store format {version}, newer than this version of Anarchon reads ({Current}). " +
            "Run the same Anarchon version on every instance.");
    }
}
