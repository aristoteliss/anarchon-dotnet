// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Sql;

/// <summary>
/// The history in <c>&lt;table&gt;_attempts</c>: one row per attempt, written by the claim that won it
/// and completed by the winner's result.
/// </summary>
internal sealed class SqlHistory(SqlTables tables, SqlDialect dialect, OpenConnection open)
{
    /// <summary>Records a won attempt inside the claim's transaction, then trims the job's attempts.</summary>
    public async Task RecordAttemptAsync(SqlSession session, RunClaim claim, int attempt, long now)
    {
        var sql = $"""
            INSERT INTO {tables.Attempts} (job, slot, attempt, instance_id, token, reason, manual, claimed_at)
            VALUES (@job, @slot, @attempt, @instanceId, @token, @reason, @manual, @now)
            """;
        short manual = claim.Reason == RunReason.Manual ? (short)1 : (short)0;
        await session.ExecuteAsync(
            sql,
            ("job", claim.Job),
            ("slot", claim.Slot),
            ("attempt", attempt),
            ("instanceId", claim.InstanceId),
            ("token", claim.Token),
            ("reason", RunText.ToText(claim.Reason)),
            ("manual", manual),
            ("now", now)).ConfigureAwait(false);

        await TrimAsync(session, claim, now).ConfigureAwait(false);
    }

    /// <summary>Writes how the attempt ended on its row, then drops attempts older than the age limit.</summary>
    public async Task SaveResultAsync(string job, StoredResult result, TimeSpan? historyTtl, CancellationToken cancellationToken)
    {
        var sql = $"""
            UPDATE {tables.Attempts}
            SET status = @status, started_at = @startedAt, finished_at = @finishedAt, duration_ms = @durationMs, error = @error
            WHERE job = @job AND slot = @slot AND attempt = @attempt AND instance_id = @by
            """;
        var finishedAt = result.FinishedAt.ToUnixTimeMilliseconds();
        (string, object?)[] parameters =
        [
            ("status", RunText.ToText(result.Status)),
            ("startedAt", result.StartedAt.ToUnixTimeMilliseconds()),
            ("finishedAt", finishedAt),
            ("durationMs", (long)result.Duration.TotalMilliseconds),
            ("error", result.Error),
            ("job", job),
            ("slot", result.Slot),
            ("attempt", result.Attempt),
            ("by", result.By),
        ];

        await SqlSession.RunAsync(open, async session =>
        {
            await session.ExecuteAsync(sql, parameters).ConfigureAwait(false);
            if (historyTtl is { } ttl) await DropOlderThanAsync(session, job, finishedAt - (long)ttl.TotalMilliseconds).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The job's attempts, newest first. An attempt without a result is Running only while it is the
    /// current attempt of its run with a live lease, by the database's clock.
    /// </summary>
    public async Task<IReadOnlyList<RunRecord>> HistoryAsync(string job, int limit, CancellationToken cancellationToken)
    {
        var sql = $"""
            SELECT a.slot, a.attempt, a.instance_id, a.reason, a.manual, a.claimed_at,
                   a.status, a.started_at, a.finished_at, a.duration_ms, a.error,
                   r.attempt AS current_attempt, r.status AS current_status, r.lease_until,
                   {dialect.NowMs} AS now
            FROM {tables.Attempts} a
            LEFT JOIN {tables.Runs} r ON r.job = a.job AND r.slot = a.slot
            WHERE a.job = @job
            ORDER BY a.slot DESC, a.attempt DESC
            LIMIT @limit
            """;
        var rows = await SqlSession.RunAsync(open, session => session.QueryAsync(sql, ("job", job), ("limit", limit)), cancellationToken).ConfigureAwait(false);

        var claims = rows.Select(ToClaim).ToList();
        var results = rows.Zip(claims, ToResult).OfType<StoredResult>();
        var pending = rows.Zip(claims).ToDictionary(pair => (pair.Second.Slot, pair.Second.Attempt), pair => PendingStatus(pair.First));
        return RunHistory.Build(job, claims, results, limit, claim => pending.GetValueOrDefault((claim.Slot, claim.Attempt), RunStatus.Interrupted));
    }

    /// <summary>Deletes the job's attempts beyond the newest <see cref="ClaimInput.HistorySize"/>, and those older than its age limit.</summary>
    private async Task TrimAsync(SqlSession session, RunClaim claim, long now)
    {
        var sql = $"""
            DELETE FROM {tables.Attempts}
            WHERE job = @job
              AND slot < (SELECT slot
                          FROM {tables.Attempts}
                          WHERE job = @job
                          ORDER BY slot DESC, attempt DESC
                          LIMIT 1 OFFSET @historySize)
            """;
        await session.ExecuteAsync(sql, ("job", claim.Job), ("historySize", claim.HistorySize)).ConfigureAwait(false);

        if (claim.HistoryTtl is { } ttl) await DropOlderThanAsync(session, claim.Job, now - (long)ttl.TotalMilliseconds).ConfigureAwait(false);
    }

    private async Task DropOlderThanAsync(SqlSession session, string job, long oldestMs)
    {
        var sql = $"DELETE FROM {tables.Attempts} WHERE job = @job AND COALESCE(finished_at, claimed_at) < @oldest";
        await session.ExecuteAsync(sql, ("job", job), ("oldest", oldestMs)).ConfigureAwait(false);
    }

    private static ClaimRecord ToClaim(SqlRow row) => new()
    {
        Slot = row.Long("slot"),
        By = row.Text("instance_id") ?? "",
        Attempt = row.Int("attempt"),
        At = DateTimeOffset.FromUnixTimeMilliseconds(row.Long("claimed_at")),
        IsManual = row.Long("manual") == 1,
        Reason = RunText.ToReason(row.Text("reason")),
    };

    /// <summary>The result on an attempt's row; null while the attempt has none.</summary>
    private static StoredResult? ToResult(SqlRow row, ClaimRecord claim)
    {
        var status = RunText.ToStatus(row.Text("status"));
        if (status is null or RunStatus.Running) return null;

        return new StoredResult
        {
            Slot = claim.Slot,
            By = claim.By,
            Attempt = claim.Attempt,
            Status = status.Value,
            StartedAt = DateTimeOffset.FromUnixTimeMilliseconds(row.Long("started_at")),
            FinishedAt = DateTimeOffset.FromUnixTimeMilliseconds(row.Long("finished_at")),
            Duration = TimeSpan.FromMilliseconds(row.Long("duration_ms")),
            Error = row.Text("error"),
        };
    }

    /// <summary>The status of an attempt without a result: still running, or its runner stopped before finishing.</summary>
    private static RunStatus PendingStatus(SqlRow row)
    {
        var current = !row.IsNull("current_attempt") && row.Int("current_attempt") == row.Int("attempt");
        if (!current) return RunStatus.Interrupted;

        var alive = row.Long("lease_until") >= row.Long("now");
        return RunText.ToStatus(row.Text("current_status")) switch
        {
            RunStatus.Running when alive => RunStatus.Running,
            RunStatus.Success => RunStatus.Success,
            RunStatus.Failed => RunStatus.Failed,
            _ => RunStatus.Interrupted,
        };
    }
}
