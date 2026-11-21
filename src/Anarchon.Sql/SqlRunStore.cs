// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Data.Common;
using Anarchon.Backends;

namespace Anarchon.Sql;

/// <summary>
/// The lease rules on the runs table (see <see cref="SqlBackend"/>). A claim is one transaction;
/// heartbeat, finish and release are single UPDATEs that match the token, so after a takeover the
/// old runner changes nothing.
/// </summary>
internal sealed class SqlRunStore(SqlTables tables, SqlDialect dialect, OpenConnection open, SqlHistory history) : IRunStore
{
    /// <summary>The columns of a run's row that a claim decides from.</summary>
    private const string RunColumns = "attempt, max_attempts, released, token, status, lease_until";

    public async Task<ClaimOutcome> ClaimAsync(RunClaim claim, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(claim);

        var connection = await open(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            if (claim.Mode == ClaimMode.Takeover)
            {
                var answer = await TakeoverWithoutWritingAsync(new SqlSession(connection, null, cancellationToken), claim).ConfigureAwait(false);
                if (answer is not null) return answer;
            }

            return await ClaimInTransactionAsync(connection, claim, cancellationToken).ConfigureAwait(false);
        }
    }

    public Task<bool> HeartbeatAsync(string job, long slot, string token, TimeSpan lease, CancellationToken cancellationToken)
    {
        // A released lease (lease_until = 0) stays released: a heartbeat sent before the release can arrive after it.
        var sql = $"""
            UPDATE {tables.Runs}
            SET lease_until = {dialect.NowMs} + @lease
            WHERE job = @job AND slot = @slot AND token = @token
              AND status = 'running' AND lease_until > 0
            """;
        return ChangedAsync(sql, [("lease", (long)lease.TotalMilliseconds), ("job", job), ("slot", slot), ("token", token)], cancellationToken);
    }

    public Task<bool> FinishAsync(string job, long slot, string token, RunStatus status, CancellationToken cancellationToken)
    {
        var sql = $"""
            UPDATE {tables.Runs}
            SET status = @status, lease_until = 0
            WHERE job = @job AND slot = @slot AND token = @token
            """;
        return ChangedAsync(sql, [("status", RunText.ToText(status)), ("job", job), ("slot", slot), ("token", token)], cancellationToken);
    }

    public Task<bool> ReleaseAsync(string job, long slot, string token, CancellationToken cancellationToken)
    {
        // Only a live lease can be released; a runner whose lease already ran out counts as having died.
        var sql = $"""
            UPDATE {tables.Runs}
            SET lease_until = 0, released = released + 1
            WHERE job = @job AND slot = @slot AND token = @token AND status = 'running'
              AND lease_until >= {dialect.NowMs}
            """;
        return ChangedAsync(sql, [("job", job), ("slot", slot), ("token", token)], cancellationToken);
    }

    public async Task<IReadOnlyList<ActiveRun>> ActiveAsync(string job, CancellationToken cancellationToken)
    {
        var sql = $"""
            SELECT slot, attempt, CASE WHEN lease_until >= {dialect.NowMs} THEN 1 ELSE 0 END AS alive
            FROM {tables.Runs}
            WHERE job = @job AND status = 'running'
            ORDER BY slot
            """;
        var rows = await SqlSession.RunAsync(open, session => session.QueryAsync(sql, ("job", job)), cancellationToken).ConfigureAwait(false);

        return [.. rows.Select(row => new ActiveRun(row.Long("slot"), row.Int("attempt"), row.Long("alive") == 1))];
    }

    /// <summary>
    /// One transaction: claims of the job take turns, the run's row is read and locked, and the
    /// writes the decision asks for commit together.
    /// </summary>
    private async Task<ClaimOutcome> ClaimInTransactionAsync(DbConnection connection, RunClaim claim, CancellationToken cancellationToken)
    {
        var transaction = await dialect.BeginClaimAsync(connection, cancellationToken).ConfigureAwait(false);
        await using (transaction.ConfigureAwait(false))
        {
            var session = new SqlSession(connection, transaction, cancellationToken);
            if (dialect.SerializeClaims(tables.Runs) is { } serialize) await session.ExecuteAsync(serialize, ("job", claim.Job)).ConfigureAwait(false);

            var now = await ClockAsync(session).ConfigureAwait(false);
            var run = await ReadRunAsync(session, claim).ConfigureAwait(false);
            var step = ClaimStep.Decide(run, claim, now);
            var outcome = await ActAsync(session, claim, step, now).ConfigureAwait(false);

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return outcome;
        }
    }

    /// <summary>
    /// A takeover answered by a plain read when it can't win. Backups ask every 1 to 2 heartbeats and
    /// nearly always find the run alive; only a claim that can win opens the transaction.
    /// </summary>
    private async Task<ClaimOutcome?> TakeoverWithoutWritingAsync(SqlSession session, RunClaim claim)
    {
        var sql = $"SELECT {RunColumns}, {dialect.NowMs} AS now FROM {tables.Runs} WHERE job = @job AND slot = @slot";
        var row = await session.QueryFirstAsync(sql, ("job", claim.Job), ("slot", claim.Slot)).ConfigureAwait(false);

        if (row is null) return new ClaimOutcome.None();

        var run = ToRunRow(row);
        // Our own claim, sent again: the transaction answers it.
        if (run.Token == claim.Token) return null;

        // Any other step writes (the lease ran out): the transaction decides who takes over.
        var step = ClaimStep.Decide(run, claim, row.Long("now"));
        return step is ClaimStep.Answer answer ? answer.Outcome : null;
    }

    /// <summary>Performs the writes the decision asks for, and answers the claim.</summary>
    private async Task<ClaimOutcome> ActAsync(SqlSession session, RunClaim claim, ClaimStep step, long now)
    {
        switch (step)
        {
            case ClaimStep.Answer answer:
                return answer.Outcome;
            case ClaimStep.GiveUp:
                await InterruptAsync(session, claim.Job, claim.Slot).ConfigureAwait(false);
                return new ClaimOutcome.Done();
            case ClaimStep.TakeOver takeOver:
                await TakeOverAsync(session, claim, takeOver.Attempt, now).ConfigureAwait(false);
                return await WinAsync(session, claim, takeOver.Attempt, now).ConfigureAwait(false);
            default: // ClaimStep.Start
                var refused = await StartRunAsync(session, claim, now).ConfigureAwait(false);
                return refused ?? await WinAsync(session, claim, 1, now).ConfigureAwait(false);
        }
    }

    /// <summary>Inserts a new run, or says why it must not start.</summary>
    private async Task<ClaimOutcome?> StartRunAsync(SqlSession session, RunClaim claim, long now)
    {
        var refused = await SettleOtherRunsAsync(session, claim, now).ConfigureAwait(false);
        if (refused is not null) return refused;

        var sql = $"""
            INSERT INTO {tables.Runs} (job, slot, attempt, max_attempts, released, token, status, instance_id, lease_until)
            VALUES (@job, @slot, 1, @maxAttempts, 0, @token, 'running', @instanceId, @leaseUntil)
            """;
        var leaseUntil = now + (long)claim.Lease.TotalMilliseconds;
        await session.ExecuteAsync(sql, ("job", claim.Job), ("slot", claim.Slot), ("maxAttempts", claim.MaxAttempts), ("token", claim.Token), ("instanceId", claim.InstanceId), ("leaseUntil", leaseUntil)).ConfigureAwait(false);
        return null;
    }

    /// <summary>
    /// Refuses to overlap a live run of the job; otherwise ends older runs whose runner died. A refused
    /// claim writes nothing.
    /// </summary>
    private async Task<ClaimOutcome?> SettleOtherRunsAsync(SqlSession session, RunClaim claim, long now)
    {
        var sql = $"SELECT slot, lease_until FROM {tables.Runs}{dialect.LockHint} WHERE job = @job AND status = 'running'{dialect.ForUpdate}";
        var rows = await session.QueryAsync(sql, ("job", claim.Job)).ConfigureAwait(false);
        var others = rows.Select(row => (Slot: row.Long("slot"), Alive: row.Long("lease_until") >= now)).ToList();

        var live = others.Where(other => other.Alive).Select(other => other.Slot).ToList();
        if (claim.Overlap == OverlapPolicy.Skip && live.Count > 0) return new ClaimOutcome.Overlap(live.Min());

        // An older run died and a newer one is starting: stop trying to finish the old one.
        foreach (var dead in others.Where(other => !other.Alive && other.Slot < claim.Slot))
        {
            await InterruptAsync(session, claim.Job, dead.Slot).ConfigureAwait(false);
        }

        return null;
    }

    /// <summary>Records the won attempt, trims the job's finished runs, and answers the claim.</summary>
    private async Task<ClaimOutcome> WinAsync(SqlSession session, RunClaim claim, int attempt, long now)
    {
        await history.RecordAttemptAsync(session, claim, attempt, now).ConfigureAwait(false);
        await TrimRunsAsync(session, claim, now).ConfigureAwait(false);
        return new ClaimOutcome.Won(attempt);
    }

    /// <summary>Gives the run's next attempt to this claim.</summary>
    private async Task TakeOverAsync(SqlSession session, RunClaim claim, int attempt, long now)
    {
        var sql = $"""
            UPDATE {tables.Runs}
            SET attempt = @attempt, token = @token, instance_id = @instanceId, lease_until = @leaseUntil
            WHERE job = @job AND slot = @slot
            """;
        var leaseUntil = now + (long)claim.Lease.TotalMilliseconds;
        await session.ExecuteAsync(sql, ("attempt", attempt), ("token", claim.Token), ("instanceId", claim.InstanceId), ("leaseUntil", leaseUntil), ("job", claim.Job), ("slot", claim.Slot)).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes the job's finished runs beyond the newest <see cref="ClaimInput.HistorySize"/>, but never
    /// a run in progress, or one young enough that a late claim could still ask for it.
    /// </summary>
    private async Task TrimRunsAsync(SqlSession session, RunClaim claim, long now)
    {
        var sql = $"""
            DELETE FROM {tables.Runs}
            WHERE job = @job AND status <> 'running' AND slot < @youngFrom
              AND slot <= (SELECT slot
                           FROM {tables.Runs}
                           WHERE job = @job
                           ORDER BY slot DESC
                           {dialect.Page("@historySize", "1")})
            """;
        var youngFrom = now - (long)(claim.Keep?.TotalMilliseconds ?? 0);
        await session.ExecuteAsync(sql, ("job", claim.Job), ("youngFrom", youngFrom), ("historySize", claim.HistorySize)).ConfigureAwait(false);
    }

    /// <summary>Ends a run that no instance will retry.</summary>
    private async Task InterruptAsync(SqlSession session, string job, long slot) =>
        await session.ExecuteAsync($"UPDATE {tables.Runs} SET status = 'interrupted' WHERE job = @job AND slot = @slot", ("job", job), ("slot", slot)).ConfigureAwait(false);

    /// <summary>The database's clock, in Unix milliseconds: every claim decides by it, never by the instance's.</summary>
    private async Task<long> ClockAsync(SqlSession session)
    {
        var row = await session.QueryFirstAsync($"SELECT {dialect.NowMs} AS now").ConfigureAwait(false);
        return row?.Long("now") ?? throw new AnarchonException(AnarchonErrorCode.Store, "The database returned no clock");
    }

    /// <summary>The run's row, locked until the transaction ends; null when nobody claimed the run.</summary>
    private async Task<RunRow?> ReadRunAsync(SqlSession session, RunClaim claim)
    {
        var sql = $"""
            SELECT {RunColumns}
            FROM {tables.Runs}{dialect.LockHint}
            WHERE job = @job AND slot = @slot{dialect.ForUpdate}
            """;
        var row = await session.QueryFirstAsync(sql, ("job", claim.Job), ("slot", claim.Slot)).ConfigureAwait(false);
        return row is null ? null : ToRunRow(row);
    }

    private static RunRow ToRunRow(SqlRow row) => new(
        Attempt: row.Int("attempt"),
        MaxAttempts: row.Int("max_attempts"),
        Released: row.Int("released"),
        Token: row.Text("token") ?? "",
        Status: row.Text("status") ?? "",
        LeaseUntil: row.Long("lease_until"));

    private async Task<bool> ChangedAsync(string sql, (string Name, object? Value)[] parameters, CancellationToken cancellationToken)
    {
        var changed = await SqlSession.RunAsync(open, session => session.ExecuteAsync(sql, parameters), cancellationToken).ConfigureAwait(false);
        return changed > 0;
    }
}
