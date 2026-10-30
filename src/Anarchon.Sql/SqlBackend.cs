// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Data.Common;
using System.Text.RegularExpressions;
using Anarchon.Backends;

namespace Anarchon.Sql;

/// <summary>
/// A backend on an SQL database, with the same lease rules as the Redis Lease strategy: backup
/// runners, retries, catch-up, hand-over on stop and no overlapping runs. Leases run on the
/// database's clock. <c>PostgresBackend</c> and <c>SqliteBackend</c> derive from it; another database
/// needs a <see cref="SqlDialect"/>, a way to open connections and a server check.
/// </summary>
/// <remarks>
/// <para>
/// <b>How each run gets exactly one runner.</b> A run is one row with primary key (job, slot), and a
/// claim is one transaction:
/// </para>
/// <list type="number">
/// <item>Make claims of this job wait for each other (<see cref="SqlDialect.SerializeClaims"/>, or a
/// transaction that holds the database's write lock).</item>
/// <item>Read the run's row and the database's clock. No row: insert it with attempt 1, the claim's
/// token and a lease from now, and win; the next claim waits at step 1, then reads this row. A live
/// lease: the run is running, and the caller becomes a backup. A lease that ran out: take the next
/// attempt, unless the run used up its attempts. Any other status: done.</item>
/// <item>Commit.</item>
/// </list>
/// <para>
/// Heartbeat, release and finish are single UPDATEs that match the claim's token: once a backup took
/// over, the old runner changes nothing and learns it lost the run. A backup checking on a live run
/// only reads.
/// </para>
/// <para>
/// Tables, created on start: <c>&lt;table&gt;</c> (one row per run), <c>&lt;table&gt;_attempts</c> (one row
/// per attempt: the history) and <c>&lt;table&gt;_meta</c> (the store format), plus the dialect's own.
/// The Node.js package uses the same tables, so instances of both can share them.
/// </para>
/// </remarks>
/// <example>Another database, in outline
/// <code>
/// sealed class EmbeddedBackend(string connectionString) : SqlBackend("anarchon_runs", new EmbeddedDialect())
/// {
///     public override string Name =&gt; "embedded";
///     protected override async ValueTask&lt;DbConnection&gt; OpenConnectionAsync(CancellationToken ct)
///     {
///         var connection = new EmbeddedConnection(connectionString);
///         await connection.OpenAsync(ct);
///         return connection;
///     }
///     protected override Task CheckServerAsync(DbConnection connection, CancellationToken ct) =&gt; Task.CompletedTask;
/// }
/// </code>
/// </example>
public abstract partial class SqlBackend : IBackend
{
    private readonly SqlDialect _dialect;
    private readonly SqlTables _tables;
    private readonly SqlRunStore _runs;
    private readonly SqlHistory _history;
    private readonly Lock _gate = new();
    private Task? _ready;

    /// <summary>Checks the table name. Nothing connects until <see cref="InitAsync"/>.</summary>
    /// <param name="table">
    /// The name of the runs table, optionally with a schema (<c>jobs.cron_runs</c>): letters, digits
    /// and underscores, not starting with a digit.
    /// </param>
    /// <param name="dialect">What the database does differently.</param>
    /// <exception cref="ArgumentException">The table name is not a plain identifier.</exception>
    protected SqlBackend(string table, SqlDialect dialect)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(dialect);
        if (!Identifier().IsMatch(table)) throw new ArgumentException($"Invalid table name \"{table}\": use letters, digits and underscores, optionally with a schema (\"jobs.cron_runs\")", nameof(table));

        _dialect = dialect;
        _tables = new SqlTables(table);
        _history = new SqlHistory(_tables, dialect, OpenReadyAsync);
        _runs = new SqlRunStore(_tables, dialect, OpenReadyAsync, _history);
    }

    /// <summary>The database, such as <c>postgres</c> or <c>sqlite</c>.</summary>
    public abstract string Name { get; }

    /// <summary>Forever: a run is never forgotten while a late claim could still ask for it.</summary>
    public TimeSpan DedupeWindow => TimeSpan.MaxValue;

    /// <summary>The run store: an SQL backend always has leases.</summary>
    public IRunStore Runs => _runs;

    /// <summary>Connects, checks the server, creates the tables and checks their store format. A failed start is tried again by the next call.</summary>
    /// <param name="cancellationToken">Stops waiting; the setup goes on for the next caller.</param>
    /// <returns>A task that completes once the tables are ready.</returns>
    public Task InitAsync(CancellationToken cancellationToken) => ReadyAsync().WaitAsync(cancellationToken);

    /// <summary>Nothing per job: all jobs share the tables.</summary>
    /// <param name="job">Not used.</param>
    /// <param name="historySize">Not used.</param>
    /// <param name="cancellationToken">Not used.</param>
    /// <returns>A completed task.</returns>
    public Task PrepareJobAsync(string job, int historySize, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>The plain one-winner claim, with a long lease and no backups. The scheduler claims through <see cref="Runs"/> instead.</summary>
    /// <param name="input">The run and who claims it.</param>
    /// <param name="cancellationToken">Stops waiting; the claim may still reach the database.</param>
    /// <returns>Whether this claim won the run.</returns>
    public Task<bool> ClaimAsync(ClaimInput input, CancellationToken cancellationToken) => _runs.ClaimWithoutBackupsAsync(input, cancellationToken);

    /// <summary>Claims a run of its own for a manual run, at the first free millisecond from its slot.</summary>
    /// <param name="input">The manual run.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The slot that was claimed.</returns>
    public async Task<long?> RecordManualRunAsync(ClaimInput input, CancellationToken cancellationToken) =>
        await _runs.ClaimManualRunAsync(input, cancellationToken).ConfigureAwait(false);

    /// <summary>Writes how the attempt ended on its row in <c>&lt;table&gt;_attempts</c>, and drops attempts older than <paramref name="historyTtl"/>.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="result">How the attempt ended.</param>
    /// <param name="historySize">Not used: claims trim the history by count.</param>
    /// <param name="historyTtl">Drop records older than this.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the result is stored.</returns>
    public Task SaveResultAsync(string job, StoredResult result, int historySize, TimeSpan? historyTtl, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(result);
        return _history.SaveResultAsync(job, result, historyTtl, cancellationToken);
    }

    /// <summary>Reads <c>&lt;table&gt;_attempts</c>, newest first.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="limit">How many attempts to return at most.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The attempts, newest first.</returns>
    public Task<IReadOnlyList<RunRecord>> HistoryAsync(string job, int limit, CancellationToken cancellationToken) =>
        _history.HistoryAsync(job, limit, cancellationToken);

    /// <summary>Closes what <see cref="CloseConnectionsAsync"/> closes. The backend may be started again afterwards.</summary>
    /// <returns>A task that completes once the connections are closed.</returns>
    public async Task CloseAsync()
    {
        lock (_gate)
        {
            _ready = null;
        }

        await CloseConnectionsAsync().ConfigureAwait(false);
    }

    /// <summary>Opens a connection to the database. The caller disposes it.</summary>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>An open connection.</returns>
    protected abstract ValueTask<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Runs once on start, before the tables are created: throws when the server can't run the
    /// backend, such as a version that is too old. It may also set the database up, as SQLite switches
    /// its journal mode here.
    /// </summary>
    /// <param name="connection">An open connection.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the server is checked.</returns>
    protected abstract Task CheckServerAsync(DbConnection connection, CancellationToken cancellationToken);

    /// <summary>Closes the connections or pools the backend opened itself; one passed in stays open. Default: nothing.</summary>
    /// <returns>A task that completes once they are closed.</returns>
    protected virtual ValueTask CloseConnectionsAsync() => ValueTask.CompletedTask;

    /// <summary>Prepares once; a failed start is tried again by the next caller.</summary>
    private Task ReadyAsync()
    {
        lock (_gate)
        {
            if (_ready is null || _ready.IsFaulted || _ready.IsCanceled) _ready = Task.Run(PrepareAsync);
            return _ready;
        }
    }

    private async Task<DbConnection> OpenReadyAsync(CancellationToken cancellationToken)
    {
        await ReadyAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
        return await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Checks the server, creates the tables and checks their store format.</summary>
    private async Task PrepareAsync()
    {
        var connection = await OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            await CheckServerAsync(connection, CancellationToken.None).ConfigureAwait(false);
            var session = new SqlSession(connection, null, CancellationToken.None);
            foreach (var statement in _tables.Create(_dialect))
            {
                await CreateAsync(session, statement).ConfigureAwait(false);
            }

            var format = await session.QueryFirstAsync($"SELECT value FROM {_tables.Meta} WHERE name = @name", ("name", "format")).ConfigureAwait(false);
            StoreFormat.Check(format?.Text("value"), $"The table \"{_tables.Runs}\"");
        }
    }

    private async Task CreateAsync(SqlSession session, string statement)
    {
        try
        {
            await session.ExecuteAsync(statement).ConfigureAwait(false);
        }
        catch (DbException error) when (_dialect.IsAlreadyCreated(error))
        {
            // Another instance created it at the same moment.
        }
    }

    [GeneratedRegex(@"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")]
    private static partial Regex Identifier();
}
