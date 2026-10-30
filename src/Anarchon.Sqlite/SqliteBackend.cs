// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Anarchon.Sql;
using Microsoft.Data.Sqlite;

namespace Anarchon.Sqlite;

/// <summary>
/// SQLite 3.24+, on Microsoft.Data.Sqlite, with the same leases, backups, no-overlap, catch-up and
/// hand-over as the Redis Lease strategy. Each claim is one <c>BEGIN IMMEDIATE</c> transaction, and
/// SQLite runs one write transaction at a time; leases run on SQLite's clock.
/// </summary>
/// <remarks>
/// <para>
/// <b>How each run gets exactly one runner.</b> A run is a row with primary key (job, slot). SQLite
/// lets one connection write at a time, and <c>BEGIN IMMEDIATE</c> takes that write lock up front,
/// so two instances' claims can't interleave: the first inserts the run's row and wins, the second
/// waits for the lock (up to the connection string's <c>Default Timeout</c>), then reads that row and
/// sees it taken. The full steps are on <see cref="SqlBackend"/>.
/// </para>
/// <para>
/// A SQLite file only coordinates the processes that open that same file: one machine, or a volume
/// with working file locks (not NFS). For instances on several machines, use PostgreSQL or Redis.
/// The file is switched to write-ahead logging (WAL) on start, so readers don't wait for the writer.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// var scheduler = new Scheduler(new SchedulerOptions { Backend = new SqliteBackend("Data Source=/var/lib/myapp/jobs.db") });
/// </code>
/// </example>
public sealed class SqliteBackend : SqlBackend
{
    /// <summary>3.24: the first version with upserts (INSERT ... ON CONFLICT), which the store format uses.</summary>
    private static readonly Version MinVersion = new(3, 24);

    private readonly string _connectionString;

    /// <summary>A backend on the file a connection string names, on the table <c>anarchon_runs</c>.</summary>
    /// <param name="connectionString">Such as <c>Data Source=/var/lib/myapp/jobs.db</c>.</param>
    /// <exception cref="ArgumentException">The connection string is not valid, or names no file.</exception>
    /// <example>
    /// <code>
    /// new SqliteBackend(builder.Configuration.GetConnectionString("sqlite")!);
    /// </code>
    /// </example>
    public SqliteBackend(string connectionString)
        : this(new SqliteBackendOptions { ConnectionString = connectionString })
    {
    }

    /// <summary>Checks the options. Nothing is opened until the scheduler starts.</summary>
    /// <param name="options">The file and the table.</param>
    /// <exception cref="ArgumentException">
    /// The connection string is not valid or names no file (an in-memory or temporary database is
    /// private to one connection), or the table name is not a plain identifier.
    /// </exception>
    /// <example>
    /// <code>
    /// new SqliteBackend(new SqliteBackendOptions { ConnectionString = "Data Source=/var/lib/myapp/jobs.db", Table = "cron_runs" });
    /// </code>
    /// </example>
    public SqliteBackend(SqliteBackendOptions options)
        : base(TableOf(options), SqliteDialect.Instance)
    {
        _connectionString = FileConnectionString(options.ConnectionString, nameof(options));
    }

    /// <summary><c>sqlite</c>.</summary>
    public override string Name => "sqlite";

    /// <summary>
    /// Opens a pooled connection, and tells SQLite to wait for another process's write lock as long as
    /// the connection string's <c>Default Timeout</c>.
    /// </summary>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>An open connection.</returns>
    protected override async ValueTask<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            // Per connection: SQLite waits in small steps for the lock, and NORMAL is safe with WAL.
            var busyTimeout = string.Create(CultureInfo.InvariantCulture, $"PRAGMA busy_timeout = {BusyTimeoutMs(connection)}");
            await ExecuteAsync(connection, busyTimeout, cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(connection, "PRAGMA synchronous = NORMAL", cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch (Exception)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Refuses SQLite older than 3.24, then switches the file to write-ahead logging.</summary>
    /// <param name="connection">An open connection.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the file is ready.</returns>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.Server"/>: SQLite is older than 3.24.</exception>
    protected override async Task CheckServerAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var text = await ExecuteAsync(connection, "SELECT sqlite_version()", cancellationToken).ConfigureAwait(false);
        if (!Version.TryParse(text, out var version) || version < MinVersion)
        {
            throw new AnarchonException(AnarchonErrorCode.Server, $"The SQLite backend needs SQLite 3.24 or newer (this one is {text})");
        }

        // Switching a new file needs it to itself; the driver retries while another connection holds it.
        await ExecuteAsync(connection, "PRAGMA journal_mode = WAL", cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Closes the pooled connections to the file, so SQLite folds the write-ahead log back into it.</summary>
    /// <returns>A completed task.</returns>
    protected override ValueTask CloseConnectionsAsync()
    {
        using var connection = new SqliteConnection(_connectionString);
        SqliteConnection.ClearPool(connection);
        return ValueTask.CompletedTask;
    }

    /// <summary>The connection's <c>Default Timeout</c> in milliseconds; 0 there means no limit.</summary>
    private static long BusyTimeoutMs(SqliteConnection connection) =>
        connection.DefaultTimeout > 0 ? Math.Min(connection.DefaultTimeout * 1_000L, int.MaxValue) : int.MaxValue;

    [SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Fixed statements and numbers the backend computed; no user text.")]
    private static async Task<string?> ExecuteAsync(DbConnection connection, string sql, CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();
        await using (command.ConfigureAwait(false))
        {
            command.CommandText = sql;
            var reply = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return Convert.ToString(reply, CultureInfo.InvariantCulture);
        }
    }

    /// <summary>The table name, refused when it has a schema: a SQLite index can't name one.</summary>
    private static string TableOf(SqliteBackendOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Table?.Contains('.', StringComparison.Ordinal) == true)
        {
            throw new ArgumentException($"Invalid table name \"{options.Table}\": a SQLite table takes no schema", nameof(options));
        }

        return options.Table!;
    }

    /// <summary>
    /// The connection string, normalized; refused when it names no file every instance can open.
    /// Messages name the data source only: the rest may hold a password.
    /// </summary>
    private static string FileConnectionString(string connectionString, string paramName)
    {
        ArgumentNullException.ThrowIfNull(connectionString, paramName);
        SqliteConnectionStringBuilder builder;
        try
        {
            builder = new SqliteConnectionStringBuilder(connectionString);
        }
        catch (ArgumentException error)
        {
            throw new ArgumentException($"Invalid SQLite connection string: {error.Message}", paramName, error);
        }

        var privateDatabase = builder.Mode == SqliteOpenMode.Memory || builder.DataSource is "" or ":memory:";
        if (privateDatabase)
        {
            throw new ArgumentException(
                $"SQLite needs a file that every instance opens, such as \"Data Source=/var/lib/myapp/jobs.db\" (got Data Source \"{builder.DataSource}\"): an in-memory or temporary database is private to one connection",
                paramName);
        }

        return builder.ToString();
    }
}
