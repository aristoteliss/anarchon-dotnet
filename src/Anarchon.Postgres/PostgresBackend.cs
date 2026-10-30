// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Data.Common;
using System.Globalization;
using Anarchon.Sql;
using Npgsql;

namespace Anarchon.Postgres;

/// <summary>
/// PostgreSQL 9.5+, on Npgsql, with the same leases, backups, no-overlap, catch-up and hand-over as
/// the Redis Lease strategy. Each claim is one short transaction; leases run on the database's clock.
/// </summary>
/// <remarks>
/// <b>How each run gets exactly one runner.</b> A run is a row with primary key (job, slot), and a
/// claim is a transaction that starts with an upsert of the job's row in <c>&lt;table&gt;_jobs</c>.
/// Postgres locks that row until commit, so claims of one job happen one after another: the first
/// inserts the run's row and wins, every later one reads that row and sees it taken (or sees its lease
/// ran out and takes the next attempt). The full steps are on <see cref="SqlBackend"/>.
/// </remarks>
/// <example>
/// <code>
/// var scheduler = new Scheduler(new SchedulerOptions { Backend = new PostgresBackend(builder.Configuration.GetConnectionString("postgres")!) });
/// </code>
/// </example>
/// <example>The data source your app already has, and tables in your own schema; stop leaves it open
/// <code>
/// await using var dataSource = NpgsqlDataSource.Create(connectionString);
/// var backend = new PostgresBackend(new PostgresBackendOptions { DataSource = dataSource, Table = "jobs.cron_runs" });
/// </code>
/// </example>
public sealed class PostgresBackend : SqlBackend
{
    private const string DefaultConnectionString = "Host=localhost;Database=postgres";

    /// <summary>9.5: the first version with INSERT ... ON CONFLICT, which claims and the store format use.</summary>
    private const int MinServerVersionNum = 90_500;

    private readonly string? _connectionString;
    private readonly NpgsqlDataSource? _dataSource;
    private readonly Lock _gate = new();
    private NpgsqlDataSource? _owned;

    /// <summary>A backend that opens its own pool with an Npgsql connection string, on the table <c>anarchon_runs</c>.</summary>
    /// <param name="connectionString">Such as <c>Host=db;Database=app;Username=app;Password=...</c>.</param>
    /// <example>
    /// <code>
    /// new PostgresBackend(builder.Configuration.GetConnectionString("postgres")!);
    /// </code>
    /// </example>
    public PostgresBackend(string connectionString)
        : this(new PostgresBackendOptions { ConnectionString = connectionString })
    {
    }

    /// <summary>Checks the options. Nothing connects until the scheduler starts.</summary>
    /// <param name="options">Where Postgres is, and the table.</param>
    /// <exception cref="ArgumentException">Both a connection string and a data source are given, or the table name is not a plain identifier.</exception>
    /// <example>
    /// <code>
    /// new PostgresBackend(new PostgresBackendOptions { ConnectionString = connectionString, Table = "billing_cron_runs" });
    /// </code>
    /// </example>
    public PostgresBackend(PostgresBackendOptions options)
        : base(TableOf(options), PostgresDialect.Instance)
    {
        if (options.ConnectionString is not null && options.DataSource is not null)
        {
            throw new ArgumentException("Give PostgresBackend a ConnectionString or a DataSource, not both", nameof(options));
        }

        _connectionString = options.ConnectionString;
        _dataSource = options.DataSource;
    }

    /// <summary><c>postgres</c>.</summary>
    public override string Name => "postgres";

    /// <summary>Takes a connection from the data source's pool.</summary>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>An open connection.</returns>
    protected override async ValueTask<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken) =>
        await DataSource().OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Refuses a server older than PostgreSQL 9.5.</summary>
    /// <param name="connection">An open connection.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the server is checked.</returns>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.Server"/>: the server is older than 9.5.</exception>
    protected override async Task CheckServerAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var command = connection.CreateCommand();
        await using (command.ConfigureAwait(false))
        {
            command.CommandText = "SHOW server_version_num";
            var reply = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            var version = int.TryParse(Convert.ToString(reply, CultureInfo.InvariantCulture), NumberStyles.Integer, CultureInfo.InvariantCulture, out var number) ? number : 0;
            if (version >= MinServerVersionNum) return;

            throw new AnarchonException(AnarchonErrorCode.Server, $"The Postgres backend needs PostgreSQL 9.5 or newer (server_version_num {reply})");
        }
    }

    /// <summary>Disposes the pool the backend opened with <see cref="PostgresBackendOptions.ConnectionString"/>. A data source that was passed in stays open.</summary>
    /// <returns>A task that completes once the pool is closed.</returns>
    protected override async ValueTask CloseConnectionsAsync()
    {
        NpgsqlDataSource? owned;
        lock (_gate)
        {
            owned = _owned;
            _owned = null;
        }

        if (owned is not null) await owned.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>The data source passed in, or the one this backend opened (once, until <see cref="SqlBackend.CloseAsync"/>).</summary>
    private NpgsqlDataSource DataSource()
    {
        if (_dataSource is not null) return _dataSource;

        lock (_gate)
        {
            return _owned ??= NpgsqlDataSource.Create(_connectionString ?? DefaultConnectionString);
        }
    }

    private static string TableOf(PostgresBackendOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.Table;
    }
}
