// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Data.Common;
using Anarchon.Sql;
using Microsoft.Data.SqlClient;

namespace Anarchon.SqlServer;

/// <summary>
/// SQL Server 2016+ and Azure SQL, on Microsoft.Data.SqlClient, with the same leases, backups,
/// no-overlap, catch-up and hand-over as the Redis Lease strategy. Each claim is one short
/// transaction; leases run on the database's clock.
/// </summary>
/// <remarks>
/// <para>
/// <b>How each run gets exactly one runner.</b> A run is a row with primary key (job, slot), and a
/// claim is a READ COMMITTED transaction that first takes an exclusive application lock on the job
/// (<c>sp_getapplock</c>), held until commit. Claims of one job happen one after another: the first
/// inserts the run's row and wins, every later one reads that row and sees it taken (or sees its lease
/// ran out and takes the next attempt). The full steps are on <see cref="SqlBackend"/>.
/// </para>
/// <para>
/// Text columns are <c>NVARCHAR</c> with a binary collation, so job names compare case-sensitively,
/// and a job name holds up to 400 characters. The Node.js package has no SQL Server backend: only
/// .NET instances share these tables.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// var scheduler = new Scheduler(new SchedulerOptions { Backend = new SqlServerBackend(builder.Configuration.GetConnectionString("sqlserver")!) });
/// </code>
/// </example>
public sealed class SqlServerBackend : SqlBackend
{
    /// <summary>SQL Server's error for a function it doesn't know, such as <c>DATEDIFF_BIG</c> before 2016.</summary>
    private const int UnknownFunction = 195;

    private readonly string _connectionString;

    /// <summary>A backend on the database a connection string names, on the table <c>anarchon_runs</c>.</summary>
    /// <param name="connectionString">Such as <c>Server=db;Database=app;User Id=app;Password=...;Encrypt=true</c>.</param>
    /// <exception cref="ArgumentException">The connection string is empty.</exception>
    /// <example>
    /// <code>
    /// new SqlServerBackend(builder.Configuration.GetConnectionString("sqlserver")!);
    /// </code>
    /// </example>
    public SqlServerBackend(string connectionString)
        : this(new SqlServerBackendOptions { ConnectionString = connectionString })
    {
    }

    /// <summary>Checks the options. Nothing connects until the scheduler starts.</summary>
    /// <param name="options">Where SQL Server is, and the table.</param>
    /// <exception cref="ArgumentException">The connection string is missing or empty, or the table name is not a plain identifier.</exception>
    /// <example>
    /// <code>
    /// new SqlServerBackend(new SqlServerBackendOptions { ConnectionString = connectionString, Table = "billing_cron_runs" });
    /// </code>
    /// </example>
    public SqlServerBackend(SqlServerBackendOptions options)
        : base(TableOf(options), SqlServerDialect.Instance)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("Give SqlServerBackend a ConnectionString, such as \"Server=db;Database=app;User Id=app;Password=...;Encrypt=true\"", nameof(options));
        }

        _connectionString = options.ConnectionString;
    }

    /// <summary><c>sqlserver</c>.</summary>
    public override string Name => "sqlserver";

    /// <summary>Takes a connection from SqlClient's pool for the connection string.</summary>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>An open connection.</returns>
    protected override async ValueTask<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(_connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch (Exception)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Refuses a server that can't compute the clock: SQL Server older than 2016.</summary>
    /// <param name="connection">An open connection.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the server is checked.</returns>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.Server"/>: the server is older than SQL Server 2016.</exception>
    protected override async Task CheckServerAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var command = connection.CreateCommand();
        await using (command.ConfigureAwait(false))
        {
            // Azure SQL reports an old major version, so the clock itself is the check.
            command.CommandText = $"SELECT {SqlServerDialect.Instance.NowMs}";
            try
            {
                await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (SqlException error) when (error.Number == UnknownFunction)
            {
                throw new AnarchonException(AnarchonErrorCode.Server, $"The SQL Server backend needs SQL Server 2016 or newer, or Azure SQL ({error.Message})", error);
            }
        }
    }

    private static string TableOf(SqlServerBackendOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.Table;
    }
}
