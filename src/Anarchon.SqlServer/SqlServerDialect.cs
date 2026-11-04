// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Data;
using System.Data.Common;
using Anarchon.Sql;
using Microsoft.Data.SqlClient;

namespace Anarchon.SqlServer;

/// <summary>
/// SQL Server: claims of one job take turns on an application lock (<c>sp_getapplock</c>) that the
/// claim's transaction owns, and read the run's rows with update locks held until commit.
/// </summary>
internal sealed class SqlServerDialect : SqlDialect
{
    public static readonly SqlServerDialect Instance = new();

    /// <summary>SQL Server's errors when another instance created the same table or index at the same moment.</summary>
    private const int TableExists = 2714;
    private const int IndexExists = 1913;

    public override string BigInt => "BIGINT";

    /// <summary>
    /// 400 characters (800 bytes) keep the key (job, slot, attempt) under SQL Server's 900-byte limit.
    /// A binary collation compares job names case-sensitively, as Postgres and SQLite do.
    /// </summary>
    public override string KeyText => "NVARCHAR(400) COLLATE Latin1_General_100_BIN2";

    public override string Text => "NVARCHAR(MAX) COLLATE Latin1_General_100_BIN2";

    /// <summary>The time of the statement, not of the transaction; <c>DATEDIFF_BIG</c> needs SQL Server 2016.</summary>
    public override string NowMs => "DATEDIFF_BIG(millisecond, '19700101', SYSUTCDATETIME())";

    /// <summary>
    /// Update locks on the rows read, held until commit. Not HOLDLOCK: its key-range locks would make
    /// claims of different jobs wait for, and deadlock with, each other. The application lock already
    /// keeps other claims from inserting the job's rows.
    /// </summary>
    public override string LockHint => " WITH (UPDLOCK, ROWLOCK)";

    /// <summary>
    /// An exclusive lock on the job's name, released at commit. A resource name holds 255 characters:
    /// longer names that start alike share a lock, so their claims only wait longer. The procedure
    /// reports a deadlock or a timeout by its return value, not an error, hence the THROW.
    /// </summary>
    public override string SerializeClaims(string table) => $"""
        DECLARE @resource NVARCHAR(255) = LEFT(N'{table}:' + @job, 255);
        DECLARE @granted INT;
        EXEC @granted = sp_getapplock @Resource = @resource, @LockMode = 'Exclusive', @LockOwner = 'Transaction';
        IF @granted < 0 THROW 50000, 'Anarchon: the lock on the job''s claims was not granted', 1;
        """;

    public override string CreateTable(string name, string columns) =>
        OneAtATime($"IF OBJECT_ID(N'{name}', N'U') IS NULL CREATE TABLE {name} (\n{columns}\n);");

    public override string CreateIndex(string name, string table, string columns) =>
        OneAtATime($"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{name}' AND object_id = OBJECT_ID(N'{table}')) CREATE INDEX {name} ON {table} ({columns});");

    /// <summary>2601 and 2627 are SQL Server's duplicate key errors.</summary>
    public override string InsertIfNew(string table, string columns, string values) => OneAtATime($"""
        BEGIN TRY
          INSERT INTO {table} ({columns}) VALUES ({values});
        END TRY
        BEGIN CATCH
          IF ERROR_NUMBER() NOT IN (2601, 2627) THROW;
        END CATCH
        """);

    public override string Page(string offset, string limit) => $"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY";

    /// <summary>
    /// READ COMMITTED, named: a pooled connection can keep the isolation level an app set on it, and
    /// under SNAPSHOT the update locks would turn into update conflicts.
    /// </summary>
    public override async ValueTask<DbTransaction> BeginClaimAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        return await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
    }

    public override bool IsAlreadyCreated(DbException error) => error is SqlException { Number: TableExists or IndexExists };

    /// <summary>
    /// A setup statement in a transaction that holds one application lock for the whole database:
    /// SQL Server can deadlock sessions that create tables at the same moment, even different tables,
    /// so the instances of every service starting together take turns.
    /// </summary>
    private static string OneAtATime(string statement) => $"""
        BEGIN TRY
          BEGIN TRANSACTION;
          DECLARE @granted INT;
          EXEC @granted = sp_getapplock @Resource = N'anarchon:setup', @LockMode = 'Exclusive', @LockOwner = 'Transaction';
          IF @granted < 0 THROW 50000, 'Anarchon: the lock on creating tables was not granted', 1;
          {statement}
          COMMIT;
        END TRY
        BEGIN CATCH
          IF @@TRANCOUNT > 0 ROLLBACK;
          THROW;
        END CATCH
        """;
}
