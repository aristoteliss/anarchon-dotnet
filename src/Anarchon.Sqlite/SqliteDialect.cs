// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Data;
using System.Data.Common;
using Anarchon.Sql;
using Microsoft.Data.Sqlite;

namespace Anarchon.Sqlite;

/// <summary>
/// SQLite: one writer at a time. A claim's transaction takes the write lock up front
/// (<c>BEGIN IMMEDIATE</c>), so claims can't interleave and need no row locks.
/// </summary>
internal sealed class SqliteDialect : SqlDialect
{
    public static readonly SqliteDialect Instance = new();

    public override string BigInt => "INTEGER";

    /// <summary>Rounded, not cut: julianday is a float, and a whole millisecond can come out as x.9999.</summary>
    public override string NowMs => "CAST(ROUND((julianday('now') - 2440587.5) * 86400000.0) AS INTEGER)";

    /// <summary><c>BEGIN IMMEDIATE</c>: a deferred transaction would take the write lock only at its first write, and could then fail.</summary>
    public override ValueTask<DbTransaction> BeginClaimAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var sqlite = (SqliteConnection)connection;
        DbTransaction transaction = sqlite.BeginTransaction(IsolationLevel.Serializable, deferred: false);
        return ValueTask.FromResult(transaction);
    }
}
