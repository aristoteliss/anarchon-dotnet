// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Data.Common;

namespace Anarchon.Sql;

/// <summary>
/// What differs between SQL databases: types, the clock, row locks and how claims take turns. The
/// tables, the claim transaction and the history are the same for every database (see
/// <see cref="SqlBackend"/>). Statements name their parameters <c>@name</c>.
/// </summary>
/// <example>A dialect for a database that serializes writers on its own
/// <code>
/// sealed class EmbeddedDialect : SqlDialect
/// {
///     public override string BigInt =&gt; "INTEGER";
///     public override string NowMs =&gt; "CAST(ROUND((julianday('now') - 2440587.5) * 86400000.0) AS INTEGER)";
/// }
/// </code>
/// </example>
public abstract class SqlDialect
{
    /// <summary>The integer type of the columns that hold Unix milliseconds, such as <c>BIGINT</c>.</summary>
    public abstract string BigInt { get; }

    /// <summary>
    /// An SQL expression for the database's clock in Unix milliseconds. Leases are decided by it,
    /// never by an instance's clock.
    /// </summary>
    public abstract string NowMs { get; }

    /// <summary>
    /// Appended to a SELECT inside a claim to lock the rows it reads until commit, such as
    /// <c> FOR UPDATE</c>. Empty when the claim's transaction already holds the database's write lock.
    /// </summary>
    public virtual string ForUpdate => "";

    /// <summary>
    /// A statement run first in every claim's transaction, with the parameter <c>@job</c>, that makes
    /// claims of one job wait for each other. Null when <see cref="BeginClaimAsync"/> already does.
    /// </summary>
    /// <param name="table">The backend's table, as validated.</param>
    /// <returns>The statement, or null.</returns>
    public virtual string? SerializeClaims(string table) => null;

    /// <summary>Statements that create tables of the dialect's own, run on start after the shared tables.</summary>
    /// <param name="table">The backend's table, as validated.</param>
    /// <returns>The statements, each one safe to run again (<c>IF NOT EXISTS</c>).</returns>
    public virtual IEnumerable<string> ExtraTables(string table) => [];

    /// <summary>Begins the transaction of one claim.</summary>
    /// <param name="connection">An open connection.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The transaction.</returns>
    public virtual ValueTask<DbTransaction> BeginClaimAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        return connection.BeginTransactionAsync(cancellationToken);
    }

    /// <summary>
    /// Whether an error of a <c>CREATE ... IF NOT EXISTS</c> only says another instance created the
    /// same table at the same moment, so starting can go on.
    /// </summary>
    /// <param name="error">The error the driver threw.</param>
    /// <returns>true to ignore the error.</returns>
    public virtual bool IsAlreadyCreated(DbException error) => false;
}
