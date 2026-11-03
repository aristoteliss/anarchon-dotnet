// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Data.Common;

namespace Anarchon.Sql;

/// <summary>
/// What differs between SQL databases: types, the clock, row locks, how claims take turns, and the
/// syntax of creating tables, of inserting a row once and of paging. The tables' columns, the claim
/// transaction and the history are the same for every database (see <see cref="SqlBackend"/>).
/// Statements name their parameters <c>@name</c>.
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
    /// Written after the table name of a SELECT inside a claim to lock the rows it reads until commit,
    /// such as <c> WITH (UPDLOCK, ROWLOCK)</c>, for a database that locks by table hint instead of
    /// <see cref="ForUpdate"/>. Default: empty.
    /// </summary>
    public virtual string LockHint => "";

    /// <summary>
    /// The type of the text columns that are part of a key or an index: job names, run statuses and
    /// the names in <c>&lt;table&gt;_meta</c>. Default <c>TEXT</c>.
    /// </summary>
    public virtual string KeyText => "TEXT";

    /// <summary>The type of the other text columns, such as tokens, instance ids and error messages. Default <c>TEXT</c>.</summary>
    public virtual string Text => "TEXT";

    /// <summary>A statement that creates a table unless it exists. Default: <c>CREATE TABLE IF NOT EXISTS</c>.</summary>
    /// <param name="name">The table, as validated.</param>
    /// <param name="columns">The column and key definitions, comma separated.</param>
    /// <returns>The statement, safe to run again.</returns>
    public virtual string CreateTable(string name, string columns) => $"CREATE TABLE IF NOT EXISTS {name} (\n{columns}\n)";

    /// <summary>A statement that creates an index unless it exists. Default: <c>CREATE INDEX IF NOT EXISTS</c>.</summary>
    /// <param name="name">The index, without a schema.</param>
    /// <param name="table">The indexed table, as validated.</param>
    /// <param name="columns">The indexed columns, comma separated.</param>
    /// <returns>The statement, safe to run again.</returns>
    public virtual string CreateIndex(string name, string table, string columns) => $"CREATE INDEX IF NOT EXISTS {name} ON {table} ({columns})";

    /// <summary>
    /// An INSERT of one row that does nothing when a row with the same key exists. Default:
    /// <c>INSERT ... ON CONFLICT DO NOTHING</c>.
    /// </summary>
    /// <param name="table">The table, as validated.</param>
    /// <param name="columns">The columns, comma separated.</param>
    /// <param name="values">The values, comma separated, in the order of <paramref name="columns"/>.</param>
    /// <returns>The statement, safe to run again.</returns>
    public virtual string InsertIfNew(string table, string columns, string values) => $"INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT DO NOTHING";

    /// <summary>
    /// Written after the ORDER BY of a SELECT to skip <paramref name="offset"/> rows and return at most
    /// <paramref name="limit"/>. Default: <c>LIMIT limit OFFSET offset</c>.
    /// </summary>
    /// <param name="offset">How many rows to skip: a number or a parameter, such as <c>@historySize</c>.</param>
    /// <param name="limit">How many rows to return at most: a number or a parameter.</param>
    /// <returns>The clause.</returns>
    public virtual string Page(string offset, string limit) => $"LIMIT {limit} OFFSET {offset}";

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
