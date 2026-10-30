// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Data;
using System.Data.Common;
using Anarchon.Backends;
using Anarchon.Specification;
using Microsoft.Data.Sqlite;

namespace Anarchon.Sql.Tests;

/// <summary>
/// The smallest <see cref="SqlBackend"/>: SQLite files through Microsoft.Data.Sqlite, built the way a
/// third party puts Anarchon on a database it doesn't support. No server, so these tests always run.
/// It counts what the base class asks of it.
/// </summary>
public sealed class EmbeddedBackend(string file, string table = "runs", SqlDialect? dialect = null)
    : SqlBackend(table, dialect ?? new EmbeddedDialect())
{
    private readonly string _connectionString = new SqliteConnectionStringBuilder { DataSource = file }.ToString();
    private int _serverChecks;
    private int _closes;

    public override string Name => "embedded";

    /// <summary>How many of the first server checks fail, as a server that is too old would.</summary>
    public int FailingChecks { get; init; }

    public int ServerChecks => Volatile.Read(ref _serverChecks);

    public int Closes => Volatile.Read(ref _closes);

    protected override async ValueTask<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        // Writers from other connections wait their turn instead of failing.
        await using var pragma = connection.CreateCommand();
        pragma.CommandText = "PRAGMA busy_timeout = 10000";
        await pragma.ExecuteNonQueryAsync(cancellationToken);
        return connection;
    }

    protected override Task CheckServerAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        if (Interlocked.Increment(ref _serverChecks) > FailingChecks) return Task.CompletedTask;

        throw new AnarchonException(AnarchonErrorCode.Server, "The embedded backend refuses this server");
    }

    protected override ValueTask CloseConnectionsAsync()
    {
        Interlocked.Increment(ref _closes);
        SqliteConnection.ClearPool(new SqliteConnection(_connectionString));
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// SQLite: one writer at a time, taken up front by <c>BEGIN IMMEDIATE</c>, so claims can't
/// interleave and need no row locks. Counts the claims' transactions.
/// </summary>
public class EmbeddedDialect : SqlDialect
{
    private int _transactions;

    public override string BigInt => "INTEGER";

    public override string NowMs => "CAST(ROUND((julianday('now') - 2440587.5) * 86400000.0) AS INTEGER)";

    public int Transactions => Volatile.Read(ref _transactions);

    public override ValueTask<DbTransaction> BeginClaimAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _transactions);
        DbTransaction transaction = ((SqliteConnection)connection).BeginTransaction(IsolationLevel.Serializable, deferred: false);
        return ValueTask.FromResult(transaction);
    }
}

/// <summary>Where the test databases go: SQLITE_DIR, or the temporary folder. Tests leave their files behind.</summary>
public static class Embedded
{
    private static readonly string Folder = RequiredServers.Env("SQLITE_DIR", Path.GetTempPath());

    /// <summary>A database file of its own for one test or scope.</summary>
    public static string File(string scope) => Path.Combine(Folder, $"anarchon-sql-{scope}.db");

    /// <summary>A new file for one test.</summary>
    public static string NewFile(string test) => File(TestStore.NewScope(test));

    /// <summary>Runs one statement on a file, outside any backend.</summary>
    public static async Task<object?> ScalarAsync(string file, string sql)
    {
        await using var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = file, Pooling = false }.ToString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync();
    }
}

/// <summary>The shared tests on SQLite files: a scope is a file.</summary>
public sealed class EmbeddedStore : TestStore
{
    public override Task RequireAsync() => Task.CompletedTask;

    public override IBackend Create(string scope) => new EmbeddedBackend(Embedded.File(scope));
}

public sealed class EmbeddedLeaseRulesTests() : LeaseRulesTests(new EmbeddedStore());

public sealed class EmbeddedSchedulerLeaseTests() : SchedulerLeaseTests(new EmbeddedStore());

public sealed class EmbeddedContractTests
{
    [Fact]
    public async Task instances_racing_for_the_same_runs_get_exactly_one_winner_each()
    {
        var file = Embedded.NewFile("race");
        await BackendContract.RaceAsync(() => new EmbeddedBackend(file));
    }

    [Fact]
    public async Task the_same_claim_sent_twice_still_wins_and_another_instance_loses()
    {
        var file = Embedded.NewFile("resend");
        await BackendContract.ResendAsync(() => new EmbeddedBackend(file));
    }

    [Fact]
    public async Task history_joins_claims_and_results_newest_first()
    {
        var file = Embedded.NewFile("history");
        await BackendContract.JoinHistoryAsync(() => new EmbeddedBackend(file));
    }
}
