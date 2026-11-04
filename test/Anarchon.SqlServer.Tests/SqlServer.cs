// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;
using Anarchon.Specification;
using Microsoft.Data.SqlClient;

namespace Anarchon.SqlServer.Tests;

/// <summary>
/// The test server, from <c>test/docker-compose.test.yml</c> by default (port 14330, so a SQL Server
/// on 1433 is never touched). SQLSERVER_URL overrides it with a SqlClient connection string. The
/// probe creates the database it names, and a second one with <c>_snapshot</c> appended.
/// </summary>
public static class SqlServer
{
    public static readonly string ConnectionString =
        RequiredServers.Env("SQLSERVER_URL", "Server=127.0.0.1,14330;Database=anarchon_test;User Id=sa;Password=Anarchon-test-1;TrustServerCertificate=true");

    /// <summary>The same server, on a database with READ_COMMITTED_SNAPSHOT on, as Azure SQL has by default.</summary>
    public static readonly string SnapshotConnectionString =
        new SqlConnectionStringBuilder(ConnectionString) { InitialCatalog = $"{new SqlConnectionStringBuilder(ConnectionString).InitialCatalog}_snapshot" }.ConnectionString;

    /// <summary>Skips the test when the server is down, or fails it when REQUIRE_SERVERS names "sqlserver".</summary>
    public static Task RequireAsync() =>
        RequiredServers.RequireAsync("sqlserver", "SQL Server", new SqlConnectionStringBuilder(ConnectionString).DataSource, IsUpAsync);

    /// <summary>A table name of its own for one test: tests never drop their tables.</summary>
    public static string Table(string test) => $"t_{TestStore.NewScope(test)}";

    /// <summary>Runs one statement on the test database (or another), outside any backend.</summary>
    public static async Task<object?> ScalarAsync(string sql, string? connectionString = null)
    {
        await using var connection = new SqlConnection(connectionString ?? ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        return await command.ExecuteScalarAsync();
    }

    private static async Task<bool> IsUpAsync()
    {
        try
        {
            await CreateDatabaseAsync(ConnectionString, snapshot: false);
            await CreateDatabaseAsync(SnapshotConnectionString, snapshot: true);
            return true;
        }
        catch (SqlException)
        {
            return false;
        }
    }

    private static async Task CreateDatabaseAsync(string connectionString, bool snapshot)
    {
        var database = new SqlConnectionStringBuilder(connectionString).InitialCatalog;
        var master = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "master", ConnectTimeout = 5 }.ConnectionString;
        // A new database has no connections yet, so switching it to snapshot reads waits for nobody.
        var setup = snapshot ? $"ALTER DATABASE [{database}] SET READ_COMMITTED_SNAPSHOT ON" : "";
        await ScalarAsync($"IF DB_ID(N'{database}') IS NULL BEGIN CREATE DATABASE [{database}]; {setup} END", master);
    }
}

/// <summary>The test database for the shared tests: a scope is a table.</summary>
public sealed class SqlServerStore(string connectionString) : TestStore
{
    public override Task RequireAsync() => SqlServer.RequireAsync();

    public override IBackend Create(string scope) =>
        new SqlServerBackend(new SqlServerBackendOptions { ConnectionString = connectionString, Table = $"t_{scope}" });
}

public sealed class SqlServerLeaseRulesTests() : LeaseRulesTests(new SqlServerStore(SqlServer.ConnectionString));

public sealed class SqlServerSchedulerLeaseTests() : SchedulerLeaseTests(new SqlServerStore(SqlServer.ConnectionString));

/// <summary>The lease rules where reads see the last committed row instead of waiting for a writer.</summary>
public sealed class SqlServerSnapshotLeaseRulesTests() : LeaseRulesTests(new SqlServerStore(SqlServer.SnapshotConnectionString));

public sealed class SqlServerContractTests
{
    [Fact]
    public async Task instances_racing_for_the_same_runs_get_exactly_one_winner_each() =>
        await BackendContract.RaceAsync(await FactoryAsync("race", SqlServer.ConnectionString));

    [Fact]
    public async Task instances_racing_on_a_snapshot_database_get_exactly_one_winner_each() =>
        await BackendContract.RaceAsync(await FactoryAsync("snapshot_race", SqlServer.SnapshotConnectionString));

    [Fact]
    public async Task the_same_claim_sent_twice_still_wins_and_another_instance_loses() =>
        await BackendContract.ResendAsync(await FactoryAsync("resend", SqlServer.ConnectionString));

    [Fact]
    public async Task history_joins_claims_and_results_newest_first() =>
        await BackendContract.JoinHistoryAsync(await FactoryAsync("history", SqlServer.ConnectionString));

    private static async Task<Func<IBackend>> FactoryAsync(string test, string connectionString)
    {
        await SqlServer.RequireAsync();
        var table = SqlServer.Table(test);
        return () => new SqlServerBackend(new SqlServerBackendOptions { ConnectionString = connectionString, Table = table });
    }
}
