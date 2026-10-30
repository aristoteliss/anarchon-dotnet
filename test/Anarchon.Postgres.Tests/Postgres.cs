// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;
using Anarchon.Specification;
using Npgsql;

namespace Anarchon.Postgres.Tests;

/// <summary>
/// The test database, from <c>test/docker-compose.test.yml</c> by default (port 5433, so a Postgres
/// on 5432 is never touched). POSTGRES_URL overrides it with an Npgsql connection string.
/// </summary>
public static class Postgres
{
    public static readonly string ConnectionString =
        RequiredServers.Env("POSTGRES_URL", "Host=localhost;Port=5433;Username=postgres;Password=postgres;Database=anarchon_test");

    /// <summary>Skips the test when the database is down, or fails it when REQUIRE_SERVERS names "postgres".</summary>
    public static Task RequireAsync() => RequiredServers.RequireAsync("postgres", "PostgreSQL", ConnectionString, IsUpAsync);

    /// <summary>A table name of its own for one test: tests never drop their tables.</summary>
    public static string Table(string test) => $"t_{TestStore.NewScope(test)}";

    /// <summary>Runs one statement on the test database, outside any backend.</summary>
    public static async Task<object?> ScalarAsync(string sql)
    {
        await using var dataSource = NpgsqlDataSource.Create(ConnectionString);
        await using var command = dataSource.CreateCommand(sql);
        return await command.ExecuteScalarAsync();
    }

    private static async Task<bool> IsUpAsync()
    {
        try
        {
            await ScalarAsync("SELECT 1");
            return true;
        }
        catch (Exception error) when (error is NpgsqlException or TimeoutException)
        {
            return false;
        }
    }
}

/// <summary>The test database for the shared tests: a scope is a table.</summary>
public sealed class PostgresStore : TestStore
{
    public override Task RequireAsync() => Postgres.RequireAsync();

    public override IBackend Create(string scope) =>
        new PostgresBackend(new PostgresBackendOptions { ConnectionString = Postgres.ConnectionString, Table = $"t_{scope}" });
}

public sealed class PostgresLeaseRulesTests() : LeaseRulesTests(new PostgresStore());

public sealed class PostgresSchedulerLeaseTests() : SchedulerLeaseTests(new PostgresStore());

public sealed class PostgresContractTests
{
    [Fact]
    public async Task instances_racing_for_the_same_runs_get_exactly_one_winner_each() =>
        await BackendContract.RaceAsync(await FactoryAsync("race"));

    [Fact]
    public async Task the_same_claim_sent_twice_still_wins_and_another_instance_loses() =>
        await BackendContract.ResendAsync(await FactoryAsync("resend"));

    [Fact]
    public async Task history_joins_claims_and_results_newest_first() =>
        await BackendContract.JoinHistoryAsync(await FactoryAsync("history"));

    private static async Task<Func<IBackend>> FactoryAsync(string test)
    {
        await Postgres.RequireAsync();
        var table = Postgres.Table(test);
        return () => new PostgresBackend(new PostgresBackendOptions { ConnectionString = Postgres.ConnectionString, Table = table });
    }
}
