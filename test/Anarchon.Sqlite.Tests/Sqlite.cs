// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;
using Anarchon.Specification;
using Microsoft.Data.Sqlite;

namespace Anarchon.Sqlite.Tests;

/// <summary>Where the test databases go: SQLITE_DIR, or the temporary folder. Tests leave their files behind.</summary>
public static class SqliteFiles
{
    private static readonly string Folder = RequiredServers.Env("SQLITE_DIR", Path.GetTempPath());

    /// <summary>The file of one scope.</summary>
    public static string File(string scope) => Path.Combine(Folder, $"anarchon-sqlite-{scope}.db");

    /// <summary>A new file for one test.</summary>
    public static string NewFile(string test) => File(TestStore.NewScope(test));

    /// <summary>The connection string of a file.</summary>
    public static string ConnectionString(string file) => $"Data Source={file}";

    /// <summary>Runs one statement on a file, on a connection of its own (not pooled).</summary>
    public static async Task<object?> ScalarAsync(string file, string sql)
    {
        await using var connection = new SqliteConnection($"Data Source={file};Pooling=False");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync();
    }
}

/// <summary>SQLite files for the shared tests: a scope is a file. No server, so nothing is skipped.</summary>
public sealed class SqliteStore : TestStore
{
    public override Task RequireAsync() => Task.CompletedTask;

    public override IBackend Create(string scope) => new SqliteBackend(SqliteFiles.ConnectionString(SqliteFiles.File(scope)));
}

public sealed class SqliteLeaseRulesTests() : LeaseRulesTests(new SqliteStore());

public sealed class SqliteSchedulerLeaseTests() : SchedulerLeaseTests(new SqliteStore());

public sealed class SqliteContractTests
{
    [Fact]
    public async Task instances_racing_for_the_same_runs_get_exactly_one_winner_each()
    {
        var file = SqliteFiles.NewFile("race");
        await BackendContract.RaceAsync(() => new SqliteBackend(SqliteFiles.ConnectionString(file)));
    }

    [Fact]
    public async Task the_same_claim_sent_twice_still_wins_and_another_instance_loses()
    {
        var file = SqliteFiles.NewFile("resend");
        await BackendContract.ResendAsync(() => new SqliteBackend(SqliteFiles.ConnectionString(file)));
    }

    [Fact]
    public async Task history_joins_claims_and_results_newest_first()
    {
        var file = SqliteFiles.NewFile("history");
        await BackendContract.JoinHistoryAsync(() => new SqliteBackend(SqliteFiles.ConnectionString(file)));
    }
}
