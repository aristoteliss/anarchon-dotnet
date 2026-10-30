// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Diagnostics;
using Anarchon.Backends;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Anarchon.Sqlite.Tests;

/// <summary>
/// What is specific to SQLite. The shared contract and lease rules are in Sqlite.cs; what every SQL
/// backend gets from <c>SqlBackend</c> is tested in Anarchon.Sql.Tests.
/// </summary>
public sealed class SqliteTests
{
    [Theory]
    [InlineData("Data Source=:memory:")]
    [InlineData("Data Source=jobs;Mode=Memory;Cache=Shared")]
    [InlineData("Data Source=")]
    [InlineData("Data Source=jobs.db;Not A Keyword=1")]
    public void a_connection_string_that_names_no_file_every_instance_can_open_is_refused(string connectionString)
    {
        var error = Assert.Throws<ArgumentException>(() => new SqliteBackend(connectionString));

        Assert.Equal("options", error.ParamName);
    }

    [Theory]
    [InlineData("main.runs")]
    [InlineData("runs; DROP TABLE x")]
    public void a_table_with_a_schema_or_that_is_not_an_identifier_is_refused(string table) =>
        Assert.Throws<ArgumentException>(() => new SqliteBackend(new SqliteBackendOptions { Table = table }));

    [Fact]
    public void a_sqlite_backend_has_leases_and_names_itself_sqlite()
    {
        var backend = new SqliteBackend(new SqliteBackendOptions());

        Assert.Equal(("sqlite", TimeSpan.MaxValue, true), (backend.Name, backend.DedupeWindow, backend.Runs is not null));
    }

    [Fact]
    public async Task start_switches_the_file_to_write_ahead_logging()
    {
        var file = SqliteFiles.NewFile("wal");
        var backend = new SqliteBackend(SqliteFiles.ConnectionString(file));

        await backend.InitAsync(CancellationToken.None);
        var mode = await SqliteFiles.ScalarAsync(file, "PRAGMA journal_mode");
        await backend.CloseAsync();

        Assert.Equal("wal", mode);
    }

    [Fact]
    public async Task start_waits_while_another_connection_holds_a_new_file()
    {
        var file = SqliteFiles.NewFile("locked");
        await using var other = new SqliteConnection($"Data Source={file};Pooling=False");
        await other.OpenAsync();
        await using (var hold = other.CreateCommand())
        {
            hold.CommandText = "BEGIN IMMEDIATE; CREATE TABLE other (a INTEGER)";
            await hold.ExecuteNonQueryAsync();
        }

        var commit = Task.Run(async () =>
        {
            await Task.Delay(200);
            await using var done = other.CreateCommand();
            done.CommandText = "COMMIT";
            await done.ExecuteNonQueryAsync();
        });
        var backend = new SqliteBackend(SqliteFiles.ConnectionString(file));
        await backend.InitAsync(CancellationToken.None);
        var won = await backend.ClaimAsync(Claim(1_000, "A"), CancellationToken.None);
        await commit;
        await backend.CloseAsync();

        Assert.True(won);
    }

    [Fact]
    public async Task the_clock_never_reads_earlier_than_the_real_one()
    {
        var backend = new SqliteBackend(SqliteFiles.ConnectionString(SqliteFiles.NewFile("clock")));
        await backend.InitAsync(CancellationToken.None);

        for (var i = 0; i < 1_000; i++)
        {
            // A claim 30 s late with MaxLate 10 s is Late by at least 30 s on the database's clock.
            var slot = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 30_000;
            var outcome = await backend.Runs.ClaimAsync(Run(slot, $"t{i}") with { MaxLate = TimeSpan.FromSeconds(10) }, CancellationToken.None);
            Assert.True(outcome is ClaimOutcome.Late { By.TotalMilliseconds: >= 30_000 }, $"claim {i}: {outcome}");
        }

        await backend.CloseAsync();
    }

    [Fact]
    public async Task stop_closes_the_file_so_sqlite_folds_the_write_ahead_log_back_in()
    {
        var file = SqliteFiles.NewFile("close");
        var backend = new SqliteBackend(SqliteFiles.ConnectionString(file));
        await backend.InitAsync(CancellationToken.None);
        await backend.ClaimAsync(Claim(1_000, "A"), CancellationToken.None);
        var walWhileOpen = File.Exists($"{file}-wal");

        await backend.CloseAsync();

        Assert.True(walWhileOpen);
        Assert.False(File.Exists($"{file}-wal"));
    }

    [Fact]
    public async Task a_write_waits_for_another_process_up_to_the_connection_strings_default_timeout()
    {
        var file = SqliteFiles.NewFile("busy");
        var backend = new SqliteBackend($"Data Source={file};Default Timeout=1");
        await backend.InitAsync(CancellationToken.None);
        await using var other = new SqliteConnection($"Data Source={file};Pooling=False");
        await other.OpenAsync();
        await using var hold = other.CreateCommand();
        hold.CommandText = "BEGIN IMMEDIATE";
        await hold.ExecuteNonQueryAsync();

        var waited = Stopwatch.StartNew();
        var error = await Assert.ThrowsAsync<SqliteException>(() => backend.ClaimAsync(Claim(1_000, "A"), CancellationToken.None));
        waited.Stop();
        await backend.CloseAsync();

        Assert.Equal(5, error.SqliteErrorCode); // SQLITE_BUSY
        Assert.InRange(waited.Elapsed, TimeSpan.FromSeconds(0.9), TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task with_a_host_add_anarchon_runs_declared_jobs_on_sqlite()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddAnarchon(options => options.InstanceId = "host-1")
            .UseSqlite(SqliteFiles.ConnectionString(SqliteFiles.NewFile("hosting")))
            .AddJobs<Reports>();
        using var host = builder.Build();

        await host.StartAsync();
        var scheduler = host.Services.GetRequiredService<Scheduler>();
        var run = await scheduler.RunNowAsync("nightly-report");
        var history = await scheduler.HistoryAsync("nightly-report");
        await host.StopAsync();

        Assert.Equal(("sqlite", RunStatus.Success), (scheduler.Backend.Name, run.Status));
        Assert.Equal(("host-1", true, RunReason.Manual), (history[0].InstanceId, history[0].IsManual, history[0].Reason));
    }

    public sealed class Reports
    {
        [Cron("0 2 * * *", Name = "nightly-report")]
        public Task Build(JobContext ctx) => Task.Delay(10, ctx.CancellationToken);
    }

    private static ClaimInput Claim(long slot, string by) =>
        new() { Job = "job", Slot = slot, Token = Guid.NewGuid().ToString(), InstanceId = by, HistorySize = 100 };

    private static RunClaim Run(long slot, string token) => new()
    {
        Job = "job",
        Slot = slot,
        Token = token,
        InstanceId = "A",
        HistorySize = 100,
        Mode = ClaimMode.New,
        Reason = RunReason.Scheduled,
        Lease = TimeSpan.FromSeconds(5),
        Overlap = OverlapPolicy.Allow,
        MaxAttempts = 1,
    };
}
