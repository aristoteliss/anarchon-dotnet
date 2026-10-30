// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Globalization;
using Anarchon.Backends;
using Anarchon.Specification;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace Anarchon.Postgres.Tests;

/// <summary>What is specific to PostgreSQL. The shared contract and lease rules are in Postgres.cs.</summary>
public sealed class PostgresTests
{
    [Fact]
    public void table_names_are_checked_and_a_connection_string_and_a_data_source_are_not_both_taken()
    {
        using var dataSource = NpgsqlDataSource.Create("Host=localhost");

        Assert.Throws<ArgumentException>(() => new PostgresBackend(new PostgresBackendOptions { Table = "runs; DROP TABLE x" }));
        Assert.Throws<ArgumentException>(() => new PostgresBackend(new PostgresBackendOptions { Table = "1runs" }));
        Assert.Throws<ArgumentException>(() => new PostgresBackend(new PostgresBackendOptions { ConnectionString = "Host=localhost", DataSource = dataSource }));
        var backend = new PostgresBackend(new PostgresBackendOptions { Table = "jobs.cron_runs" });
        Assert.Equal(("postgres", TimeSpan.MaxValue, true), (backend.Name, backend.DedupeWindow, backend.Runs is not null));
    }

    [Fact]
    public async Task a_data_source_of_the_app_is_shared_and_stays_open_after_stop()
    {
        await Postgres.RequireAsync();
        await using var dataSource = NpgsqlDataSource.Create(Postgres.ConnectionString);
        var table = Postgres.Table("own");

        // The contract closes every backend it made when it ends.
        await BackendContract.RaceAsync(() => new PostgresBackend(new PostgresBackendOptions { DataSource = dataSource, Table = table }));

        await using var command = dataSource.CreateCommand("SELECT 1");
        Assert.Equal(1, await command.ExecuteScalarAsync());
    }

    [Fact]
    public async Task instances_starting_together_on_new_tables_all_start()
    {
        await Postgres.RequireAsync();
        var table = Postgres.Table("together");
        var backends = Enumerable.Range(0, 5).Select(_ => new PostgresBackend(new PostgresBackendOptions { ConnectionString = Postgres.ConnectionString, Table = table })).ToList();

        await Task.WhenAll(backends.Select(backend => backend.InitAsync(CancellationToken.None)));
        await Task.WhenAll(backends.Select(backend => backend.CloseAsync()));
    }

    [Fact]
    public async Task tables_may_live_in_a_schema_of_their_own()
    {
        await Postgres.RequireAsync();
        var schema = Postgres.Table("schema");
        await Postgres.ScalarAsync($"CREATE SCHEMA {schema}");
        var backend = new PostgresBackend(new PostgresBackendOptions { ConnectionString = Postgres.ConnectionString, Table = $"{schema}.cron_runs" });

        await backend.InitAsync(CancellationToken.None);
        var won = await backend.ClaimAsync(Claim(1_000, "A"), CancellationToken.None);
        await backend.CloseAsync();

        Assert.True(won);
        Assert.Equal(1L, await Postgres.ScalarAsync($"SELECT count(*) FROM {schema}.cron_runs_attempts"));
    }

    [Fact]
    public async Task the_tables_record_their_store_format_and_a_newer_format_is_refused()
    {
        await Postgres.RequireAsync();
        var table = Postgres.Table("format");
        var first = new PostgresBackend(new PostgresBackendOptions { ConnectionString = Postgres.ConnectionString, Table = table });
        await first.InitAsync(CancellationToken.None);
        await first.CloseAsync();

        var recorded = await Postgres.ScalarAsync($"SELECT value FROM {table}_meta WHERE name = 'format'");
        await Postgres.ScalarAsync($"UPDATE {table}_meta SET value = '{StoreFormat.Current + 1}' WHERE name = 'format'");
        var later = new PostgresBackend(new PostgresBackendOptions { ConnectionString = Postgres.ConnectionString, Table = table });
        var error = await Assert.ThrowsAsync<AnarchonException>(() => later.InitAsync(CancellationToken.None));
        await later.CloseAsync();

        Assert.Equal(StoreFormat.Current.ToString(CultureInfo.InvariantCulture), recorded);
        Assert.Equal(AnarchonErrorCode.StoreFormat, error.Code);
        Assert.Contains($"format {StoreFormat.Current + 1}", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task a_backup_checking_on_a_live_run_only_reads()
    {
        await Postgres.RequireAsync();
        var table = Postgres.Table("reads");
        var backend = new PostgresBackend(new PostgresBackendOptions { ConnectionString = Postgres.ConnectionString, Table = table });
        var runs = backend.Runs!;
        await backend.InitAsync(CancellationToken.None);
        // Every claim's transaction counts itself on the job's row of <table>_jobs.
        Task<object?> Transactions() => Postgres.ScalarAsync($"SELECT claimed_at FROM {table}_jobs WHERE job = 'job'");

        var won = await runs.ClaimAsync(Run(5_000, "A", ClaimMode.New), CancellationToken.None);
        var before = await Transactions();
        var running = await runs.ClaimAsync(Run(5_000, "B", ClaimMode.Takeover), CancellationToken.None);
        var none = await runs.ClaimAsync(Run(6_000, "B", ClaimMode.Takeover), CancellationToken.None);
        var after = await Transactions();
        await backend.CloseAsync();

        Assert.Equal((new ClaimOutcome.Won(1), new ClaimOutcome.Running(1), new ClaimOutcome.None()), (won, running, none));
        Assert.Equal(before, after);
    }

    [Fact]
    public async Task with_a_host_add_anarchon_runs_declared_jobs_on_postgres()
    {
        await Postgres.RequireAsync();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(Postgres.ConnectionString));
        builder.Services.AddAnarchon(options => options.InstanceId = "host-1")
            .UsePostgres((services, options) =>
            {
                options.DataSource = services.GetRequiredService<NpgsqlDataSource>();
                options.Table = Postgres.Table("hosting");
            })
            .AddJobs<Reports>();
        using var host = builder.Build();

        await host.StartAsync();
        var scheduler = host.Services.GetRequiredService<Scheduler>();
        var run = await scheduler.RunNowAsync("nightly-report");
        var history = await scheduler.HistoryAsync("nightly-report");
        await host.StopAsync();

        Assert.Equal(("postgres", RunStatus.Success), (scheduler.Backend.Name, run.Status));
        Assert.Equal(("host-1", true, RunReason.Manual), (history[0].InstanceId, history[0].IsManual, history[0].Reason));
    }

    public sealed class Reports
    {
        [Cron("0 2 * * *", Name = "nightly-report")]
        public Task Build(JobContext ctx) => Task.Delay(10, ctx.CancellationToken);
    }

    private static ClaimInput Claim(long slot, string by) =>
        new() { Job = "job", Slot = slot, Token = Guid.NewGuid().ToString(), InstanceId = by, HistorySize = 100 };

    private static RunClaim Run(long slot, string by, ClaimMode mode) => new()
    {
        Job = "job",
        Slot = slot,
        Token = by,
        InstanceId = by,
        HistorySize = 100,
        Mode = mode,
        Reason = mode == ClaimMode.Takeover ? RunReason.Takeover : RunReason.Scheduled,
        Lease = TimeSpan.FromMinutes(1),
        Overlap = OverlapPolicy.Skip,
        MaxAttempts = 3,
    };
}
