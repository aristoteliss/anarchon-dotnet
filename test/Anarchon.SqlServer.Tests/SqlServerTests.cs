// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Globalization;
using Anarchon.Backends;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Anarchon.SqlServer.Tests;

/// <summary>What is specific to SQL Server. The shared contract and lease rules are in SqlServer.cs.</summary>
public sealed class SqlServerTests
{
    [Fact]
    public void table_names_are_checked_and_a_connection_string_is_required()
    {
        Assert.Throws<ArgumentException>(() => new SqlServerBackend(new SqlServerBackendOptions { ConnectionString = "Server=db", Table = "runs; DROP TABLE x" }));
        Assert.Throws<ArgumentException>(() => new SqlServerBackend(new SqlServerBackendOptions { ConnectionString = "Server=db", Table = "1runs" }));
        Assert.Throws<ArgumentException>(() => new SqlServerBackend(new SqlServerBackendOptions()));
        Assert.Throws<ArgumentException>(() => new SqlServerBackend(" "));
        var backend = new SqlServerBackend(new SqlServerBackendOptions { ConnectionString = "Server=db", Table = "jobs.cron_runs" });
        Assert.Equal(("sqlserver", TimeSpan.MaxValue, true), (backend.Name, backend.DedupeWindow, backend.Runs is not null));
    }

    [Fact]
    public async Task instances_of_several_services_starting_together_on_new_tables_all_start()
    {
        await SqlServer.RequireAsync();

        // SQL Server can deadlock sessions that create tables at the same moment, even different tables.
        for (var round = 0; round < 3; round++)
        {
            var tables = Enumerable.Range(0, 8).Select(_ => SqlServer.Table("together")).ToList();
            var backends = tables.SelectMany(table => Enumerable.Range(0, 3).Select(_ => Backend(table))).ToList();
            await Task.WhenAll(backends.Select(backend => backend.InitAsync(CancellationToken.None)));
            await Task.WhenAll(backends.Select(backend => backend.CloseAsync()));
        }
    }

    [Fact]
    public async Task tables_may_live_in_a_schema_of_their_own()
    {
        await SqlServer.RequireAsync();
        var schema = SqlServer.Table("schema");
        await SqlServer.ScalarAsync($"EXEC('CREATE SCHEMA {schema}')");
        var backend = Backend($"{schema}.cron_runs");

        await backend.InitAsync(CancellationToken.None);
        var won = await backend.ClaimAsync(Claim("job", 1_000), CancellationToken.None);
        await backend.CloseAsync();

        Assert.True(won);
        Assert.Equal(1, await SqlServer.ScalarAsync($"SELECT count(*) FROM {schema}.cron_runs_attempts"));
    }

    [Fact]
    public async Task the_tables_record_their_store_format_and_a_newer_format_is_refused()
    {
        await SqlServer.RequireAsync();
        var table = SqlServer.Table("format");
        var first = Backend(table);
        await first.InitAsync(CancellationToken.None);
        await first.CloseAsync();
        var again = Backend(table);
        await again.InitAsync(CancellationToken.None);
        await again.CloseAsync();

        var recorded = await SqlServer.ScalarAsync($"SELECT value FROM {table}_meta WHERE name = 'format'");
        await SqlServer.ScalarAsync($"UPDATE {table}_meta SET value = '{StoreFormat.Current + 1}' WHERE name = 'format'");
        var later = Backend(table);
        var error = await Assert.ThrowsAsync<AnarchonException>(() => later.InitAsync(CancellationToken.None));
        await later.CloseAsync();

        Assert.Equal(StoreFormat.Current.ToString(CultureInfo.InvariantCulture), recorded);
        Assert.Equal(AnarchonErrorCode.StoreFormat, error.Code);
    }

    [Fact]
    public async Task job_names_that_differ_only_in_case_are_different_jobs()
    {
        await SqlServer.RequireAsync();
        var backend = Backend(SqlServer.Table("case"));

        var upper = await backend.ClaimAsync(Claim("Report", 1_000), CancellationToken.None);
        var lower = await backend.ClaimAsync(Claim("report", 1_000), CancellationToken.None);
        await backend.CloseAsync();

        Assert.Equal((true, true), (upper, lower));
    }

    [Fact]
    public async Task a_job_name_of_400_characters_claims_and_keeps_history()
    {
        await SqlServer.RequireAsync();
        var backend = Backend(SqlServer.Table("long"));
        var job = new string('j', 400);

        var won = await backend.ClaimAsync(Claim(job, 1_000), CancellationToken.None);
        var lost = await backend.ClaimAsync(Claim(job, 1_000), CancellationToken.None);
        var history = await backend.HistoryAsync(job, 10, CancellationToken.None);
        await backend.CloseAsync();

        Assert.Equal((true, false, 1), (won, lost, history.Count));
    }

    [Fact]
    public async Task with_a_host_add_anarchon_runs_declared_jobs_on_sql_server()
    {
        await SqlServer.RequireAsync();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddAnarchon(options => options.InstanceId = "host-1")
            .UseSqlServer(options =>
            {
                options.ConnectionString = SqlServer.ConnectionString;
                options.Table = SqlServer.Table("hosting");
            })
            .AddJobs<Reports>();
        using var host = builder.Build();

        await host.StartAsync();
        var scheduler = host.Services.GetRequiredService<Scheduler>();
        var run = await scheduler.RunNowAsync("nightly-report");
        var history = await scheduler.HistoryAsync("nightly-report");
        await host.StopAsync();

        Assert.Equal(("sqlserver", RunStatus.Success), (scheduler.Backend.Name, run.Status));
        Assert.Equal(("host-1", true, RunReason.Manual), (history[0].InstanceId, history[0].IsManual, history[0].Reason));
    }

    [Fact]
    public void use_sql_server_takes_a_connection_string_or_options()
    {
        var withString = new ServiceCollection();
        withString.AddAnarchon().UseSqlServer("Server=db;Database=app");
        var withOptions = new ServiceCollection();
        withOptions.AddAnarchon().UseSqlServer((_, options) => options.ConnectionString = "Server=db;Database=app");

        using var first = withString.BuildServiceProvider();
        using var second = withOptions.BuildServiceProvider();

        Assert.Equal(("sqlserver", "sqlserver"), (first.GetRequiredService<Scheduler>().Backend.Name, second.GetRequiredService<Scheduler>().Backend.Name));
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddAnarchon().UseSqlServer(options => options.Table = "bad table").Services.BuildServiceProvider().GetRequiredService<Scheduler>());
    }

    public sealed class Reports
    {
        [Cron("0 2 * * *", Name = "nightly-report")]
        public Task Build(JobContext ctx) => Task.Delay(10, ctx.CancellationToken);
    }

    private static SqlServerBackend Backend(string table) =>
        new(new SqlServerBackendOptions { ConnectionString = SqlServer.ConnectionString, Table = table });

    private static ClaimInput Claim(string job, long slot) =>
        new() { Job = job, Slot = slot, Token = Guid.NewGuid().ToString(), InstanceId = "A", HistorySize = 100 };
}
