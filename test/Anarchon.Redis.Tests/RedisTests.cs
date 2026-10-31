// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;

namespace Anarchon.Redis.Tests;

public sealed class RedisTests
{
    [Theory]
    [InlineData("redis-7")]
    [InlineData("valkey")]
    public async Task idmp_refuses_valkey_and_redis_older_than_8_6_and_points_to_the_other_strategies(string name)
    {
        var server = await Servers.RequireAsync(name);
        var backend = new RedisBackend(new RedisBackendOptions { Configuration = server.Url, Strategy = RedisStrategy.Idmp, Prefix = Servers.Prefix("idmp") });

        var error = await Assert.ThrowsAsync<AnarchonException>(() => backend.InitAsync(CancellationToken.None));
        await backend.CloseAsync();

        Assert.Equal(AnarchonErrorCode.Server, error.Code);
        Assert.Contains(name == "valkey" ? "this server is Valkey" : "this server is Redis 7.0", error.Message, StringComparison.Ordinal);
        Assert.Contains("StreamId or Script", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task a_strategy_takes_only_its_own_settings_and_values_in_range()
    {
        await using var unused = await ConnectionMultiplexer.ConnectAsync("localhost:1,abortConnect=false,connectTimeout=100");
        Assert.Throws<ArgumentException>(() => new RedisBackend(new RedisBackendOptions { ScriptWindow = TimeSpan.FromMinutes(10) }));
        Assert.Throws<ArgumentException>(() => new RedisBackend(new RedisBackendOptions { Strategy = RedisStrategy.Script, IdmpMaxSize = 10 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RedisBackend(new RedisBackendOptions { Strategy = RedisStrategy.Idmp, IdmpDuration = TimeSpan.FromDays(2) }));
        Assert.Throws<ArgumentException>(() => new RedisBackend(new RedisBackendOptions { Configuration = "localhost", Connection = unused }));

        var script = new RedisBackend(new RedisBackendOptions { Strategy = RedisStrategy.Script, ScriptWindow = TimeSpan.FromMinutes(10) });
        Assert.Equal(("redis:script", TimeSpan.FromMinutes(10), false), (script.Name, script.DedupeWindow, script.Runs is not null));
        Assert.Equal(("redis:lease", TimeSpan.MaxValue, true), (new RedisBackend("localhost").Name, new RedisBackend("localhost").DedupeWindow, new RedisBackend("localhost").Runs is not null));
    }

    [Fact]
    public void the_script_strategy_refuses_a_job_whose_late_claims_could_outlive_its_window()
    {
        var scheduler = new Scheduler(new SchedulerOptions
        {
            Backend = new RedisBackend(new RedisBackendOptions { Strategy = RedisStrategy.Script, ScriptWindow = TimeSpan.FromSeconds(30) }),
        });

        var window = Assert.Throws<AnarchonException>(() => scheduler.Every("slow", TimeSpan.FromMinutes(5), _ => Task.CompletedTask, new() { MaxLate = TimeSpan.FromMinutes(1) }));
        var history = Assert.Throws<AnarchonException>(() => scheduler.Every("short", TimeSpan.FromSeconds(1), _ => Task.CompletedTask, new() { MaxLate = TimeSpan.FromSeconds(10), HistorySize = 5 }));
        var ttl = Assert.Throws<AnarchonException>(() => scheduler.Every("brief", TimeSpan.FromMinutes(1), _ => Task.CompletedTask, new() { MaxLate = TimeSpan.FromSeconds(5), Jitter = TimeSpan.Zero, HistoryTtl = TimeSpan.FromSeconds(30) }));

        Assert.Equal((AnarchonErrorCode.UnsafeJob, AnarchonErrorCode.UnsafeJob, AnarchonErrorCode.UnsafeJob), (window.Code, history.Code, ttl.Code));
        Assert.Contains("HistoryTtl must be longer than the Script strategy's window", ttl.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void the_idmp_strategy_refuses_a_job_whose_late_claims_could_outlive_the_ids_it_keeps()
    {
        var scheduler = new Scheduler(new SchedulerOptions
        {
            Backend = new RedisBackend(new RedisBackendOptions { Strategy = RedisStrategy.Idmp, IdmpMaxSize = 5 }),
        });

        var error = Assert.Throws<AnarchonException>(() => scheduler.Every("short", TimeSpan.FromSeconds(1), _ => Task.CompletedTask, new() { MaxLate = TimeSpan.FromSeconds(10) }));

        Assert.Equal(AnarchonErrorCode.UnsafeJob, error.Code);
        Assert.Contains("IdmpMaxSize (5) runs", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task the_keys_record_their_store_format_and_a_newer_format_is_refused()
    {
        var server = await Servers.RequireAsync("redis");
        var prefix = Servers.Prefix("format");
        await using var admin = await Servers.AdminAsync(server);
        var backend = new RedisBackend(new RedisBackendOptions { Configuration = server.Url, Prefix = prefix });

        await backend.InitAsync(CancellationToken.None);
        var recorded = await admin.GetDatabase().StringGetAsync($"{prefix}format");
        await admin.GetDatabase().StringSetAsync($"{prefix}format", StoreFormat.Current + 1);
        var error = await Assert.ThrowsAsync<AnarchonException>(() => backend.InitAsync(CancellationToken.None));
        await backend.CloseAsync();

        Assert.Equal(StoreFormat.Current.ToString(System.Globalization.CultureInfo.InvariantCulture), recorded.ToString());
        Assert.Equal(AnarchonErrorCode.StoreFormat, error.Code);
    }

    [Theory]
    [MemberData(nameof(Servers.Names), MemberType = typeof(Servers))]
    public async Task claims_keep_working_after_the_server_forgets_its_scripts(string name)
    {
        var server = await Servers.RequireAsync(name);
        await using var admin = await Servers.AdminAsync(server);
        var prefix = Servers.Prefix("flush");
        var lease = new RedisBackend(new RedisBackendOptions { Configuration = server.Url, Prefix = prefix });
        var script = new RedisBackend(new RedisBackendOptions { Configuration = server.Url, Prefix = prefix, Strategy = RedisStrategy.Script });
        await lease.InitAsync(CancellationToken.None);
        await script.InitAsync(CancellationToken.None);
        var slot = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        Assert.True(await lease.ClaimAsync(Claim("a", slot), CancellationToken.None));
        Assert.True(await script.ClaimAsync(Claim("b", slot), CancellationToken.None));

        await admin.GetServers()[0].ScriptFlushAsync();

        Assert.True(await lease.ClaimAsync(Claim("a", slot + 1), CancellationToken.None));
        Assert.True(await script.ClaimAsync(Claim("b", slot + 1), CancellationToken.None));
        await lease.CloseAsync();
        await script.CloseAsync();
    }

    [Fact]
    public async Task a_takeover_keeps_the_fields_of_a_run_it_does_not_know_so_an_older_instance_never_drops_what_a_newer_one_wrote()
    {
        var server = await Servers.RequireAsync("redis");
        await using var admin = await Servers.AdminAsync(server);
        var prefix = Servers.Prefix("fields");
        var backend = new RedisBackend(new RedisBackendOptions { Configuration = server.Url, Prefix = prefix });
        await backend.InitAsync(CancellationToken.None);
        var claim = new RunClaim
        {
            Job = "j",
            Slot = 1_000,
            Token = "a",
            InstanceId = "A",
            HistorySize = 10,
            Mode = ClaimMode.New,
            Reason = RunReason.Scheduled,
            Lease = TimeSpan.FromMilliseconds(100),
            Overlap = OverlapPolicy.Skip,
            MaxAttempts = 3,
        };
        Assert.IsType<ClaimOutcome.Won>(await backend.Runs!.ClaimAsync(claim, CancellationToken.None));
        var state = admin.GetDatabase();
        var key = $"{{{prefix}j}}:state";
        var stored = (await state.HashGetAsync(key, 1_000)).ToString();
        await state.HashSetAsync(key, 1_000, stored.TrimEnd('}') + ",\"future\":\"kept\"}");

        await Task.Delay(200);
        var takeover = await backend.Runs.ClaimAsync(claim with { Token = "b", InstanceId = "B", Mode = ClaimMode.Takeover, Reason = RunReason.Takeover }, CancellationToken.None);
        var after = (await state.HashGetAsync(key, 1_000)).ToString();
        await backend.CloseAsync();

        Assert.Equal(new ClaimOutcome.Won(2), takeover);
        Assert.Contains("\"future\":\"kept\"", after, StringComparison.Ordinal);
    }

    [Fact]
    public async Task a_manual_run_on_stream_id_does_not_take_a_run_from_the_instances_that_claim_it()
    {
        var server = await Servers.RequireAsync("redis");
        var backend = new RedisBackend(new RedisBackendOptions { Configuration = server.Url, Strategy = RedisStrategy.StreamId, Prefix = Servers.Prefix("manual") });
        await backend.InitAsync(CancellationToken.None);
        var due = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1_000;

        await backend.RecordManualRunAsync(Claim("m", due + 500), CancellationToken.None);
        var scheduled = await backend.ClaimAsync(Claim("s", due), CancellationToken.None);
        var history = await backend.HistoryAsync("job", 10, CancellationToken.None);
        await backend.CloseAsync();

        Assert.True(scheduled);
        Assert.Equal([(due + 500, true), (due, false)], history.Select(r => (r.Slot, r.IsManual)));
    }

    [Fact]
    public async Task history_skips_a_stored_result_that_is_not_json_and_keeps_the_claim()
    {
        var server = await Servers.RequireAsync("redis");
        await using var admin = await Servers.AdminAsync(server);
        var prefix = Servers.Prefix("corrupt");
        var backend = new RedisBackend(new RedisBackendOptions { Configuration = server.Url, Strategy = RedisStrategy.StreamId, Prefix = prefix });
        await backend.InitAsync(CancellationToken.None);
        var due = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1_000;

        await backend.ClaimAsync(Claim("a", due), CancellationToken.None);
        await admin.GetDatabase().SortedSetAddAsync($"{prefix}job:results", "not json", due);
        var history = await backend.HistoryAsync("job", 10, CancellationToken.None);
        await backend.CloseAsync();

        Assert.Equal([due], history.Select(r => r.Slot));
    }

    [Fact]
    public async Task with_a_host_add_anarchon_runs_declared_jobs_on_redis()
    {
        var server = await Servers.RequireAsync("redis");
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddAnarchon(options => options.InstanceId = "host-1")
            .UseRedis(options =>
            {
                options.Configuration = server.Url;
                options.Prefix = Servers.Prefix("hosting");
            })
            .AddJobs<Reports>();
        using var host = builder.Build();

        await host.StartAsync();
        var scheduler = host.Services.GetRequiredService<Scheduler>();
        var run = await scheduler.RunNowAsync("nightly-report");
        var history = await scheduler.HistoryAsync("nightly-report");
        await host.StopAsync();

        Assert.Equal(("redis:lease", RunStatus.Success), (scheduler.Backend.Name, run.Status));
        Assert.Equal(("host-1", true), (history[0].InstanceId, history[0].IsManual));
    }

    public sealed class Reports
    {
        [Cron("0 2 * * *", Name = "nightly-report", TimeZone = "Europe/Athens")]
        public Task Build(JobContext ctx) => Task.Delay(10, ctx.CancellationToken);
    }

    private static ClaimInput Claim(string by, long slot) =>
        new() { Job = "job", Slot = slot, Token = Guid.NewGuid().ToString(), InstanceId = by, HistorySize = 100 };
}
