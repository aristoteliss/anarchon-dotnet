// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Text;
using Anarchon.Backends;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NATS.Client.Core;
using NATS.Client.KeyValueStore;

namespace Anarchon.Nats.Tests;

/// <summary>What is specific to NATS: the MsgId claim, the stream, and the Lease strategy's bucket. The shared suites are in Nats.cs.</summary>
public sealed class NatsTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task options_are_checked_when_the_backend_is_made()
    {
        await using var unused = new NatsConnection();

        Assert.Throws<ArgumentException>(() => new NatsBackend(new NatsBackendOptions { ConnectionString = NatsServer.Url, Connection = unused }));
        Assert.Throws<ArgumentException>(() => new NatsBackend(new NatsBackendOptions { Strategy = (NatsStrategy)7 }));
        Assert.Throws<ArgumentException>(() => new NatsBackend(new NatsBackendOptions { Strategy = NatsStrategy.Lease, Stream = "CRON.RUNS" }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new NatsBackend(new NatsBackendOptions { DuplicateWindow = TimeSpan.Zero }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new NatsBackend(new NatsBackendOptions { HistorySize = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new NatsBackend(new NatsBackendOptions { Replicas = 0 }));

        var msgId = new NatsBackend(new NatsBackendOptions { DuplicateWindow = TimeSpan.FromMinutes(10) });
        var lease = new NatsBackend(new NatsBackendOptions { Strategy = NatsStrategy.Lease });
        Assert.Equal(("nats", TimeSpan.FromMinutes(10), false), (msgId.Name, msgId.DedupeWindow, msgId.Runs is not null));
        Assert.Equal(("nats:lease", TimeSpan.MaxValue, true), (lease.Name, lease.DedupeWindow, lease.Runs is not null));
    }

    [Theory]
    [InlineData(NatsStrategy.MsgId)]
    [InlineData(NatsStrategy.Lease)]
    public void job_names_must_be_simple_and_history_is_kept_by_count_only(NatsStrategy strategy)
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new NatsBackend(new NatsBackendOptions { Strategy = strategy }) });

        var dotted = Assert.Throws<AnarchonException>(() => scheduler.Every("a.b", TimeSpan.FromMinutes(1), _ => Task.CompletedTask));
        var ttl = Assert.Throws<AnarchonException>(() => scheduler.Every("a", TimeSpan.FromMinutes(1), _ => Task.CompletedTask, new() { HistoryTtl = TimeSpan.FromHours(1) }));

        Assert.Equal((AnarchonErrorCode.Unsupported, AnarchonErrorCode.Unsupported), (dotted.Code, ttl.Code));
        Assert.Contains("HistoryTtl is not supported by nats", ttl.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void the_msg_id_strategy_takes_no_lease_options()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new NatsBackend(new NatsBackendOptions()) });

        var error = Assert.Throws<AnarchonException>(() => scheduler.Every("a", TimeSpan.FromMinutes(1), _ => Task.CompletedTask, new() { MaxAttempts = 3 }));

        Assert.Equal(AnarchonErrorCode.Unsupported, error.Code);
    }

    [Fact]
    public async Task reading_history_leaves_no_consumer_behind_on_the_server()
    {
        await NatsServer.RequireAsync();
        var (stream, prefix) = NatsServer.Scope("consumers");
        var backend = NatsServer.Backend(stream, prefix, NatsStrategy.MsgId);
        await backend.InitAsync(CancellationToken.None);
        await backend.ClaimAsync(Claim(1_000, "A"), CancellationToken.None);
        var (connection, js) = await NatsServer.ConnectAsync();
        await using (connection)
        {
            for (var i = 0; i < 3; i++)
            {
                Assert.Single(await backend.HistoryAsync("job", 10, CancellationToken.None));
            }

            var info = await js.GetStreamAsync(stream);
            await backend.CloseAsync();
            Assert.Equal(0, info.Info.State.ConsumerCount);
        }
    }

    [Fact]
    public async Task the_stream_keeps_as_many_runs_per_job_as_the_job_that_asks_for_the_most()
    {
        await NatsServer.RequireAsync();
        var (stream, prefix) = NatsServer.Scope("size");
        var backend = NatsServer.Backend(stream, prefix, NatsStrategy.MsgId, historySize: 10);
        await backend.InitAsync(CancellationToken.None);
        await backend.PrepareJobAsync("small", 5, CancellationToken.None);
        await backend.PrepareJobAsync("large", 250, CancellationToken.None);
        var (connection, js) = await NatsServer.ConnectAsync();
        await using (connection)
        {
            var info = await js.GetStreamAsync(stream);
            await backend.CloseAsync();
            Assert.Equal(250, info.Info.Config.MaxMsgsPerSubject);
        }
    }

    [Fact]
    public async Task with_lease_the_runs_of_a_job_are_one_key_in_the_bucket_and_history_stays_in_the_stream()
    {
        await NatsServer.RequireAsync();
        var (stream, prefix) = NatsServer.Scope("bucket");
        var backend = NatsServer.Backend(stream, prefix, NatsStrategy.Lease);
        await backend.InitAsync(CancellationToken.None);
        Assert.True(await backend.ClaimAsync(Claim(1_000, "A"), CancellationToken.None));
        Assert.True(await backend.ClaimAsync(Claim(2_000, "A"), CancellationToken.None));
        var (connection, js) = await NatsServer.ConnectAsync();
        await using (connection)
        {
            var store = await new NatsKVContext(js).GetStoreAsync($"{stream}_RUNS");
            var value = await store.GetEntryAsync<string>("job");
            var history = await backend.HistoryAsync("job", 10, CancellationToken.None);
            await backend.CloseAsync();

            Assert.Contains("\"1000\":", value.Value, StringComparison.Ordinal);
            Assert.Contains("\"2000\":", value.Value, StringComparison.Ordinal);
            Assert.Equal([2_000L, 1_000L], history.Select(record => record.Slot));
        }
    }

    [Fact]
    public async Task a_takeover_keeps_the_fields_it_does_not_know_so_an_older_instance_never_drops_what_a_newer_one_wrote()
    {
        await NatsServer.RequireAsync();
        var (stream, prefix) = NatsServer.Scope("fields");
        var backend = NatsServer.Backend(stream, prefix, NatsStrategy.Lease);
        await backend.InitAsync(CancellationToken.None);
        var claim = Run(1_000, "a", "A", ClaimMode.New);
        Assert.IsType<ClaimOutcome.Won>(await backend.Runs!.ClaimAsync(claim, CancellationToken.None));
        var (connection, js) = await NatsServer.ConnectAsync();
        await using (connection)
        {
            var store = await new NatsKVContext(js).GetStoreAsync($"{stream}_RUNS");
            var entry = await store.GetEntryAsync<string>("job");
            var added = entry.Value!.Replace("\"t\":\"a\"", "\"t\":\"a\",\"future\":\"kept\"", StringComparison.Ordinal).Replace("{\"runs\"", "{\"newer\":1,\"runs\"", StringComparison.Ordinal);
            await store.UpdateAsync("job", added, entry.Revision);

            await Task.Delay(200);
            var takeover = await backend.Runs.ClaimAsync(claim with { Token = "b", InstanceId = "B", Mode = ClaimMode.Takeover, Reason = RunReason.Takeover }, CancellationToken.None);
            var after = (await store.GetEntryAsync<string>("job")).Value;
            await backend.CloseAsync();

            Assert.Equal(new ClaimOutcome.Won(2), takeover);
            Assert.Contains("\"future\":\"kept\"", after, StringComparison.Ordinal);
            Assert.Contains("\"newer\":1", after, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task a_bucket_that_records_a_newer_store_format_is_refused()
    {
        await NatsServer.RequireAsync();
        var (stream, prefix) = NatsServer.Scope("format");
        var first = NatsServer.Backend(stream, prefix, NatsStrategy.Lease);
        await first.InitAsync(CancellationToken.None);
        await first.CloseAsync();
        var (connection, js) = await NatsServer.ConnectAsync();
        await using (connection)
        {
            var store = await new NatsKVContext(js).GetStoreAsync($"{stream}_RUNS");
            await store.PutAsync("meta.format", Encoding.UTF8.GetBytes($"{StoreFormat.Current + 1}"));

            var later = NatsServer.Backend(stream, prefix, NatsStrategy.Lease);
            var error = await Assert.ThrowsAsync<AnarchonException>(() => later.InitAsync(CancellationToken.None));
            await later.CloseAsync();

            Assert.Equal(AnarchonErrorCode.StoreFormat, error.Code);
        }
    }

    [Fact]
    public async Task writes_to_one_job_from_many_instances_at_once_all_land_each_decided_again_after_losing_a_race()
    {
        await NatsServer.RequireAsync();
        var (stream, prefix) = NatsServer.Scope("races");
        var backends = Enumerable.Range(0, 6).Select(_ => NatsServer.Backend(stream, prefix, NatsStrategy.Lease)).ToList();
        await Task.WhenAll(backends.Select(backend => backend.InitAsync(CancellationToken.None)));

        // Every instance claims a run of its own, of one job, at the same moment: one key, many writers.
        var outcomes = await Task.WhenAll(backends.Select((backend, i) =>
            backend.Runs!.ClaimAsync(Run(1_000 + i, $"t{i}", $"I{i}", ClaimMode.New) with { Overlap = OverlapPolicy.Allow }, CancellationToken.None)));
        var active = await backends[0].Runs!.ActiveAsync("job", CancellationToken.None);
        await Task.WhenAll(backends.Select(backend => backend.CloseAsync()));

        Assert.All(outcomes, outcome => Assert.Equal(new ClaimOutcome.Won(1), outcome));
        Assert.Equal(6, active.Count);
    }

    [Theory]
    [InlineData(NatsStrategy.MsgId)]
    [InlineData(NatsStrategy.Lease)]
    public async Task a_manual_run_shows_as_running_then_as_success_in_history(NatsStrategy strategy)
    {
        await NatsServer.RequireAsync();
        var (stream, prefix) = NatsServer.Scope("now");
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = NatsServer.Backend(stream, prefix, strategy), InstanceId = "api-1" });
        var started = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        scheduler.Every("slow", TimeSpan.FromHours(1), async _ =>
        {
            started.SetResult();
            await release.Task;
        }, strategy == NatsStrategy.Lease ? new() { CatchUp = TimeSpan.Zero } : null);
        await scheduler.StartAsync();

        var run = scheduler.RunNowAsync("slow");
        await started.Task.WaitAsync(Patience);
        var during = await scheduler.LastRunAsync("slow");
        release.SetResult();
        await run;
        var after = await scheduler.LastRunAsync("slow");
        await scheduler.StopAsync();

        Assert.Equal(RunStatus.Running, during?.Status);
        Assert.Equal((RunStatus.Success, true, "api-1"), (after?.Status, after?.IsManual, after?.InstanceId));
    }

    [Fact]
    public async Task with_a_host_add_anarchon_runs_declared_jobs_on_nats()
    {
        await NatsServer.RequireAsync();
        var (stream, prefix) = NatsServer.Scope("hosting");
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddAnarchon(options => options.InstanceId = "host-1")
            .UseNats(options =>
            {
                options.ConnectionString = NatsServer.Url;
                options.Strategy = NatsStrategy.Lease;
                options.Stream = stream;
                options.Prefix = prefix;
            })
            .AddJobs<Reports>();
        using var host = builder.Build();

        await host.StartAsync();
        var scheduler = host.Services.GetRequiredService<Scheduler>();
        var run = await scheduler.RunNowAsync("nightly-report");
        var history = await scheduler.HistoryAsync("nightly-report");
        await host.StopAsync();

        Assert.Equal(("nats:lease", RunStatus.Success), (scheduler.Backend.Name, run.Status));
        Assert.Equal(("host-1", true, RunReason.Manual), (history[0].InstanceId, history[0].IsManual, history[0].Reason));
    }

    public sealed class Reports
    {
        [Cron("0 2 * * *", Name = "nightly-report")]
        public Task Build(JobContext ctx) => Task.Delay(10, ctx.CancellationToken);
    }

    private static ClaimInput Claim(long slot, string by) =>
        new() { Job = "job", Slot = slot, Token = Guid.NewGuid().ToString(), InstanceId = by, HistorySize = 100 };

    private static RunClaim Run(long slot, string token, string by, ClaimMode mode) => new()
    {
        Job = "job",
        Slot = slot,
        Token = token,
        InstanceId = by,
        HistorySize = 100,
        Mode = mode,
        Reason = mode == ClaimMode.Takeover ? RunReason.Takeover : RunReason.Scheduled,
        Lease = TimeSpan.FromMilliseconds(100),
        Overlap = OverlapPolicy.Skip,
        MaxAttempts = 3,
    };
}
