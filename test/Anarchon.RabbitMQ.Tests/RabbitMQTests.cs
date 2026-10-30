// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;
using Anarchon.Specification;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Anarchon.RabbitMQ.Tests;

/// <summary>What is specific to RabbitMQ. The shared contract is in Rabbit.cs.</summary>
public sealed class RabbitMQTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    [Fact]
    public void options_are_checked_when_the_backend_is_made()
    {
        Assert.Throws<ArgumentException>(() => new RabbitMQBackend("http://localhost"));
        Assert.Throws<ArgumentException>(() => new RabbitMQBackend("not a uri"));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RabbitMQBackend(new RabbitMQBackendOptions { SlotTtl = TimeSpan.Zero }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RabbitMQBackend(new RabbitMQBackendOptions { SlotTtl = TimeSpan.FromDays(30) }));
        var backend = new RabbitMQBackend(new RabbitMQBackendOptions { SlotTtl = TimeSpan.FromMinutes(10) });
        Assert.Equal(("rabbitmq", TimeSpan.FromMinutes(10), false), (backend.Name, backend.DedupeWindow, ((IBackend)backend).Runs is not null));
    }

    [Fact]
    public void job_names_must_be_simple_and_history_is_kept_by_count_only()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new RabbitMQBackend(new RabbitMQBackendOptions()) });

        var dotted = Assert.Throws<AnarchonException>(() => scheduler.Every("a.b", TimeSpan.FromMinutes(1), _ => Task.CompletedTask));
        var ttl = Assert.Throws<AnarchonException>(() => scheduler.Every("a", TimeSpan.FromMinutes(1), _ => Task.CompletedTask, new() { HistoryTtl = TimeSpan.FromHours(1) }));
        var late = Assert.Throws<AnarchonException>(() => scheduler.Every("b", TimeSpan.FromMinutes(10), _ => Task.CompletedTask, new() { MaxLate = TimeSpan.FromMinutes(6) }));

        Assert.Equal((AnarchonErrorCode.Unsupported, AnarchonErrorCode.Unsupported), (dotted.Code, ttl.Code));
        Assert.Contains("letters, digits", dotted.Message, StringComparison.Ordinal);
        Assert.Contains("HistoryTtl is not supported by rabbitmq", ttl.Message, StringComparison.Ordinal);
        Assert.Equal(AnarchonErrorCode.UnsafeJob, late.Code);
    }

    [Fact]
    public async Task a_gate_queue_holds_one_claim_and_deletes_itself_after_the_slot_ttl()
    {
        await Rabbit.RequireAsync();
        var prefix = Rabbit.Prefix("gate");
        var backend = Rabbit.Backend(prefix, TimeSpan.FromMilliseconds(1_500));
        await using var connection = await Rabbit.ConnectAsync();
        await backend.InitAsync(CancellationToken.None);
        var slot = FutureSlot();
        var mine = Claim(slot, "A");

        var claims = (await backend.ClaimAsync(mine, CancellationToken.None), await backend.ClaimAsync(mine, CancellationToken.None), await backend.ClaimAsync(Claim(slot, "B"), CancellationToken.None));
        var queue = $"{prefix}job.slot.{slot}";
        uint held;
        await using (var channel = await connection.CreateChannelAsync())
        {
            held = (await channel.QueueDeclarePassiveAsync(queue)).MessageCount;
        }

        await Task.Delay(3_000);
        await using var after = await connection.CreateChannelAsync();
        var gone = await Assert.ThrowsAsync<OperationInterruptedException>(() => after.QueueDeclarePassiveAsync(queue));
        await backend.CloseAsync();

        Assert.Equal((true, true, false), claims);
        Assert.Equal(1u, held);
        Assert.Equal((ushort)Constants.NotFound, gone.ShutdownReason?.ReplyCode);
    }

    [Fact]
    public async Task instances_with_other_settings_share_a_job_and_a_refused_declaration_fails_no_other_claim()
    {
        await Rabbit.RequireAsync();
        var prefix = Rabbit.Prefix("args");
        var a = Rabbit.Backend(prefix, TimeSpan.FromMinutes(1));
        var b = Rabbit.Backend(prefix, TimeSpan.FromMinutes(2));
        var slot = FutureSlot();

        var first = await a.ClaimAsync(Claim(slot, "A"), CancellationToken.None);
        var claims = await Task.WhenAll(b.ClaimAsync(Claim(slot, "B"), CancellationToken.None), b.ClaimAsync(Claim(slot, "B") with { Job = "other" }, CancellationToken.None));
        await a.PrepareJobAsync("job", 100, CancellationToken.None);
        await b.PrepareJobAsync("job", 5_000, CancellationToken.None);
        await Task.WhenAll(a.CloseAsync(), b.CloseAsync());

        Assert.True(first);
        Assert.Equal([false, true], claims);
    }

    [Fact]
    public async Task many_claims_at_once_on_one_connection_each_get_their_answer()
    {
        await Rabbit.RequireAsync();
        var prefix = Rabbit.Prefix("many");
        var a = Rabbit.Backend(prefix);
        var b = Rabbit.Backend(prefix);
        var slots = Enumerable.Range(0, 50).Select(i => FutureSlot() + (i * 1_000)).ToList();

        var won = await Task.WhenAll(slots.Select(slot => a.ClaimAsync(Claim(slot, "A"), CancellationToken.None)));
        var lost = await Task.WhenAll(slots.Select(slot => b.ClaimAsync(Claim(slot, "B"), CancellationToken.None)));
        await Task.WhenAll(a.CloseAsync(), b.CloseAsync());

        Assert.Equal((50, 0), (won.Count(w => w), lost.Count(l => l)));
    }

    [Fact]
    public async Task the_backend_connects_again_after_the_broker_closed_its_connection()
    {
        await Rabbit.RequireAsync();
        var prefix = Rabbit.Prefix("reconnect");
        var backend = Rabbit.Backend(prefix);
        var slot = FutureSlot();
        Assert.True(await backend.ClaimAsync(Claim(slot, "A"), CancellationToken.None));

        await Rabbit.CloseConnectionOfAsync(prefix);
        await Task.Delay(300); // the client learns the connection closed
        var again = await backend.ClaimAsync(Claim(slot + 1_000, "A"), CancellationToken.None);
        await backend.CloseAsync();

        Assert.True(again);
    }

    [Fact]
    public async Task a_connection_of_the_app_is_shared_and_stays_open_after_stop()
    {
        await Rabbit.RequireAsync();
        await using var connection = await Rabbit.ConnectAsync();
        var prefix = Rabbit.Prefix("own");
        Assert.Throws<ArgumentException>(() => new RabbitMQBackend(new RabbitMQBackendOptions { ConnectionString = Rabbit.Url, Connection = connection }));

        await BackendContract.RaceAsync(() => new RabbitMQBackend(new RabbitMQBackendOptions { Connection = connection, Prefix = prefix }));

        Assert.True(connection.IsOpen);
    }

    [Fact]
    public async Task a_manual_run_shows_as_running_then_as_success_in_history()
    {
        await Rabbit.RequireAsync();
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = Rabbit.Backend(Rabbit.Prefix("now")), InstanceId = "api-1" });
        var started = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        scheduler.Every("slow", TimeSpan.FromHours(1), async _ =>
        {
            started.SetResult();
            await release.Task;
        });
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
    public async Task with_a_host_add_anarchon_runs_declared_jobs_on_rabbitmq()
    {
        await Rabbit.RequireAsync();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddAnarchon(options => options.InstanceId = "host-1")
            .UseRabbitMQ(options =>
            {
                options.ConnectionString = Rabbit.Url;
                options.Prefix = Rabbit.Prefix("hosting");
            })
            .AddJobs<Reports>();
        using var host = builder.Build();

        await host.StartAsync();
        var scheduler = host.Services.GetRequiredService<Scheduler>();
        var run = await scheduler.RunNowAsync("nightly-report");
        var history = await scheduler.HistoryAsync("nightly-report");
        await host.StopAsync();

        Assert.Equal(("rabbitmq", RunStatus.Success), (scheduler.Backend.Name, run.Status));
        Assert.Equal(("host-1", true), (history[0].InstanceId, history[0].IsManual));
    }

    public sealed class Reports
    {
        [Cron("0 2 * * *", Name = "nightly-report")]
        public Task Build(JobContext ctx) => Task.Delay(10, ctx.CancellationToken);
    }

    private static long FutureSlot() => ((DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1_000) + 60) * 1_000;

    private static ClaimInput Claim(long slot, string by) =>
        new() { Job = "job", Slot = slot, Token = Guid.NewGuid().ToString(), InstanceId = by, HistorySize = 100 };
}
