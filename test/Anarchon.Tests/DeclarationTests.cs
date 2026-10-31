// Copyright (C) 2026-present Aristotelis — see repository license.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Anarchon.Tests;

public sealed class DeclarationTests
{
    [Fact]
    public async Task register_adds_the_jobs_declared_on_a_class_named_type_method_each_calling_its_object()
    {
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });
        var reports = new Reports();
        scheduler.Register(reports);
        await scheduler.StartAsync();

        await scheduler.RunNowAsync("Reports-Digest");
        await scheduler.RunNowAsync("sync-customers");

        Assert.Equal(["Reports-Digest", "Reports-Weekly", "sync-customers"], scheduler.GetJobs().Select(job => job.Name).Order());
        Assert.Equal(["digest:Reports-Digest", "sync:True"], reports.Calls);
    }

    [Fact]
    public void register_options_prefix_every_name_and_give_settings_an_attribute_leaves_out()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });

        scheduler.Register(new Reports(), new RegisterOptions { Prefix = "acme:" });
        scheduler.Register(new Reports(), new RegisterOptions { Prefix = "globex:" });

        Assert.Equal(6, scheduler.GetJobs().Count);
        Assert.Contains(scheduler.GetJobs(), job => job.Name == "acme:sync-customers");
    }

    [Fact]
    public void an_object_whose_jobs_cannot_all_be_added_adds_none()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });
        scheduler.Every("sync-customers", TimeSpan.FromMinutes(1), _ => Task.CompletedTask);

        var error = Assert.Throws<AnarchonException>(() => scheduler.Register(new Reports()));

        Assert.Equal(AnarchonErrorCode.JobExists, error.Code);
        Assert.Single(scheduler.GetJobs());
    }

    [Fact]
    public void a_class_without_jobs_or_with_a_method_the_scheduler_cannot_call_is_refused()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });

        var none = Assert.Throws<ArgumentException>(() => scheduler.Register(new object()));
        var signature = Assert.Throws<ArgumentException>(() => scheduler.Register(new WrongSignature()));
        var setting = Assert.Throws<ArgumentException>(() => scheduler.Register(new WrongSetting()));

        Assert.Contains("declares no jobs", none.Message, StringComparison.Ordinal);
        Assert.Contains("WrongSignature.Run: a job method takes JobContext and CancellationToken only, not String name", signature.Message, StringComparison.Ordinal);
        Assert.Contains("Invalid duration \"soon\"", setting.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void zero_turns_off_jitter_and_catch_up_in_an_attribute_and_is_refused_where_a_duration_must_be_positive()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new LeaseBackendStub() });

        scheduler.Register(new ZeroSettings());
        var timeout = Assert.Throws<ArgumentException>(() => scheduler.Register(new ZeroTimeout()));

        Assert.Equal(["no-catch-up", "no-jitter"], scheduler.GetJobs().Select(job => job.Name).Order());
        Assert.Contains("Invalid duration \"0s\": it must be at least 1 ms", timeout.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(typeof(DotJitter))]
    [InlineData(typeof(StackedUnitsJitter))]
    [InlineData(typeof(StackedUnitsCatchUp))]
    public void a_zero_written_wrong_in_jitter_or_catch_up_is_refused_like_any_bad_duration(Type jobs)
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new LeaseBackendStub() });

        var error = Assert.Throws<ArgumentException>(() => scheduler.Register(Activator.CreateInstance(jobs)!));

        Assert.Contains("Invalid duration", error.Message, StringComparison.Ordinal);
        Assert.Empty(scheduler.GetJobs());
    }

    [Fact]
    public void attribute_settings_read_back_what_was_set_and_their_defaults_when_not_set()
    {
        var set = new EveryAttribute("1m") { HistorySize = 5, MaxAttempts = 2, YieldAfterFailure = true, Overlap = OverlapPolicy.Allow, Offset = "10s" };
        var unset = new CronAttribute("0 * * * *");
        var at = new AtAttribute("2026-01-01T00:00:00Z") { Name = "once" };

        Assert.Equal((5, 2, true, OverlapPolicy.Allow, "1m", "10s"), (set.HistorySize, set.MaxAttempts, set.YieldAfterFailure, set.Overlap, set.Interval, set.Offset));
        Assert.Equal((0, 0, false, OverlapPolicy.Skip, "0 * * * *"), (unset.HistorySize, unset.MaxAttempts, unset.YieldAfterFailure, unset.Overlap, unset.Expression));
        Assert.Equal(("2026-01-01T00:00:00Z", "once"), (at.When, at.Name));
    }

    [Fact]
    public async Task with_a_host_add_anarchon_starts_the_scheduler_and_each_run_gets_its_own_scope()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<Counter>();
        builder.Services.AddAnarchon(options => options.InstanceId = "host-1")
            .UseBackend(_ => new MemoryBackend())
            .AddJobs<ScopedJobs>();
        using var host = builder.Build();

        await host.StartAsync();
        var scheduler = host.Services.GetRequiredService<Scheduler>();
        await scheduler.RunNowAsync("ScopedJobs-Count");
        await scheduler.RunNowAsync("ScopedJobs-Count");
        await host.StopAsync();

        var counter = host.Services.GetRequiredService<Counter>();
        Assert.Equal("host-1", scheduler.InstanceId);
        Assert.Equal(2, counter.Runs);
        Assert.Equal(2, counter.Disposed);
        Assert.Equal(2, counter.Instances.Distinct().Count());
    }

    [Fact]
    public void a_named_scheduler_is_a_keyed_service_with_its_own_backend_and_jobs()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Counter>();
        services.AddAnarchon().UseBackend(_ => new MemoryBackend());
        services.AddAnarchon("billing").UseBackend(_ => new LeaseBackendStub()).AddJobs<ScopedJobs>();
        using var provider = services.BuildServiceProvider();

        var main = provider.GetRequiredService<Scheduler>();
        var billing = provider.GetRequiredKeyedService<Scheduler>("billing");

        Assert.Equal("memory", main.Backend.Name);
        Assert.Equal("lease-stub", billing.Backend.Name);
        Assert.Empty(main.GetJobs());
        Assert.Single(billing.GetJobs());
        Assert.Equal(2, provider.GetServices<IHostedService>().Count());
    }

    public sealed class Reports
    {
        public List<string> Calls { get; } = [];

        [Cron("0 8 * * 1-5", TimeZone = "Europe/Athens", Timeout = "20m")]
        public Task Digest(JobContext ctx)
        {
            Calls.Add($"digest:{ctx.Name}");
            return Task.CompletedTask;
        }

        [Every("10s", Name = "sync-customers", Jitter = "1s")]
        public void Sync(JobContext ctx, CancellationToken token) => Calls.Add($"sync:{ctx.IsManual && !token.IsCancellationRequested}");

        [Cron("0 9 * * 1")]
        public ValueTask Weekly() => ValueTask.CompletedTask;
    }

    public sealed class WrongSignature
    {
        [Every("1m")]
        public Task Run(string name) => Task.CompletedTask;
    }

    public sealed class ZeroSettings
    {
        [Every("10s", Name = "no-jitter", Jitter = "0s")]
        public Task NoJitter() => Task.CompletedTask;

        [Every("10s", Name = "no-catch-up", CatchUp = "0", Jitter = "0.0ms")]
        public Task NoCatchUp() => Task.CompletedTask;
    }

    public sealed class DotJitter
    {
        [Every("10s", Jitter = ".")]
        public Task Run() => Task.CompletedTask;
    }

    public sealed class StackedUnitsJitter
    {
        [Every("10s", Jitter = "0hms")]
        public Task Run() => Task.CompletedTask;
    }

    public sealed class StackedUnitsCatchUp
    {
        [Every("10s", CatchUp = "0dd")]
        public Task Run() => Task.CompletedTask;
    }

    public sealed class ZeroTimeout
    {
        [Every("10s", Timeout = "0s")]
        public Task Run() => Task.CompletedTask;
    }

    public sealed class WrongSetting
    {
        [Every("1m", Timeout = "soon")]
        public Task Run() => Task.CompletedTask;
    }

    public sealed class Counter
    {
        private int _runs;
        private int _disposed;

        public int Runs => _runs;

        public int Disposed => _disposed;

        public List<object> Instances { get; } = [];

        public void Run(object instance)
        {
            Interlocked.Increment(ref _runs);
            lock (Instances)
            {
                Instances.Add(instance);
            }
        }

        public void Dispose() => Interlocked.Increment(ref _disposed);
    }

    public sealed class ScopedJobs(Counter counter) : IDisposable
    {
        [Every("1h")]
        public Task Count()
        {
            counter.Run(this);
            return Task.CompletedTask;
        }

        public void Dispose() => counter.Dispose();
    }
}
