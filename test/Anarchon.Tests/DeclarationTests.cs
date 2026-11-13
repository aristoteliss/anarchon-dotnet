// Copyright (C) 2026-present Aristotelis — see repository license.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;

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

    [Theory]
    [InlineData("Jitter")]
    [InlineData("MaxLate")]
    [InlineData("Timeout")]
    [InlineData("HistoryTtl")]
    [InlineData("CatchUp")]
    [InlineData("HistorySize")]
    [InlineData("MaxAttempts")]
    public void an_attribute_setting_wins_over_the_register_default_and_the_default_fills_a_setting_the_attribute_leaves_out(string setting)
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new LeaseBackendStub() });
        var defaults = new RegisterOptions { Defaults = InvalidDefault(setting) };

        scheduler.Register(new AllSettings(), defaults);
        var error = Assert.Throws<ArgumentOutOfRangeException>(() => scheduler.Register(new NoSettings(), defaults));

        Assert.Equal($"Job \"NoSettings-Run\": {setting}", error.ParamName);
        Assert.Equal("AllSettings-Run", Assert.Single(scheduler.GetJobs()).Name);
    }

    [Fact]
    public void a_cron_job_takes_the_time_zone_of_its_attribute_then_of_the_register_options_then_of_the_scheduler()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend(), TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Asia/Tokyo") });

        scheduler.Register(new Zones(), new RegisterOptions { Prefix = "a:", TimeZone = TimeZoneInfo.FindSystemTimeZoneById("America/New_York") });
        scheduler.Register(new Zones(), new RegisterOptions { Prefix = "b:" });

        var schedules = scheduler.GetJobs().ToDictionary(job => job.Name, job => job.Schedule);
        Assert.Equal("cron \"0 8 * * *\" (Europe/Athens)", schedules["a:own"]);
        Assert.Equal("cron \"0 9 * * *\" (America/New_York)", schedules["a:inherited"]);
        Assert.Equal("cron \"0 9 * * *\" (Asia/Tokyo)", schedules["b:inherited"]);
    }

    [Fact]
    public void an_every_attribute_takes_an_interval_of_one_second_or_more()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });

        scheduler.Register(new OneSecond());
        var error = Assert.Throws<ArgumentException>(() => scheduler.Register(new UnderOneSecond()));

        Assert.Contains("the interval must be at least 1s (got 999 ms)", error.Message, StringComparison.Ordinal);
        Assert.Equal("every 1s", Assert.Single(scheduler.GetJobs()).Schedule);
    }

    [Theory]
    [InlineData(typeof(BadCron))]
    [InlineData(typeof(BadZone))]
    [InlineData(typeof(BadWhen))]
    public void a_schedule_an_attribute_cannot_give_is_refused_with_the_job_and_its_method(Type jobs)
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });

        var error = Assert.Throws<ArgumentException>(() => scheduler.Register(Activator.CreateInstance(jobs)!));

        Assert.StartsWith($"Job \"{jobs.Name}-Run\" ({jobs.Name}.Run): ", error.Message, StringComparison.Ordinal);
        Assert.Empty(scheduler.GetJobs());
    }

    [Fact]
    public void jobs_of_one_object_with_the_same_name_are_refused_and_none_is_added()
    {
        var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });

        var error = Assert.Throws<AnarchonException>(() => scheduler.Register(new SameName()));

        Assert.Equal(AnarchonErrorCode.JobExists, error.Code);
        Assert.Empty(scheduler.GetJobs());
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

    [Fact]
    public async Task the_hosted_service_of_a_named_scheduler_starts_that_scheduler()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Counter>();
        services.AddAnarchon().UseBackend(_ => new MemoryBackend());
        services.AddAnarchon("billing").UseBackend(_ => new MemoryBackend()).AddJobs<ScopedJobs>();
        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().ToList();

        foreach (var service in hosted) await service.StartAsync(CancellationToken.None);
        var run = await provider.GetRequiredKeyedService<Scheduler>("billing").RunNowAsync("ScopedJobs-Count");
        foreach (var service in hosted) await service.StopAsync(CancellationToken.None);

        Assert.Equal(RunStatus.Success, run.Status);
    }

    [Fact]
    public void add_anarchon_twice_with_one_name_adds_one_scheduler_with_both_configurations()
    {
        var services = new ServiceCollection();
        services.AddAnarchon(options => options.InstanceId = "first").UseBackend(_ => new MemoryBackend());
        services.AddAnarchon(options => options.InstanceId = "second");
        using var provider = services.BuildServiceProvider();

        Assert.Equal("second", provider.GetRequiredService<Scheduler>().InstanceId);
        Assert.Single(provider.GetServices<IHostedService>());
    }

    [Fact]
    public void a_time_provider_in_the_options_wins_over_the_one_in_the_services_and_that_one_is_used_otherwise()
    {
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 30, 0, TimeSpan.Zero)));
        services.AddAnarchon().UseBackend(_ => new MemoryBackend());
        var own = new FakeTimeProvider(new DateTimeOffset(2040, 1, 1, 0, 30, 0, TimeSpan.Zero));
        services.AddAnarchon("own", options => options.TimeProvider = own).UseBackend(_ => new MemoryBackend());
        using var provider = services.BuildServiceProvider();
        var registered = provider.GetRequiredService<Scheduler>().Every("tick", TimeSpan.FromHours(1), _ => Task.CompletedTask);
        var configured = provider.GetRequiredKeyedService<Scheduler>("own").Every("tick", TimeSpan.FromHours(1), _ => Task.CompletedTask);

        Assert.Equal(new DateTimeOffset(2030, 1, 1, 1, 0, 0, TimeSpan.Zero), registered.GetNextRun("tick"));
        Assert.Equal(new DateTimeOffset(2040, 1, 1, 1, 0, 0, TimeSpan.Zero), configured.GetNextRun("tick"));
    }

    private static RecurringJobOptions InvalidDefault(string setting) => setting switch
    {
        "Jitter" => new() { Jitter = TimeSpan.FromSeconds(-1) },
        "MaxLate" => new() { MaxLate = TimeSpan.Zero },
        "Timeout" => new() { Timeout = TimeSpan.Zero },
        "HistoryTtl" => new() { HistoryTtl = TimeSpan.Zero },
        "CatchUp" => new() { CatchUp = TimeSpan.FromSeconds(-1) },
        "HistorySize" => new() { HistorySize = 0 },
        "MaxAttempts" => new() { MaxAttempts = 0 },
        _ => throw new ArgumentOutOfRangeException(nameof(setting)),
    };

    public sealed class AllSettings
    {
        [Every("1m", Jitter = "1s", MaxLate = "5s", Timeout = "1m", HistoryTtl = "1h", CatchUp = "1m", HistorySize = 5, MaxAttempts = 2)]
        public Task Run() => Task.CompletedTask;
    }

    public sealed class NoSettings
    {
        [Every("1m")]
        public Task Run() => Task.CompletedTask;
    }

    public sealed class Zones
    {
        [Cron("0 8 * * *", Name = "own", TimeZone = "Europe/Athens")]
        public Task Own() => Task.CompletedTask;

        [Cron("0 9 * * *", Name = "inherited")]
        public Task Inherited() => Task.CompletedTask;
    }

    public sealed class OneSecond
    {
        [Every("1s")]
        public Task Run() => Task.CompletedTask;
    }

    public sealed class UnderOneSecond
    {
        [Every("999ms")]
        public Task Run() => Task.CompletedTask;
    }

    public sealed class BadCron
    {
        [Cron("not a cron")]
        public Task Run() => Task.CompletedTask;
    }

    public sealed class BadZone
    {
        [Cron("0 8 * * *", TimeZone = "Nowhere/Nothing")]
        public Task Run() => Task.CompletedTask;
    }

    public sealed class BadWhen
    {
        [At("tomorrow morning")]
        public Task Run() => Task.CompletedTask;
    }

    public sealed class SameName
    {
        [Every("1m", Name = "sync")]
        public Task First() => Task.CompletedTask;

        [Every("5m", Name = "sync")]
        public Task Second() => Task.CompletedTask;
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
