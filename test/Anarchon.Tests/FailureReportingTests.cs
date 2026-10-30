// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Anarchon.Tests;

/// <summary>How the scheduler tells about runs and problems: the Error event, and the log.</summary>
public sealed class FailureReportingTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task a_claim_that_throws_is_reported_through_the_error_event_and_the_run_is_skipped_here()
    {
        var backend = new MemoryBackend { ClaimFailure = new IOException("the store went away") };
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = backend });
        var reported = new TaskCompletionSource<SchedulerErrorEvent>();
        var ran = false;
        scheduler.Error += (_, e) => reported.TrySetResult(e);
        var due = DateTimeOffset.UtcNow.AddMilliseconds(200);
        scheduler.At("report", due, _ =>
        {
            ran = true;
            return Task.CompletedTask;
        }, new() { Jitter = TimeSpan.Zero });

        await scheduler.StartAsync();
        var error = await reported.Task.WaitAsync(Patience);
        await scheduler.StopAsync();

        Assert.False(ran);
        Assert.Equal(("report", due.ToUnixTimeMilliseconds(), "the store went away"), (error.Job, error.Slot, error.Error.Message));
    }

    [Fact]
    public async Task the_scheduler_logs_runs_at_debug_failed_runs_at_warning_and_problems_nobody_handles_at_error()
    {
        var log = new ListLogger();
        using var factory = LoggerFactory.Create(builder => builder.AddProvider(log).SetMinimumLevel(LogLevel.Debug));
        var backend = new MemoryBackend();
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = backend, LoggerFactory = factory });
        scheduler
            .Every("ok", TimeSpan.FromHours(1), _ => Task.CompletedTask)
            .Every("broken", TimeSpan.FromHours(1), _ => throw new InvalidOperationException("boom"))
            .At("due", DateTimeOffset.UtcNow.AddMilliseconds(200), _ => Task.CompletedTask, new() { Jitter = TimeSpan.Zero });
        await scheduler.StartAsync();

        await scheduler.RunNowAsync("ok");
        await scheduler.RunNowAsync("broken");
        backend.ClaimFailure = new IOException("the store went away"); // and nobody handles Error
        await WaitForAsync(() => log.Entries.Any(e => e.Level == LogLevel.Error));
        await scheduler.StopAsync();

        Assert.All(log.Entries, entry => Assert.Equal("Anarchon.Scheduler", entry.Category));
        Assert.Contains(log.Entries, e => e is { Level: LogLevel.Debug } && e.Message.StartsWith("ok @ ", StringComparison.Ordinal));
        Assert.Contains(log.Entries, e => e is { Level: LogLevel.Warning } && e.Message.StartsWith("broken @ ", StringComparison.Ordinal) && e.Message.Contains("boom", StringComparison.Ordinal));
        Assert.Contains(log.Entries, e => e is { Level: LogLevel.Error } && e.Message.Contains("job \"due\"", StringComparison.Ordinal) && e.Message.Contains("the store went away", StringComparison.Ordinal));
    }

    [Fact]
    public async Task data_of_another_type_than_a_job_asks_for_fails_the_run_with_both_types_named()
    {
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend() });
        scheduler.Every("typed", TimeSpan.FromHours(1), ctx => Task.FromResult(ctx.GetData<Uri>()), new() { Data = "not a uri" });
        await scheduler.StartAsync();

        var run = await scheduler.RunNowAsync("typed");

        Assert.Equal(RunStatus.Failed, run.Status);
        Assert.Equal("InvalidOperationException: Job \"typed\": the run's data is String, not Uri", run.Error);
    }

    [Fact]
    public async Task a_start_that_cannot_connect_throws_closes_the_backend_and_can_be_tried_again()
    {
        var backend = new MemoryBackend { InitFailure = new IOException("no route to the store") };
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = backend });

        var error = await Assert.ThrowsAsync<IOException>(() => scheduler.StartAsync());
        var closesAfterFailure = backend.Closes;
        backend.InitFailure = null;
        await scheduler.StartAsync();
        await scheduler.StopAsync();

        Assert.Equal("no route to the store", error.Message);
        Assert.Equal((1, 2), (closesAfterFailure, backend.Closes));
    }

    [Fact]
    public async Task a_backend_that_fails_to_close_is_logged_and_stop_still_returns()
    {
        var log = new ListLogger();
        using var factory = LoggerFactory.Create(builder => builder.AddProvider(log));
        var backend = new MemoryBackend { CloseFailure = new IOException("connection reset") };
        var scheduler = new Scheduler(new SchedulerOptions { Backend = backend, LoggerFactory = factory });
        await scheduler.StartAsync();

        await scheduler.StopAsync().WaitAsync(Patience);

        Assert.Contains(log.Entries, e => e.Level >= LogLevel.Warning && e.Message.Contains("memory", StringComparison.Ordinal));
    }

    [Fact]
    public async Task a_run_reached_later_than_max_late_is_skipped_as_late()
    {
        var time = new Microsoft.Extensions.Time.Testing.FakeTimeProvider(DateTimeOffset.UtcNow);
        await using var scheduler = new Scheduler(new SchedulerOptions { Backend = new MemoryBackend(), TimeProvider = time });
        var skipped = new TaskCompletionSource<SkipEvent>();
        var ran = false;
        scheduler.Skipped += (_, e) => skipped.TrySetResult(e);
        scheduler.At("report", time.GetUtcNow().AddSeconds(1), _ =>
        {
            ran = true;
            return Task.CompletedTask;
        }, new() { Jitter = TimeSpan.Zero, MaxLate = TimeSpan.FromSeconds(2) });
        await scheduler.StartAsync();

        // The process was frozen (a GC pause, a suspended VM): the timer fires 10 s after the run was due.
        time.Advance(TimeSpan.FromSeconds(11));
        var skip = await skipped.Task.WaitAsync(Patience);
        await scheduler.StopAsync();

        Assert.False(ran);
        Assert.Equal(SkipReason.Late, skip.Reason);
        Assert.True(skip.Late >= TimeSpan.FromSeconds(10), $"late by {skip.Late}");
    }

    private static async Task WaitForAsync(Func<bool> condition)
    {
        var until = DateTimeOffset.UtcNow + Patience;
        while (!condition())
        {
            Assert.True(DateTimeOffset.UtcNow < until, "the condition never held");
            await Task.Delay(20);
        }
    }

    /// <summary>Keeps every log entry, with its category and level.</summary>
    private sealed class ListLogger : ILoggerProvider
    {
        public ConcurrentQueue<(string Category, LogLevel Level, string Message)> Entries { get; } = new();

        public ILogger CreateLogger(string categoryName) => new Logger(this, categoryName);

        public void Dispose()
        {
        }

        private sealed class Logger(ListLogger owner, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
                owner.Entries.Enqueue((category, logLevel, formatter(state, exception)));
        }
    }
}
