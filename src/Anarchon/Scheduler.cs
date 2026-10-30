// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Collections.Concurrent;
using Anarchon.Backends;
using Anarchon.Scheduling;
using Microsoft.Extensions.Logging;

namespace Anarchon;

/// <summary>
/// Runs cron, interval and one-off jobs once per scheduled time across any number of instances. No
/// leader and no locks: each run is claimed with one atomic operation on the backend that only one
/// instance can win, and a random delay before claiming spreads the work.
/// </summary>
/// <remarks>
/// <para>
/// With a lease backend (such as Redis with the Lease strategy) a running job also renews a lease.
/// That gives: no overlapping runs, backup runners when the runner dies or freezes, retries, and
/// catch-up of a run that was due while every instance was down.
/// </para>
/// <para>
/// Register the same jobs, with the same names and schedules, on every instance. In an app with a
/// host, <c>services.AddAnarchon()</c> creates, starts and stops the scheduler for you.
/// </para>
/// </remarks>
/// <example>The smallest setup: Redis on localhost, one job. Start the same code on every instance.
/// <code>
/// await using var scheduler = new Scheduler(new SchedulerOptions { Backend = new RedisBackend("localhost:6379") });
/// scheduler.Every("sync", TimeSpan.FromSeconds(10), ctx => SyncCustomersAsync(ctx.CancellationToken));
/// await scheduler.StartAsync();
/// </code>
/// </example>
/// <example>Several kinds of job, and a clean shutdown
/// <code>
/// scheduler
///     .Every("sync", TimeSpan.FromSeconds(10), ctx => SyncAsync(ctx.CancellationToken))
///     .Cron("digest", "0 8 * * 1-5", ctx => SendDigestAsync(ctx.CancellationToken))
///     .At("migration", DateTimeOffset.Parse("2026-12-01T03:00:00Z"), ctx => MigrateAsync(ctx.CancellationToken));
/// await scheduler.StartAsync();
/// // ...
/// await scheduler.StopAsync(); // running jobs are handed to the other instances
/// </code>
/// </example>
public sealed partial class Scheduler : IAsyncDisposable, IDisposable
{
    private const int MaxHistoryLimit = 10_000;

    private readonly SchedulerDefaults _defaults;
    private readonly ILogger _logger;
    private readonly TimeSpan _stopTimeout;
    private readonly Lock _gate = new();
    private readonly Dictionary<string, Job> _registry = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Task, byte> _inflight = new();
    private readonly JobTimers _timers;
    private readonly Executor _executor;
    private readonly ClaimOnlyRunner _claimOnly;

    /// <summary>Present when the backend has leases.</summary>
    private readonly LeaseRunner? _leases;

    private volatile State _state = State.Idle;
    private CancellationTokenSource _lifecycle = new();
    private Task? _ready;
    private Task? _stopping;
    private object? _data;

    /// <summary>Stop has run and start hasn't since: a read opens the connection only for itself.</summary>
    private bool _stopped;

    /// <summary>Reads (history) in progress.</summary>
    private int _reads;

    /// <summary>Creates a scheduler. Nothing connects and nothing runs until <see cref="StartAsync"/>.</summary>
    /// <param name="options">The scheduler's options; <see cref="SchedulerOptions.Backend"/> is required.</param>
    /// <exception cref="ArgumentException">No backend is given, or a value is out of range.</exception>
    /// <exception cref="AnarchonException">
    /// <see cref="AnarchonErrorCode.Unsupported"/>: a lease option is set on a backend without leases.
    /// </exception>
    /// <example>Faster failover: a dead runner is replaced after about 10 seconds instead of 30
    /// <code>
    /// new Scheduler(new SchedulerOptions { Backend = backend, Lease = TimeSpan.FromSeconds(10), Heartbeat = TimeSpan.FromSeconds(2) });
    /// </code>
    /// </example>
    public Scheduler(SchedulerOptions options)
    {
        var settings = Settings.Read(options);
        Backend = settings.Backend;
        InstanceId = settings.InstanceId;
        LeaseMs = settings.LeaseMs;
        HeartbeatMs = settings.HeartbeatMs;
        Time = settings.Time;
        _data = settings.Data;
        _defaults = settings.Defaults;
        _logger = settings.Logger;
        _stopTimeout = settings.StopTimeout;

        _executor = new Executor(this);
        _claimOnly = new ClaimOnlyRunner(this, _executor);
        _leases = Backend.Runs is { } runs ? new LeaseRunner(this, _executor, runs) : null;
        _timers = new JobTimers(this, (job, slot) => Track(FireAsync(job, slot)));
    }

    private enum State
    {
        Idle,
        Starting,
        Running,
        Stopping,
    }

    /// <summary>This instance's name in history and events. Default <c>machine:pid</c>.</summary>
    public string InstanceId { get; }

    /// <summary>
    /// Where runs are claimed and recorded, as given in the options. Its <see cref="IBackend.Name"/>
    /// tells which one, such as <c>redis:lease</c>.
    /// </summary>
    public IBackend Backend { get; }

    private long LeaseMs { get; }

    private long HeartbeatMs { get; }

    private TimeProvider Time { get; }

    /// <summary>
    /// Connects to the backend and starts scheduling. Jobs can be added before or after. Calling it
    /// again while running does nothing.
    /// </summary>
    /// <remarks>
    /// With leases, starting also picks up what was left behind: an unfinished run whose runner died
    /// is taken over, and the latest run is caught up if it was due (at most the job's
    /// <c>CatchUp</c> ago) while every instance was down.
    /// </remarks>
    /// <param name="cancellationToken">Stops waiting for the backend; the scheduler stays stopped.</param>
    /// <returns>A task that completes once the scheduler runs.</returns>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.State"/>: the scheduler is starting or stopping.</exception>
    /// <example>
    /// <code>
    /// try
    /// {
    ///     await scheduler.StartAsync();
    /// }
    /// catch (Exception e)
    /// {
    ///     logger.LogCritical(e, "the scheduler could not start");
    ///     throw;
    /// }
    /// </code>
    /// </example>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (_state == State.Running) return;
            if (_state != State.Idle) throw new AnarchonException(AnarchonErrorCode.State, $"Cannot start while {_state.ToString().ToLowerInvariant()}");
            _state = State.Starting;
            _stopped = false;
        }

        try
        {
            await EnsureReadyAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
            await Task.WhenAll(Snapshot().Select(PrepareAsync)).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _state = State.Idle;
            await CloseBackendAsync().ConfigureAwait(false);
            throw;
        }

        if (!TryEnterRunning(out var stopping))
        {
            // Stop was called meanwhile, and closed the backend while it was still being connected or
            // prepared: what reconnected after that must be closed too.
            if (stopping is not null) await stopping.ConfigureAwait(false);
            if (_state == State.Idle) await CloseBackendAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Stops scheduling, cancels the running jobs' tokens, waits for them up to
    /// <see cref="SchedulerOptions.StopTimeout"/> or until <paramref name="cancellationToken"/> is
    /// cancelled, then closes the connections the backend opened. Clients you passed in stay open.
    /// </summary>
    /// <remarks>
    /// On a lease backend, a job that throws once its token is cancelled is handed to another instance
    /// right away. A job still running when the wait ends stops renewing its lease, so a backup takes
    /// it over when the lease runs out. A run won after this call doesn't start.
    /// </remarks>
    /// <param name="cancellationToken">Ends the wait for running jobs early.</param>
    /// <returns>A task that completes once the scheduler is stopped.</returns>
    /// <example>
    /// <code>
    /// using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    /// await scheduler.StopAsync(shutdown.Token);
    /// </code>
    /// </example>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            // Started on the thread pool, so its first steps never run inside this lock; it clears
            // _stopping under the same lock, after this assignment.
            _stopping ??= Task.Run(() => DoStopAsync(cancellationToken), CancellationToken.None);
            return _stopping;
        }
    }

    /// <summary>Stops the scheduler: the same as <see cref="StopAsync"/>.</summary>
    /// <returns>A task that completes once the scheduler is stopped.</returns>
    public async ValueTask DisposeAsync() => await StopAsync().ConfigureAwait(false);

    /// <summary>
    /// Stops the scheduler and blocks until it has stopped, at most <see cref="SchedulerOptions.StopTimeout"/>
    /// plus the time to close the backend. For a container that is disposed synchronously; prefer
    /// <see cref="DisposeAsync"/>.
    /// </summary>
    public void Dispose() => StopAsync().GetAwaiter().GetResult();

    private bool TryEnterRunning(out Task? stopping)
    {
        Job[] jobs;
        lock (_gate)
        {
            stopping = _stopping;
            if (_state != State.Starting) return false;
            _lifecycle = new CancellationTokenSource();
            _state = State.Running;
            jobs = [.. _registry.Values];
        }

        foreach (var job in jobs)
        {
            Schedule(job);
        }

        return true;
    }

    private async Task DoStopAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (EnterStopping(out var jobs))
            {
                foreach (var job in jobs)
                {
                    JobTimers.Disarm(job);
                }

                _leases?.StopWatching();
                await _lifecycle.CancelAsync().ConfigureAwait(false);
                await WaitForRunsAsync(cancellationToken).ConfigureAwait(false);
                _leases?.StopRenewing();
            }

            lock (_gate)
            {
                _state = State.Idle;
                _stopped = true;
            }

            await CloseBackendAsync().ConfigureAwait(false);
        }
        finally
        {
            lock (_gate)
            {
                _stopping = null;
            }
        }
    }

    private bool EnterStopping(out Job[] jobs)
    {
        lock (_gate)
        {
            jobs = [.. _registry.Values];
            if (_state is not (State.Running or State.Starting)) return false;
            _state = State.Stopping;
            return true;
        }
    }

    /// <summary>Waits for the runs and claims in flight, up to the stop timeout or until the caller gives up.</summary>
    private async Task WaitForRunsAsync(CancellationToken cancellationToken)
    {
        var pending = _inflight.Keys.ToArray();
        if (pending.Length == 0) return;

        var settled = Task.WhenAll(pending.Select(task => task.ContinueWith(static _ => { }, TaskScheduler.Default)));
        try
        {
            await settled.WaitAsync(_stopTimeout, Time, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception error) when (error is TimeoutException or OperationCanceledException)
        {
            // The runs still going are left to their lease.
        }
    }

    /// <summary>Connects the backend once; later calls wait for the same connection, and a failed one is tried again.</summary>
    private Task EnsureReadyAsync()
    {
        lock (_gate)
        {
            if (_ready is null || _ready.IsFaulted || _ready.IsCanceled) _ready = Task.Run(() => Backend.InitAsync(CancellationToken.None));
            return _ready;
        }
    }

    private Task PrepareAsync(Job job)
    {
        lock (job.Gate)
        {
            if (job.Prepared is null || job.Prepared.IsFaulted || job.Prepared.IsCanceled)
            {
                job.Prepared = Task.Run(() => Backend.PrepareJobAsync(job.Name, job.HistorySize, CancellationToken.None));
            }

            return job.Prepared;
        }
    }

    private async Task CloseBackendAsync()
    {
        lock (_gate)
        {
            _ready = null;
            foreach (var job in _registry.Values)
            {
                job.Prepared = null;
            }
        }

        try
        {
            await Backend.CloseAsync().ConfigureAwait(false);
        }
        catch (Exception error)
        {
            Log.CloseFailed(_logger, Backend.Name, error);
        }
    }

    private Task Track(Task work)
    {
        _inflight.TryAdd(work, 0);
        work.ContinueWith(task => _inflight.TryRemove(task, out _), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return work;
    }

    private Task<T> Track<T>(Task<T> work)
    {
        Track((Task)work);
        return work;
    }

    /// <summary>Sets the job's timer and, with leases, picks up what other instances left behind.</summary>
    private void Schedule(Job job)
    {
        _timers.Arm(job);
        if (_leases is not null) Track(_leases.RecoverAsync(job));
    }

    /// <summary>A scheduled run is due. Never throws: problems are reported as events.</summary>
    private Task FireAsync(Job job, long slot)
    {
        var stop = _lifecycle.Token;
        _timers.Arm(job); // the next run first, so a long job doesn't delay it

        var lateMs = Now() - slot;
        if (lateMs > job.MaxLateMs)
        {
            OnSkipped(new SkipEvent { Job = job.Name, Slot = slot, InstanceId = InstanceId, Reason = SkipReason.Late, Late = TimeSpan.FromMilliseconds(lateMs) });
            return Task.CompletedTask;
        }

        return _leases is not null ? _leases.RunAsync(job, slot, RunReason.Scheduled, stop) : _claimOnly.RunAsync(job, slot, stop);
    }

    private Job[] Snapshot()
    {
        lock (_gate)
        {
            return [.. _registry.Values];
        }
    }

    private long Now() => Time.GetUtcNow().ToUnixTimeMilliseconds();
}
