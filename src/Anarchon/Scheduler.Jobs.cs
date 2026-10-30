// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Declarations;
using Anarchon.Scheduling;
using Cronos;

namespace Anarchon;

public sealed partial class Scheduler
{
    /// <summary>
    /// Adds a cron job. The expression has 5 fields (minute hour day month weekday), or 6 with seconds
    /// first. Times are in the job's <see cref="CronJobOptions.TimeZone"/>, else the scheduler's, else
    /// the server's zone.
    /// </summary>
    /// <param name="name">Unique per scheduler. It is also the job's name in the backend, so keep it stable.</param>
    /// <param name="expression">The cron expression, such as <c>0 8 * * 1-5</c> or <c>*/15 * * * * *</c>.</param>
    /// <param name="handler">What the job does.</param>
    /// <param name="options">The job's settings.</param>
    /// <returns>This scheduler, to add more jobs.</returns>
    /// <exception cref="ArgumentException">The expression is invalid, or never runs again.</exception>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.JobExists"/>: the name is already registered.</exception>
    /// <example>
    /// <code>
    /// scheduler.Cron("digest", "0 8 * * 1-5", SendDigestAsync);                                  // 08:00, Monday to Friday
    /// scheduler.Cron("poll", "*/15 * * * * *", PollAsync);                                        // every 15 seconds
    /// scheduler.Cron("report", "0 0 1 * *", BuildReportAsync, new() { Timeout = TimeSpan.FromMinutes(30) }); // monthly
    /// </code>
    /// </example>
    public Scheduler Cron(string name, string expression, Func<JobContext, Task> handler, CronJobOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(expression);
        options ??= new CronJobOptions();
        RefuseTaken(name);

        Scheduling.Schedule schedule;
        try
        {
            schedule = CronSchedule.Create(expression, options.TimeZone ?? _defaults.TimeZone, Now());
        }
        catch (CronFormatException error)
        {
            throw new ArgumentException($"Job \"{name}\": invalid cron \"{expression}\": {error.Message}", nameof(expression), error);
        }
        catch (ArgumentException error)
        {
            throw new ArgumentException($"Job \"{name}\": {error.Message}", nameof(expression), error);
        }

        return Add(name, schedule, handler, options);
    }

    /// <summary>
    /// Adds an interval job. Runs fall on a fixed UTC grid (multiples of the interval since 1970), not
    /// "interval after start": every instance computes the same run times, and a restart doesn't shift
    /// them. For times on the wall clock of a time zone, use <see cref="Cron"/>.
    /// </summary>
    /// <param name="name">Unique per scheduler, and stable: it names the job in the backend.</param>
    /// <param name="interval">The time between runs; at least 1 second.</param>
    /// <param name="handler">What the job does.</param>
    /// <param name="options">The job's settings.</param>
    /// <returns>This scheduler, to add more jobs.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The interval is under 1 second.</exception>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.JobExists"/>: the name is already registered.</exception>
    /// <example>
    /// <code>
    /// scheduler.Every("sync", TimeSpan.FromSeconds(10), SyncAsync);
    /// scheduler.Every("backup", Duration.Parse("1h30m"), BackupAsync);
    /// scheduler.Every("quarter-past", TimeSpan.FromHours(1), RunAsync, new() { Offset = TimeSpan.FromMinutes(15) }); // 00:15, 01:15...
    /// </code>
    /// </example>
    public Scheduler Every(string name, TimeSpan interval, Func<JobContext, Task> handler, EveryJobOptions? options = null)
    {
        options ??= new EveryJobOptions();
        var intervalMs = (long)interval.TotalMilliseconds;
        if (intervalMs < 1_000)
        {
            throw new ArgumentOutOfRangeException(nameof(interval), interval, $"Job \"{name}\": the interval must be at least 1s (got {intervalMs} ms)");
        }

        RefuseTaken(name);
        var offsetMs = (long)(options.Offset ?? TimeSpan.Zero).TotalMilliseconds;
        return Add(name, new EverySchedule(intervalMs, offsetMs), handler, options);
    }

    /// <summary>Adds a one-off job: it runs once, at <paramref name="when"/>, on one instance.</summary>
    /// <param name="name">Unique per scheduler, and stable: it names the job in the backend.</param>
    /// <param name="when">When it runs.</param>
    /// <param name="handler">What the job does.</param>
    /// <param name="options">The job's settings. A one-off job has nothing to overlap, so no overlap setting.</param>
    /// <returns>This scheduler, to add more jobs.</returns>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.JobExists"/>: the name is already registered.</exception>
    /// <example>
    /// <code>
    /// scheduler.At("launch", DateTimeOffset.Parse("2026-12-01T09:00:00Z"), AnnounceAsync);
    /// scheduler.At("reminder", DateTimeOffset.UtcNow.AddHours(1), RemindAsync);
    /// </code>
    /// </example>
    public Scheduler At(string name, DateTimeOffset when, Func<JobContext, Task> handler, JobOptions? options = null)
    {
        RefuseTaken(name);
        return Add(name, new OnceSchedule(when.ToUnixTimeMilliseconds()), handler, options ?? new JobOptions());
    }

    /// <summary>
    /// Adds the jobs declared with <see cref="CronAttribute"/>, <see cref="EveryAttribute"/> and
    /// <see cref="AtAttribute"/> on an object's public methods. Each job calls its method on that
    /// object. A job is named <c>Type-Method</c> unless its attribute gives a name. Nothing is added
    /// when one of the jobs is invalid.
    /// </summary>
    /// <remarks>
    /// A job method returns <see cref="Task"/>, <see cref="ValueTask"/> or nothing, and takes any of
    /// <see cref="JobContext"/> and <see cref="CancellationToken"/>. With a host, prefer
    /// <c>AddJobs&lt;T&gt;()</c> on the builder of <c>AddAnarchon()</c>: it creates the object in a
    /// new service scope for every run.
    /// </remarks>
    /// <param name="target">The object whose methods run.</param>
    /// <param name="options">A prefix for the job names, and settings for every job of the object.</param>
    /// <returns>This scheduler.</returns>
    /// <exception cref="ArgumentException">The object has no declared jobs, or a declaration is invalid.</exception>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.JobExists"/>: a name is already registered.</exception>
    /// <example>One object per tenant, each with its own jobs and time zone
    /// <code>
    /// foreach (var tenant in tenants)
    /// {
    ///     scheduler.Register(new Reports(tenant.Mailer), new RegisterOptions { Prefix = $"{tenant.Id}:", TimeZone = tenant.TimeZone });
    /// }
    /// </code>
    /// </example>
    public Scheduler Register(object target, RegisterOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(target);
        var declared = DeclaredJob.Read(target.GetType());
        return AddDeclared(declared, (job, ctx) => job.InvokeAsync(target, ctx), options);
    }

    /// <summary>
    /// Stops scheduling a job on this instance. Other instances keep running it, its history is kept,
    /// and a run already in progress here finishes.
    /// </summary>
    /// <param name="name">The job's name.</param>
    /// <returns>false when no job has that name.</returns>
    /// <example>Replacing a job's schedule at run time
    /// <code>
    /// scheduler.Remove("sync");
    /// scheduler.Every("sync", TimeSpan.FromSeconds(30), SyncAsync);
    /// </code>
    /// </example>
    public bool Remove(string name)
    {
        Job? job;
        lock (_gate)
        {
            if (!_registry.Remove(name, out job)) return false;
        }

        job.Removed = true;
        JobTimers.Disarm(job);
        _leases?.StopWatching(name);
        return true;
    }

    /// <summary>
    /// Runs a job now, on this instance, whatever its schedule. The run is not deduplicated (calling it
    /// on two instances runs it twice), may overlap a scheduled run, and is recorded in history as
    /// manual. Completes with the run's record when the job has finished.
    /// </summary>
    /// <param name="name">The job's name.</param>
    /// <param name="data">Data for this run only, on top of the job's.</param>
    /// <returns>The run's record. If stop is called before the handler starts, its status is Interrupted and the job doesn't run.</returns>
    /// <exception cref="AnarchonException">
    /// <see cref="AnarchonErrorCode.UnknownJob"/>: the job is not registered here.
    /// <see cref="AnarchonErrorCode.State"/>: the scheduler is not started.
    /// </exception>
    /// <example>Behind an admin endpoint
    /// <code>
    /// app.MapPost("/jobs/{name}/run", async (string name, Scheduler scheduler) =&gt;
    /// {
    ///     var run = await scheduler.RunNowAsync(name);
    ///     return new { run.Status, run.Duration, run.Error };
    /// });
    /// </code>
    /// </example>
    public Task<RunRecord> RunNowAsync(string name, object? data = null)
    {
        var job = GetJob(name);
        if (_state != State.Running) throw new AnarchonException(AnarchonErrorCode.State, "Call StartAsync() before RunNowAsync()");

        var stop = _lifecycle.Token;
        return Track(RunManualAsync(job, data, stop));
    }

    /// <summary>
    /// Replaces the scheduler's data: what every handler gets as <see cref="JobContext.Data"/>, under
    /// each job's own. Runs that start after the call get the new value.
    /// </summary>
    /// <param name="data">A value, or a <see cref="JobDataProvider"/>; null removes it.</param>
    /// <returns>This scheduler.</returns>
    /// <example>
    /// <code>
    /// features.Changed += (_, current) =&gt; scheduler.SetData(new Dictionary&lt;string, object?&gt; { ["features"] = current });
    /// </code>
    /// </example>
    public Scheduler SetData(object? data)
    {
        Volatile.Write(ref _data, data);
        return this;
    }

    /// <summary>
    /// The latest runs of a job across all instances, newest first: who ran it, when, and how it
    /// ended. One record per attempt, so a run that was taken over shows twice. Works for any job
    /// name, also before start and after stop.
    /// </summary>
    /// <param name="name">The job's name.</param>
    /// <param name="limit">How many records at most, 1 to 10000.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The records, newest first.</returns>
    /// <example>
    /// <code>
    /// foreach (var run in await scheduler.HistoryAsync("sync", 5))
    /// {
    ///     Console.WriteLine($"{run.ScheduledAt} {run.InstanceId} {run.Attempt} {run.Status} {run.Duration}");
    /// }
    /// </code>
    /// </example>
    public async Task<IReadOnlyList<RunRecord>> HistoryAsync(string name, int limit = 20, CancellationToken cancellationToken = default)
    {
        Settings.InRange(nameof(limit), limit, 1, MaxHistoryLimit);
        Interlocked.Increment(ref _reads);

        try
        {
            await EnsureReadyAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
            return await Backend.HistoryAsync(name, limit, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // After stop, nothing else closes what this read opened.
            if (Interlocked.Decrement(ref _reads) == 0 && IsStoppedAndIdle()) await CloseBackendAsync().ConfigureAwait(false);
        }
    }

    /// <summary>The most recent run of a job on any instance, or null when it never ran.</summary>
    /// <param name="name">The job's name.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The latest record, or null.</returns>
    /// <example>A health check: has "sync" succeeded in the last minute?
    /// <code>
    /// var last = await scheduler.LastRunAsync("sync");
    /// var healthy = last is { Status: RunStatus.Success } &amp;&amp; DateTimeOffset.UtcNow - last.StartedAt &lt; TimeSpan.FromMinutes(1);
    /// </code>
    /// </example>
    public async Task<RunRecord?> LastRunAsync(string name, CancellationToken cancellationToken = default)
    {
        var runs = await HistoryAsync(name, 1, cancellationToken).ConfigureAwait(false);
        return runs.Count > 0 ? runs[0] : null;
    }

    /// <summary>
    /// When a registered job is next due, or null when it has no more runs. The run starts up to its
    /// jitter later, on whichever instance claims it.
    /// </summary>
    /// <param name="name">The job's name.</param>
    /// <returns>The next run time, or null.</returns>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.UnknownJob"/>: the job is not registered here.</exception>
    /// <example>
    /// <code>
    /// var next = scheduler.GetNextRun("digest"); // the next weekday at 08:00
    /// </code>
    /// </example>
    public DateTimeOffset? GetNextRun(string name) => NextRunOf(GetJob(name));

    /// <summary>The jobs registered on this instance, with their schedule, next run, and how many runs of each are executing here.</summary>
    /// <returns>One entry per job.</returns>
    /// <example>
    /// <code>
    /// foreach (var job in scheduler.GetJobs()) Console.WriteLine($"{job.Name,-10} {job.Schedule,-40} {job.NextRunAt:O} {job.RunningHere}");
    /// </code>
    /// </example>
    public IReadOnlyList<JobInfo> GetJobs() => [.. Snapshot().Select(job => new JobInfo
    {
        Name = job.Name,
        Type = job.Schedule.Type,
        Schedule = job.Schedule.Description,
        NextRunAt = NextRunOf(job),
        RunningHere = job.RunningHere,
    })];

    /// <summary>Adds jobs declared with attributes, all or none; <paramref name="invoke"/> runs a declared job's method.</summary>
    internal Scheduler AddDeclared(
        IReadOnlyList<DeclaredJob> declared,
        Func<DeclaredJob, JobContext, Task> invoke,
        RegisterOptions? options)
    {
        var inputs = declared.Select(job => ToInput(job, invoke, options ?? new RegisterOptions())).ToList();
        var jobs = inputs.Select(JobFactory.Create).ToList();
        var duplicate = jobs.GroupBy(job => job.Name).FirstOrDefault(group => group.Count() > 1);
        if (duplicate is not null) throw Taken(duplicate.Key);

        bool running;
        lock (_gate)
        {
            var taken = jobs.FirstOrDefault(job => _registry.ContainsKey(job.Name));
            if (taken is not null) throw Taken(taken.Name);
            jobs.ForEach(job => _registry.Add(job.Name, job));
            running = _state == State.Running;
        }

        if (running) jobs.ForEach(Schedule);
        return this;
    }

    private JobInput ToInput(DeclaredJob declared, Func<DeclaredJob, JobContext, Task> invoke, RegisterOptions options)
    {
        var name = $"{options.Prefix}{declared.Name}";
        var (schedule, jobOptions) = declared.ToSchedule(name, options, _defaults.TimeZone, Now());
        return new JobInput(name, schedule, ctx => invoke(declared, ctx), jobOptions, _defaults, Backend);
    }

    private Scheduler Add(string name, Scheduling.Schedule schedule, Func<JobContext, Task> handler, JobOptions options)
    {
        var job = JobFactory.Create(new JobInput(name, schedule, handler, options, _defaults, Backend));

        bool running;
        lock (_gate)
        {
            if (!_registry.TryAdd(name, job)) throw Taken(name);
            running = _state == State.Running;
        }

        if (running) Schedule(job);
        return this;
    }

    private void RefuseTaken(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        lock (_gate)
        {
            if (_registry.ContainsKey(name)) throw Taken(name);
        }
    }

    private static AnarchonException Taken(string name) =>
        new(AnarchonErrorCode.JobExists, $"Job \"{name}\" is already registered");

    private async Task<RunRecord> RunManualAsync(Job job, object? data, CancellationToken stop)
    {
        await PrepareAsync(job).ConfigureAwait(false);
        return _leases is not null
            ? await _leases.RunManualAsync(job, data, stop).ConfigureAwait(false)
            : await _claimOnly.RunManualAsync(job, data, stop).ConfigureAwait(false);
    }

    private Job GetJob(string name)
    {
        lock (_gate)
        {
            return _registry.TryGetValue(name, out var job)
                ? job
                : throw new AnarchonException(AnarchonErrorCode.UnknownJob, $"Unknown job \"{name}\"");
        }
    }

    private DateTimeOffset? NextRunOf(Job job)
    {
        long? slot;
        lock (job.Gate)
        {
            slot = _state == State.Running ? job.NextSlot : job.Schedule.Next(Now());
        }

        return slot is { } ms ? DateTimeOffset.FromUnixTimeMilliseconds(ms) : null;
    }

    private bool IsStoppedAndIdle()
    {
        lock (_gate)
        {
            return _stopped && _state == State.Idle;
        }
    }
}
