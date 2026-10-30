// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon;

/// <summary>What a job's handler receives.</summary>
/// <example>A job that is safe to run again: <see cref="Slot"/> is the same on every instance and every attempt
/// <code>
/// scheduler.Every("invoice", TimeSpan.FromHours(1), async ctx =>
/// {
///     await db.ExecuteAsync("INSERT INTO invoice_runs (period) VALUES (@slot) ON CONFLICT DO NOTHING", new { slot = ctx.Slot });
///     await BuildInvoicesAsync(from: ctx.ScheduledAt.AddHours(-1), to: ctx.ScheduledAt, ctx.CancellationToken);
/// });
/// </code>
/// </example>
/// <example>A long job that stops when asked, so another instance can continue it
/// <code>
/// scheduler.Every("export", TimeSpan.FromMinutes(10), async ctx =>
/// {
///     foreach (var page in pages)
///     {
///         ctx.CancellationToken.ThrowIfCancellationRequested();
///         await ExportPageAsync(page, ctx.CancellationToken);
///     }
/// });
/// </code>
/// </example>
public sealed class JobContext
{
    internal JobContext(string name, long slot, string instanceId, int attempt, RunReason reason, CancellationToken token)
    {
        Name = name;
        Slot = slot;
        InstanceId = instanceId;
        Attempt = attempt;
        Reason = reason;
        CancellationToken = token;
    }

    /// <summary>The job's name. Useful when one handler serves several jobs.</summary>
    public string Name { get; }

    /// <summary>
    /// When the run was due, in Unix milliseconds: the same value on every instance and every
    /// attempt. Use it as the run's key for idempotent writes.
    /// </summary>
    public long Slot { get; }

    /// <summary>
    /// <see cref="Slot"/> as a time: when the run was due, not when it started (up to the job's
    /// jitter later).
    /// </summary>
    public DateTimeOffset ScheduledAt => DateTimeOffset.FromUnixTimeMilliseconds(Slot);

    /// <summary>The instance running this job (you): the scheduler's <see cref="Scheduler.InstanceId"/>.</summary>
    public string InstanceId { get; }

    /// <summary>true when started with <c>RunNowAsync()</c>, for example to skip work only scheduled runs do.</summary>
    public bool IsManual => Reason == RunReason.Manual;

    /// <summary>
    /// 1 for the first try; 2, 3... when a backup took over after the previous runner died (lease
    /// backends). A later attempt may find part of the work done.
    /// </summary>
    public int Attempt { get; }

    /// <summary>Why this run started.</summary>
    public RunReason Reason { get; }

    /// <summary>
    /// Cancelled when <c>StopAsync()</c> is called, when the run times out, or (lease backends) when
    /// this instance lost the run to a backup. Pass it to everything that accepts one, and check it
    /// before side effects.
    /// </summary>
    /// <remarks>
    /// When the handler throws after <c>StopAsync()</c> cancelled it, the run is recorded as
    /// Interrupted and, on a lease backend, handed to another instance right away. When the handler
    /// returns normally, the run counts as finished.
    /// </remarks>
    public CancellationToken CancellationToken { get; }

    /// <summary>
    /// The data given for this run: the scheduler's <c>Data</c>, then the job's, then the one passed
    /// to <c>RunNowAsync()</c>, each on top of the one before. null when none was given.
    /// </summary>
    /// <remarks>
    /// A more specific value replaces a more general one, except that two
    /// <see cref="IReadOnlyDictionary{TKey, TValue}"/> of <c>string</c> to <c>object?</c> are merged,
    /// with the keys of the more specific one winning.
    /// </remarks>
    public object? Data { get; internal set; }

    /// <summary>The data as <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">The type the data was given as.</typeparam>
    /// <returns>The data.</returns>
    /// <exception cref="InvalidOperationException">The run has no data, or data of another type.</exception>
    /// <example>
    /// <code>
    /// scheduler.Every("export", TimeSpan.FromHours(1), ctx => ExportAsync(ctx.GetData&lt;ExportSettings&gt;()), new() { Data = new ExportSettings("csv") });
    /// </code>
    /// </example>
    public T GetData<T>()
    {
        if (Data is T data) return data;
        var actual = Data?.GetType().Name ?? "none";
        throw new InvalidOperationException($"Job \"{Name}\": the run's data is {actual}, not {typeof(T).Name}");
    }
}

/// <summary>
/// Gives a run its data when the run starts, so the value is read at that moment: a fresh token,
/// the permissions as they are now. If it throws, the run fails with that error.
/// </summary>
/// <param name="run">The run about to start; its <see cref="JobContext.Data"/> is still null.</param>
/// <returns>The run's data.</returns>
/// <example>
/// <code>
/// JobDataProvider freshToken = async run => new PullSettings(await auth.TokenForAsync(run.Name));
/// scheduler.Every("pull-orders", TimeSpan.FromMinutes(5), PullAsync, new() { Data = freshToken });
/// </code>
/// </example>
public delegate ValueTask<object?> JobDataProvider(JobContext run);
