// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon;

/// <summary>
/// What every job attribute takes. Durations are text that <see cref="Duration.Parse"/> reads, such
/// as <c>"30s"</c> or <c>"1h30m"</c>. A setting left out falls back to <see cref="RegisterOptions"/>,
/// then to the scheduler's options.
/// </summary>
/// <example>
/// <code>
/// public sealed class Reports(IMailer mailer)
/// {
///     [Cron("0 8 * * 1-5", TimeZone = "Europe/Athens", Timeout = "20m")]
///     public Task Digest(JobContext ctx) =&gt; mailer.SendDigestAsync(ctx.ScheduledAt, ctx.CancellationToken);
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = false)]
public abstract class JobAttribute : Attribute
{
    private int? _maxAttempts;
    private int? _historySize;
    private bool? _yieldAfterFailure;

    private protected JobAttribute()
    {
    }

    /// <summary>The job's name. Default <c>Type-Method</c>.</summary>
    public string? Name { get; set; }

    /// <summary>See <see cref="JobOptions.Jitter"/>. <c>"0"</c> or <c>"0s"</c>: no jitter.</summary>
    public string? Jitter { get; set; }

    /// <summary>See <see cref="JobOptions.MaxLate"/>.</summary>
    public string? MaxLate { get; set; }

    /// <summary>See <see cref="JobOptions.Timeout"/>.</summary>
    public string? Timeout { get; set; }

    /// <summary>See <see cref="JobOptions.HistoryTtl"/>.</summary>
    public string? HistoryTtl { get; set; }

    /// <summary>See <see cref="JobOptions.CatchUp"/>. <c>"0"</c> or <c>"0s"</c>: no catch-up.</summary>
    public string? CatchUp { get; set; }

    /// <summary>See <see cref="JobOptions.HistorySize"/>.</summary>
    public int HistorySize
    {
        get => _historySize ?? 0;
        set => _historySize = value;
    }

    /// <summary>See <see cref="JobOptions.MaxAttempts"/>.</summary>
    public int MaxAttempts
    {
        get => _maxAttempts ?? 0;
        set => _maxAttempts = value;
    }

    /// <summary>See <see cref="JobOptions.YieldAfterFailure"/>.</summary>
    public bool YieldAfterFailure
    {
        get => _yieldAfterFailure ?? false;
        set => _yieldAfterFailure = value;
    }

    internal int? HistorySizeSetting => _historySize;

    internal int? MaxAttemptsSetting => _maxAttempts;

    internal bool? YieldAfterFailureSetting => _yieldAfterFailure;
}

/// <summary>Declares a cron job on a method. See <see cref="Scheduler.Cron"/>.</summary>
/// <param name="expression">The cron expression, 5 fields or 6 with seconds first.</param>
/// <example>
/// <code>
/// [Cron("0 2 * * *", Name = "nightly-export", Timeout = "2h", MaxAttempts = 2)]
/// public async Task ExportAsync(CancellationToken ct) =&gt; await exporter.RunAsync(ct);
/// </code>
/// </example>
public sealed class CronAttribute(string expression) : JobAttribute
{
    private OverlapPolicy? _overlap;

    /// <summary>The cron expression.</summary>
    public string Expression { get; } = expression;

    /// <summary>The time zone, as an IANA or Windows id, such as <c>Europe/Athens</c>. See <see cref="CronJobOptions.TimeZone"/>.</summary>
    public string? TimeZone { get; set; }

    /// <summary>See <see cref="RecurringJobOptions.Overlap"/>.</summary>
    public OverlapPolicy Overlap
    {
        get => _overlap ?? OverlapPolicy.Skip;
        set => _overlap = value;
    }

    internal OverlapPolicy? OverlapSetting => _overlap;
}

/// <summary>Declares an interval job on a method. See <see cref="Scheduler.Every"/>.</summary>
/// <param name="interval">The time between runs, such as <c>"10s"</c> or <c>"1h30m"</c>.</param>
/// <example>
/// <code>
/// [Every("10s", Name = "sync-customers", Timeout = "8s")]
/// public Task SyncAsync(JobContext ctx) =&gt; customers.SyncAsync(ctx.CancellationToken);
/// </code>
/// </example>
public sealed class EveryAttribute(string interval) : JobAttribute
{
    private OverlapPolicy? _overlap;

    /// <summary>The time between runs.</summary>
    public string Interval { get; } = interval;

    /// <summary>See <see cref="EveryJobOptions.Offset"/>.</summary>
    public string? Offset { get; set; }

    /// <summary>See <see cref="RecurringJobOptions.Overlap"/>.</summary>
    public OverlapPolicy Overlap
    {
        get => _overlap ?? OverlapPolicy.Skip;
        set => _overlap = value;
    }

    internal OverlapPolicy? OverlapSetting => _overlap;
}

/// <summary>Declares a one-off job on a method. See <see cref="Scheduler.At"/>.</summary>
/// <param name="when">When it runs, as an ISO 8601 time; without an offset it is read as UTC.</param>
/// <example>
/// <code>
/// [At("2026-12-01T03:00:00Z", Name = "migrate-2026-12", MaxAttempts = 1)]
/// public Task MigrateAsync(CancellationToken ct) =&gt; migrations.RunAsync(ct);
/// </code>
/// </example>
public sealed class AtAttribute(string when) : JobAttribute
{
    /// <summary>When it runs.</summary>
    public string When { get; } = when;
}

/// <summary>What <see cref="Scheduler.Register"/> and <c>AddJobs&lt;T&gt;()</c> apply to every job of an object.</summary>
/// <example>
/// <code>
/// scheduler.Register(new Reports(tenant.Mailer), new RegisterOptions
/// {
///     Prefix = $"{tenant.Id}:",                                     // "acme:Reports-Digest"
///     TimeZone = TimeZoneInfo.FindSystemTimeZoneById(tenant.TimeZone),
///     Defaults = new() { Timeout = TimeSpan.FromMinutes(1) },
/// });
/// </code>
/// </example>
public sealed class RegisterOptions
{
    /// <summary>Goes in front of every job name, so several objects of one class can each have their own jobs.</summary>
    public string? Prefix { get; set; }

    /// <summary>The time zone of the object's cron jobs that name none. Default: the scheduler's.</summary>
    public TimeZoneInfo? TimeZone { get; set; }

    /// <summary>
    /// Settings for every job of the object; a setting in an attribute wins. Overlap doesn't apply to
    /// one-off jobs.
    /// </summary>
    public RecurringJobOptions? Defaults { get; set; }
}
