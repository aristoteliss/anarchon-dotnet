// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Reflection;
using Anarchon.Scheduling;
using Cronos;

namespace Anarchon.Declarations;

/// <summary>A job declared with an attribute on a method: its attribute, its method and how to call it.</summary>
internal sealed class DeclaredJob
{
    private readonly JobAttribute _attribute;
    private readonly MethodInfo _method;
    private readonly Func<JobContext, object?[]> _arguments;

    private DeclaredJob(string name, JobAttribute attribute, MethodInfo method, Func<JobContext, object?[]> arguments)
    {
        Name = name;
        _attribute = attribute;
        _method = method;
        _arguments = arguments;
    }

    /// <summary>The name in the attribute, or <c>Type-Method</c>.</summary>
    public string Name { get; }

    /// <summary>The jobs declared on a type's public instance methods, inherited ones included; an override's own attributes replace the overridden method's.</summary>
    /// <exception cref="ArgumentException">The type declares no jobs, or a job method has a signature the scheduler can't call.</exception>
    public static IReadOnlyList<DeclaredJob> Read([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] Type type)
    {
        var jobs = new List<DeclaredJob>();
        foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance))
        {
            foreach (var attribute in method.GetCustomAttributes<JobAttribute>(inherit: false))
            {
                var arguments = Bind(type, method);
                jobs.Add(new DeclaredJob(attribute.Name ?? $"{type.Name}-{method.Name}", attribute, method, arguments));
            }
        }

        if (jobs.Count == 0)
        {
            throw new ArgumentException($"{type.Name} declares no jobs: mark public methods with [Cron], [Every] or [At]", nameof(type));
        }

        return jobs;
    }

    /// <summary>Calls the method on <paramref name="target"/> and waits for it.</summary>
    public async Task InvokeAsync(object target, JobContext context)
    {
        var result = _method.Invoke(target, BindingFlags.DoNotWrapExceptions, null, _arguments(context), CultureInfo.InvariantCulture);
        switch (result)
        {
            case Task task:
                await task.ConfigureAwait(false);
                break;
            case ValueTask valueTask:
                await valueTask.ConfigureAwait(false);
                break;
        }
    }

    /// <summary>The job's schedule and options: the attribute's settings over the object's defaults.</summary>
    /// <exception cref="ArgumentException">A setting in the attribute can't be read.</exception>
    public (Scheduling.Schedule Schedule, JobOptions Options) ToSchedule(string name, RegisterOptions register, TimeZoneInfo? zone, long nowMs)
    {
        try
        {
            return _attribute switch
            {
                CronAttribute cron => ToCron(cron, register, zone, nowMs),
                EveryAttribute every => ToEvery(every, register),
                AtAttribute at => (new OnceSchedule(ParseWhen(at.When)), Fill(new JobOptions(), register.Defaults)),
                _ => throw new ArgumentException($"{_attribute.GetType().Name} is not a job attribute"),
            };
        }
        catch (Exception error) when (error is FormatException or CronFormatException or TimeZoneNotFoundException or ArgumentException)
        {
            throw new ArgumentException($"Job \"{name}\" ({_method.DeclaringType?.Name}.{_method.Name}): {error.Message}", nameof(name), error);
        }
    }

    private (Scheduling.Schedule, JobOptions) ToCron(CronAttribute cron, RegisterOptions register, TimeZoneInfo? zone, long nowMs)
    {
        var timeZone = cron.TimeZone is { } id ? TimeZoneInfo.FindSystemTimeZoneById(id) : register.TimeZone;
        var options = Fill(new CronJobOptions { TimeZone = timeZone }, register.Defaults);
        options.Overlap = cron.OverlapSetting ?? register.Defaults?.Overlap;
        return (CronSchedule.Create(cron.Expression, timeZone ?? zone, nowMs), options);
    }

    private (Scheduling.Schedule, JobOptions) ToEvery(EveryAttribute every, RegisterOptions register)
    {
        var interval = (long)Duration.Parse(every.Interval).TotalMilliseconds;
        if (interval < 1_000) throw new ArgumentException($"the interval must be at least 1s (got {interval} ms)");

        var offset = every.Offset is { } text ? (long)Duration.Parse(text).TotalMilliseconds : 0;
        var options = Fill(new EveryJobOptions(), register.Defaults);
        options.Overlap = every.OverlapSetting ?? register.Defaults?.Overlap;
        return (new EverySchedule(interval, offset), options);
    }

    /// <summary>Sets every option the attribute gives, and the object's default for the others.</summary>
    private T Fill<T>(T options, JobOptions? defaults)
        where T : JobOptions
    {
        var a = _attribute;
        options.Jitter = ParseZeroOrMore(a.Jitter) ?? defaults?.Jitter;
        options.MaxLate = Parse(a.MaxLate) ?? defaults?.MaxLate;
        options.Timeout = Parse(a.Timeout) ?? defaults?.Timeout;
        options.HistoryTtl = Parse(a.HistoryTtl) ?? defaults?.HistoryTtl;
        options.CatchUp = ParseZeroOrMore(a.CatchUp) ?? defaults?.CatchUp;
        options.HistorySize = a.HistorySizeSetting ?? defaults?.HistorySize;
        options.MaxAttempts = a.MaxAttemptsSetting ?? defaults?.MaxAttempts;
        options.YieldAfterFailure = a.YieldAfterFailureSetting ?? defaults?.YieldAfterFailure;
        options.Data = defaults?.Data;
        return options;
    }

    private static TimeSpan? Parse(string? duration) => duration is null ? null : Duration.Parse(duration);

    /// <summary>A duration for an option where zero means "none" (Jitter, CatchUp): <c>"0"</c> or zero of one unit, such as <c>"0s"</c>.</summary>
    private static TimeSpan? ParseZeroOrMore(string? duration) => duration is null ? null : IsZero(duration) ? TimeSpan.Zero : Duration.Parse(duration);

    private static bool IsZero(string duration)
    {
        var number = duration.Trim().TrimEnd('d', 'h', 'm', 's').Trim();
        return number.Length > 0 && number.All(c => c is '0' or '.') && number.Count(c => c == '.') <= 1;
    }

    private static long ParseWhen(string when) =>
        DateTimeOffset.Parse(when, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal).ToUnixTimeMilliseconds();

    /// <summary>
    /// How to call the method: its parameters may be <see cref="JobContext"/> and <see cref="CancellationToken"/>,
    /// and it returns <see cref="Task"/>, <see cref="ValueTask"/> or nothing.
    /// </summary>
    private static Func<JobContext, object?[]> Bind(Type type, MethodInfo method)
    {
        var label = $"{type.Name}.{method.Name}";
        var returns = method.ReturnType;
        var callable = returns == typeof(void) || typeof(Task).IsAssignableFrom(returns) || returns == typeof(ValueTask);
        if (!callable) throw new ArgumentException($"{label}: a job method returns Task, ValueTask or void, not {returns.Name}", nameof(method));

        var parameters = method.GetParameters();
        var unknown = parameters.FirstOrDefault(p => p.ParameterType != typeof(JobContext) && p.ParameterType != typeof(CancellationToken));
        if (unknown is not null)
        {
            throw new ArgumentException($"{label}: a job method takes JobContext and CancellationToken only, not {unknown.ParameterType.Name} {unknown.Name}", nameof(method));
        }

        return ctx => [.. parameters.Select(p => p.ParameterType == typeof(JobContext) ? ctx : (object)ctx.CancellationToken)];
    }
}
