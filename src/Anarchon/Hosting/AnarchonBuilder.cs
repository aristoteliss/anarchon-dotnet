// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Diagnostics.CodeAnalysis;
using Anarchon.Backends;
using Anarchon.Declarations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Anarchon;

/// <summary>
/// Sets up one scheduler in a service collection: its backend and its jobs. Returned by
/// <c>AddAnarchon()</c>; backend packages add their own methods, such as <c>UseRedis()</c>.
/// </summary>
/// <example>
/// <code>
/// builder.Services.AddAnarchon(options =&gt; options.TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Athens"))
///     .UseRedis(builder.Configuration.GetConnectionString("redis")!)
///     .AddJobs&lt;ReportJobs&gt;();
/// </code>
/// </example>
public sealed class AnarchonBuilder
{
    internal AnarchonBuilder(IServiceCollection services, string name)
    {
        Services = services;
        Name = name;
    }

    /// <summary>The service collection the scheduler is registered in.</summary>
    public IServiceCollection Services { get; }

    /// <summary>The scheduler's name: empty for the default scheduler, else its service key.</summary>
    public string Name { get; }

    /// <summary>Sets the scheduler's backend, created from the services when the scheduler is.</summary>
    /// <param name="factory">Creates the backend.</param>
    /// <returns>This builder.</returns>
    /// <example>A backend on a Redis connection the app already registered
    /// <code>
    /// builder.Services.AddAnarchon().UseBackend(services =&gt;
    ///     new RedisBackend(new RedisBackendOptions { Connection = services.GetRequiredService&lt;IConnectionMultiplexer&gt;() }));
    /// </code>
    /// </example>
    public AnarchonBuilder UseBackend(Func<IServiceProvider, IBackend> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        Services.AddOptions<SchedulerOptions>(Name).Configure<IServiceProvider>((options, services) => options.Backend = factory(services));
        return this;
    }

    /// <summary>
    /// Adds the jobs declared with <see cref="CronAttribute"/>, <see cref="EveryAttribute"/> and
    /// <see cref="AtAttribute"/> on <typeparamref name="T"/>'s public methods. Every run creates a new
    /// service scope and gets <typeparamref name="T"/> from it, so the class can take scoped services
    /// such as a database context. <typeparamref name="T"/> is registered as scoped unless it already is.
    /// </summary>
    /// <typeparam name="T">The class whose methods are jobs.</typeparam>
    /// <param name="options">A prefix for the job names, and settings for every job of the class.</param>
    /// <returns>This builder.</returns>
    /// <example>
    /// <code>
    /// public sealed class ReportJobs(ReportsDbContext db)
    /// {
    ///     [Cron("0 6 * * *", TimeZone = "Europe/Athens")]
    ///     public Task Daily(JobContext ctx) =&gt; db.BuildDailyAsync(ctx.ScheduledAt, ctx.CancellationToken);
    /// }
    ///
    /// builder.Services.AddAnarchon().UseRedis(redisUrl).AddJobs&lt;ReportJobs&gt;();
    /// </code>
    /// </example>
    public AnarchonBuilder AddJobs<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] T>(RegisterOptions? options = null)
        where T : class
    {
        var declared = DeclaredJob.Read(typeof(T));
        Services.TryAddScoped<T>();
        Services.Configure<SchedulerSetup>(Name, setup => setup.Steps.Add((scheduler, services) =>
            scheduler.AddDeclared(declared, (job, ctx) => RunScopedAsync<T>(services, job, ctx), options)));
        return this;
    }

    private static async Task RunScopedAsync<T>(IServiceProvider services, DeclaredJob job, JobContext context)
        where T : class
    {
        var scope = services.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var target = scope.ServiceProvider.GetRequiredService<T>();
            await job.InvokeAsync(target, context).ConfigureAwait(false);
        }
    }
}

/// <summary>What happens to a scheduler created from the services, after it is created.</summary>
internal sealed class SchedulerSetup
{
    public List<Action<Scheduler, IServiceProvider>> Steps { get; } = [];
}
