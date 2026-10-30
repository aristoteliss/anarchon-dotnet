// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon;
using Anarchon.Hosting;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers Anarchon schedulers in a service collection.</summary>
public static class AnarchonServiceCollectionExtensions
{
    /// <summary>
    /// Registers the default <see cref="Scheduler"/> as a singleton, started and stopped with the
    /// host. Calling it again returns the same scheduler's builder, and adds to its options.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Sets the scheduler's options. The backend is set on the builder.</param>
    /// <returns>A builder for the scheduler's backend and jobs.</returns>
    /// <example>
    /// <code>
    /// var builder = Host.CreateApplicationBuilder(args);
    /// builder.Services.AddAnarchon(options =&gt; options.InstanceId = Environment.GetEnvironmentVariable("POD_NAME"))
    ///     .UseRedis(builder.Configuration.GetConnectionString("redis")!)
    ///     .AddJobs&lt;ReportJobs&gt;();
    /// await builder.Build().RunAsync();
    /// </code>
    /// </example>
    public static AnarchonBuilder AddAnarchon(this IServiceCollection services, Action<SchedulerOptions> configure) =>
        services.AddAnarchon(Options.Options.DefaultName, configure);

    /// <summary>Registers the default <see cref="Scheduler"/> with default options. See <see cref="AddAnarchon(IServiceCollection, Action{SchedulerOptions})"/>.</summary>
    /// <param name="services">The service collection.</param>
    /// <returns>A builder for the scheduler's backend and jobs.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseRedis("localhost:6379").AddJobs&lt;ReportJobs&gt;();
    /// </code>
    /// </example>
    public static AnarchonBuilder AddAnarchon(this IServiceCollection services) =>
        services.AddAnarchon(Options.Options.DefaultName, null);

    /// <summary>
    /// Registers a named <see cref="Scheduler"/> as a keyed singleton, started and stopped with the
    /// host. Use it for jobs that run on another store than the default scheduler's.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The scheduler's name and service key.</param>
    /// <param name="configure">Sets the scheduler's options.</param>
    /// <returns>A builder for the scheduler's backend and jobs.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon("billing").UseRedis(billingRedisUrl).AddJobs&lt;BillingJobs&gt;();
    /// app.MapGet("/billing/jobs", ([FromKeyedServices("billing")] Scheduler billing) =&gt; billing.GetJobs());
    /// </code>
    /// </example>
    public static AnarchonBuilder AddAnarchon(this IServiceCollection services, string name, Action<SchedulerOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(name);

        var optionsBuilder = services.AddOptions<SchedulerOptions>(name);
        if (configure is not null) optionsBuilder.Configure(configure);
        if (IsRegistered(services, name)) return new AnarchonBuilder(services, name);

        services.AddSingleton(new SchedulerRegistration(name));
        optionsBuilder.Configure<IServiceProvider>((options, provider) =>
        {
            options.LoggerFactory ??= provider.GetService<ILoggerFactory>();
            options.TimeProvider ??= provider.GetService<TimeProvider>();
        });

        if (name.Length == 0) services.AddSingleton(provider => Create(provider, name));
        else services.AddKeyedSingleton(name, (provider, _) => Create(provider, name));
        services.AddSingleton<IHostedService>(provider => new SchedulerHostedService(Resolve(provider, name)));
        return new AnarchonBuilder(services, name);
    }

    private static bool IsRegistered(IServiceCollection services, string name) =>
        services.Any(d => d.ServiceType == typeof(SchedulerRegistration) && d.ImplementationInstance is SchedulerRegistration r && r.Name == name);

    private static Scheduler Create(IServiceProvider provider, string name)
    {
        var options = provider.GetRequiredService<IOptionsMonitor<SchedulerOptions>>().Get(name);
        var setup = provider.GetRequiredService<IOptionsMonitor<SchedulerSetup>>().Get(name);
        var scheduler = new Scheduler(options);
        foreach (var step in setup.Steps)
        {
            step(scheduler, provider);
        }

        return scheduler;
    }

    private static Scheduler Resolve(IServiceProvider provider, string name) =>
        name.Length == 0 ? provider.GetRequiredService<Scheduler>() : provider.GetRequiredKeyedService<Scheduler>(name);

    /// <summary>Marks a scheduler name as registered, so a second AddAnarchon() with it only adds options.</summary>
    private sealed record SchedulerRegistration(string Name);
}
