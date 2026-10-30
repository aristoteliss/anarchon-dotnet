// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon;
using Anarchon.Nats;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Puts a scheduler registered with <c>AddAnarchon()</c> on NATS JetStream.</summary>
public static class NatsAnarchonBuilderExtensions
{
    /// <summary>Uses NATS with the MsgId strategy, connecting to a NATS URL.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="connectionString">A NATS URL, or several separated by commas, such as <c>nats://nats.internal:4222</c>.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseNats(builder.Configuration.GetConnectionString("nats")!);
    /// </code>
    /// </example>
    public static AnarchonBuilder UseNats(this AnarchonBuilder builder, string connectionString) =>
        builder.UseNats(options => options.ConnectionString = connectionString);

    /// <summary>Uses NATS with the given options.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="configure">Sets the backend's options.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseNats(options =&gt;
    /// {
    ///     options.ConnectionString = natsUrl;
    ///     options.Strategy = NatsStrategy.Lease;
    ///     options.Stream = "BILLING_CRON";
    /// });
    /// </code>
    /// </example>
    public static AnarchonBuilder UseNats(this AnarchonBuilder builder, Action<NatsBackendOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return builder.UseNats((_, options) => configure(options));
    }

    /// <summary>Uses NATS with options that may come from other services, such as a connection the app registered.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="configure">Sets the backend's options, given the app's services.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseNats((services, options) =&gt; options.Connection = services.GetRequiredService&lt;INatsConnection&gt;());
    /// </code>
    /// </example>
    public static AnarchonBuilder UseNats(this AnarchonBuilder builder, Action<IServiceProvider, NatsBackendOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        return builder.UseBackend(services =>
        {
            var options = new NatsBackendOptions();
            configure(services, options);
            return new NatsBackend(options);
        });
    }
}
