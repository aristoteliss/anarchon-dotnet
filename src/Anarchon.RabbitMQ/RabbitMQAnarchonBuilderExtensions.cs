// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon;
using Anarchon.RabbitMQ;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Puts a scheduler registered with <c>AddAnarchon()</c> on RabbitMQ.</summary>
public static class RabbitMQAnarchonBuilderExtensions
{
    /// <summary>Uses RabbitMQ, connecting with an AMQP URI.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="connectionString">An AMQP URI, such as <c>amqp://rabbit.internal</c>.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseRabbitMQ(builder.Configuration.GetConnectionString("rabbitmq")!);
    /// </code>
    /// </example>
    public static AnarchonBuilder UseRabbitMQ(this AnarchonBuilder builder, string connectionString) =>
        builder.UseRabbitMQ(options => options.ConnectionString = connectionString);

    /// <summary>Uses RabbitMQ with the given options.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="configure">Sets the backend's options.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseRabbitMQ(options =&gt;
    /// {
    ///     options.ConnectionString = rabbitUri;
    ///     options.Prefix = "billing.cron.";
    /// });
    /// </code>
    /// </example>
    public static AnarchonBuilder UseRabbitMQ(this AnarchonBuilder builder, Action<RabbitMQBackendOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return builder.UseRabbitMQ((_, options) => configure(options));
    }

    /// <summary>Uses RabbitMQ with options that may come from other services, such as a connection the app registered.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="configure">Sets the backend's options, given the app's services.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseRabbitMQ((services, options) =&gt; options.Connection = services.GetRequiredService&lt;IConnection&gt;());
    /// </code>
    /// </example>
    public static AnarchonBuilder UseRabbitMQ(this AnarchonBuilder builder, Action<IServiceProvider, RabbitMQBackendOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        return builder.UseBackend(services =>
        {
            var options = new RabbitMQBackendOptions();
            configure(services, options);
            return new RabbitMQBackend(options);
        });
    }
}
