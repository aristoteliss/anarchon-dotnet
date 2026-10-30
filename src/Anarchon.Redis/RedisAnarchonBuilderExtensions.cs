// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon;
using Anarchon.Redis;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Puts a scheduler registered with <c>AddAnarchon()</c> on Redis or Valkey.</summary>
public static class RedisAnarchonBuilderExtensions
{
    /// <summary>Uses Redis with the Lease strategy, connecting with a StackExchange.Redis configuration string.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="configuration">Such as <c>localhost:6379</c> or <c>cache:6380,password=...,ssl=true</c>.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseRedis(builder.Configuration.GetConnectionString("redis")!);
    /// </code>
    /// </example>
    public static AnarchonBuilder UseRedis(this AnarchonBuilder builder, string configuration) =>
        builder.UseRedis(options => options.Configuration = configuration);

    /// <summary>Uses Redis with the given options.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="configure">Sets the backend's options.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseRedis(options =&gt;
    /// {
    ///     options.Configuration = redisUrl;
    ///     options.Prefix = "billing:cron:";
    /// });
    /// </code>
    /// </example>
    public static AnarchonBuilder UseRedis(this AnarchonBuilder builder, Action<RedisBackendOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return builder.UseRedis((_, options) => configure(options));
    }

    /// <summary>Uses Redis with options that may come from other services, such as a connection the app registered.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="configure">Sets the backend's options, given the app's services.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddSingleton&lt;IConnectionMultiplexer&gt;(_ =&gt; ConnectionMultiplexer.Connect(redisUrl));
    /// builder.Services.AddAnarchon().UseRedis((services, options) =&gt; options.Connection = services.GetRequiredService&lt;IConnectionMultiplexer&gt;());
    /// </code>
    /// </example>
    public static AnarchonBuilder UseRedis(this AnarchonBuilder builder, Action<IServiceProvider, RedisBackendOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        return builder.UseBackend(services =>
        {
            var options = new RedisBackendOptions();
            configure(services, options);
            return new RedisBackend(options);
        });
    }
}
