// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon;
using Anarchon.Postgres;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Puts a scheduler registered with <c>AddAnarchon()</c> on PostgreSQL.</summary>
public static class PostgresAnarchonBuilderExtensions
{
    /// <summary>Uses PostgreSQL, opening a pool of its own with an Npgsql connection string.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="connectionString">Such as <c>Host=db;Database=app;Username=app;Password=...</c>.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UsePostgres(builder.Configuration.GetConnectionString("postgres")!);
    /// </code>
    /// </example>
    public static AnarchonBuilder UsePostgres(this AnarchonBuilder builder, string connectionString) =>
        builder.UsePostgres(options => options.ConnectionString = connectionString);

    /// <summary>Uses PostgreSQL with the given options.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="configure">Sets the backend's options.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UsePostgres(options =&gt;
    /// {
    ///     options.ConnectionString = connectionString;
    ///     options.Table = "billing_cron_runs";
    /// });
    /// </code>
    /// </example>
    public static AnarchonBuilder UsePostgres(this AnarchonBuilder builder, Action<PostgresBackendOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return builder.UsePostgres((_, options) => configure(options));
    }

    /// <summary>Uses PostgreSQL with options that may come from other services, such as a data source the app registered.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="configure">Sets the backend's options, given the app's services.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddNpgsqlDataSource(connectionString);
    /// builder.Services.AddAnarchon().UsePostgres((services, options) =&gt; options.DataSource = services.GetRequiredService&lt;NpgsqlDataSource&gt;());
    /// </code>
    /// </example>
    public static AnarchonBuilder UsePostgres(this AnarchonBuilder builder, Action<IServiceProvider, PostgresBackendOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        return builder.UseBackend(services =>
        {
            var options = new PostgresBackendOptions();
            configure(services, options);
            return new PostgresBackend(options);
        });
    }
}
