// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon;
using Anarchon.Sqlite;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Puts a scheduler registered with <c>AddAnarchon()</c> on a SQLite file.</summary>
public static class SqliteAnarchonBuilderExtensions
{
    /// <summary>Uses the SQLite file a Microsoft.Data.Sqlite connection string names.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="connectionString">Such as <c>Data Source=/var/lib/myapp/jobs.db</c>.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseSqlite(builder.Configuration.GetConnectionString("sqlite")!);
    /// </code>
    /// </example>
    public static AnarchonBuilder UseSqlite(this AnarchonBuilder builder, string connectionString) =>
        builder.UseSqlite(options => options.ConnectionString = connectionString);

    /// <summary>Uses SQLite with the given options.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="configure">Sets the backend's options.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseSqlite(options =&gt;
    /// {
    ///     options.ConnectionString = "Data Source=/var/lib/myapp/jobs.db";
    ///     options.Table = "cron_runs";
    /// });
    /// </code>
    /// </example>
    public static AnarchonBuilder UseSqlite(this AnarchonBuilder builder, Action<SqliteBackendOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return builder.UseSqlite((_, options) => configure(options));
    }

    /// <summary>Uses SQLite with options that may come from other services, such as the app's configuration.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="configure">Sets the backend's options, given the app's services.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseSqlite((services, options) =&gt;
    ///     options.ConnectionString = services.GetRequiredService&lt;IConfiguration&gt;().GetConnectionString("sqlite")!);
    /// </code>
    /// </example>
    public static AnarchonBuilder UseSqlite(this AnarchonBuilder builder, Action<IServiceProvider, SqliteBackendOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        return builder.UseBackend(services =>
        {
            var options = new SqliteBackendOptions();
            configure(services, options);
            return new SqliteBackend(options);
        });
    }
}
