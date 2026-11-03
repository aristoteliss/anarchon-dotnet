// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon;
using Anarchon.SqlServer;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Puts a scheduler registered with <c>AddAnarchon()</c> on SQL Server or Azure SQL.</summary>
public static class SqlServerAnarchonBuilderExtensions
{
    /// <summary>Uses the SQL Server database a Microsoft.Data.SqlClient connection string names.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="connectionString">Such as <c>Server=db;Database=app;User Id=app;Password=...;Encrypt=true</c>.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseSqlServer(builder.Configuration.GetConnectionString("sqlserver")!);
    /// </code>
    /// </example>
    public static AnarchonBuilder UseSqlServer(this AnarchonBuilder builder, string connectionString) =>
        builder.UseSqlServer(options => options.ConnectionString = connectionString);

    /// <summary>Uses SQL Server with the given options.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="configure">Sets the backend's options.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseSqlServer(options =&gt;
    /// {
    ///     options.ConnectionString = connectionString;
    ///     options.Table = "billing_cron_runs";
    /// });
    /// </code>
    /// </example>
    public static AnarchonBuilder UseSqlServer(this AnarchonBuilder builder, Action<SqlServerBackendOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return builder.UseSqlServer((_, options) => configure(options));
    }

    /// <summary>Uses SQL Server with options that may come from other services, such as the app's configuration.</summary>
    /// <param name="builder">The scheduler's builder.</param>
    /// <param name="configure">Sets the backend's options, given the app's services.</param>
    /// <returns>The builder.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddAnarchon().UseSqlServer((services, options) =&gt;
    ///     options.ConnectionString = services.GetRequiredService&lt;IConfiguration&gt;().GetConnectionString("sqlserver"));
    /// </code>
    /// </example>
    public static AnarchonBuilder UseSqlServer(this AnarchonBuilder builder, Action<IServiceProvider, SqlServerBackendOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        return builder.UseBackend(services =>
        {
            var options = new SqlServerBackendOptions();
            configure(services, options);
            return new SqlServerBackend(options);
        });
    }
}
