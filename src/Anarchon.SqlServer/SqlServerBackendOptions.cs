// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.SqlServer;

/// <summary>Options of <see cref="SqlServerBackend"/>.</summary>
/// <example>Tables in a schema of their own
/// <code>
/// new SqlServerBackend(new SqlServerBackendOptions { ConnectionString = connectionString, Table = "scheduler.runs" }); // scheduler.runs, scheduler.runs_attempts, ...
/// </code>
/// </example>
public sealed class SqlServerBackendOptions
{
    /// <summary>
    /// A Microsoft.Data.SqlClient connection string, such as
    /// <c>Server=db;Database=app;User Id=app;Password=...;Encrypt=true</c>, or for Azure SQL
    /// <c>Server=tcp:app.database.windows.net;Database=app;Authentication=Active Directory Default</c>.
    /// Required. Connections come from SqlClient's pool for that connection string, which the app's
    /// own connections with the same string share.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// The runs table, optionally in a schema that exists (<c>jobs.cron_runs</c>). Default
    /// <c>anarchon_runs</c>, in the user's default schema. This table, <c>&lt;table&gt;_attempts</c> and
    /// <c>&lt;table&gt;_meta</c> are created on start. Services sharing a database need different names;
    /// instances that share jobs must use the same one.
    /// </summary>
    public string Table { get; set; } = "anarchon_runs";
}
