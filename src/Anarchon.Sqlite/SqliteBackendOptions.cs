// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.Sqlite;

/// <summary>Options of <see cref="SqliteBackend"/>.</summary>
/// <example>Several processes on one machine, sharing one file
/// <code>
/// new SqliteBackend(new SqliteBackendOptions { ConnectionString = "Data Source=/var/lib/myapp/jobs.db;Default Timeout=10", Table = "cron_runs" });
/// </code>
/// </example>
public sealed class SqliteBackendOptions
{
    /// <summary>
    /// A Microsoft.Data.Sqlite connection string that names a file, such as
    /// <c>Data Source=/var/lib/myapp/jobs.db</c>. Every instance must open the same file. Its
    /// <c>Default Timeout</c> (30 seconds unless set) is how long a write waits for another process's.
    /// Default <c>Data Source=anarchon.db</c>, in the working directory.
    /// </summary>
    public string ConnectionString { get; set; } = "Data Source=anarchon.db";

    /// <summary>
    /// The runs table, without a schema. Default <c>anarchon_runs</c>. This table,
    /// <c>&lt;table&gt;_attempts</c> and <c>&lt;table&gt;_meta</c> are created on start. Services sharing a
    /// file need different names; instances that share jobs must use the same one.
    /// </summary>
    public string Table { get; set; } = "anarchon_runs";
}
