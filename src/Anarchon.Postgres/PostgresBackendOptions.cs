// Copyright (C) 2026-present Aristotelis — see repository license.

using Npgsql;

namespace Anarchon.Postgres;

/// <summary>Options of <see cref="PostgresBackend"/>.</summary>
/// <example>The app's data source, and tables in a schema of their own
/// <code>
/// new PostgresBackend(new PostgresBackendOptions { DataSource = dataSource, Table = "scheduler.runs" }); // scheduler.runs, scheduler.runs_attempts, ...
/// </code>
/// </example>
public sealed class PostgresBackendOptions
{
    /// <summary>
    /// An Npgsql connection string, such as <c>Host=db;Database=app;Username=app;Password=...</c>. The
    /// backend opens a pool with it and closes the pool on stop. Default
    /// <c>Host=localhost;Database=postgres</c>. Leave it out when you set <see cref="DataSource"/>.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// A data source your app already has, such as the one <c>AddNpgsqlDataSource()</c> registers.
    /// Claims take connections from its pool; it is never disposed by the scheduler. Leave
    /// <see cref="ConnectionString"/> out when you set it.
    /// </summary>
    public NpgsqlDataSource? DataSource { get; set; }

    /// <summary>
    /// The runs table, optionally in a schema that exists (<c>jobs.cron_runs</c>). Default
    /// <c>anarchon_runs</c>. This table, <c>&lt;table&gt;_attempts</c>, <c>&lt;table&gt;_jobs</c> and
    /// <c>&lt;table&gt;_meta</c> are created on start. Services sharing a database need different names;
    /// instances that share jobs must use the same one.
    /// </summary>
    public string Table { get; set; } = "anarchon_runs";
}
