// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Data.Common;
using Anarchon.Sql;
using Npgsql;

namespace Anarchon.Postgres;

/// <summary>
/// PostgreSQL: row locks with <c>FOR UPDATE</c>, and claims of one job take turns on the job's row in
/// <c>&lt;table&gt;_jobs</c>, which the first statement of every claim writes and so locks until commit.
/// </summary>
internal sealed class PostgresDialect : SqlDialect
{
    public static readonly PostgresDialect Instance = new();

    public override string BigInt => "BIGINT";

    /// <summary>The real time (<c>clock_timestamp()</c>), not the start of the transaction (<c>now()</c>).</summary>
    public override string NowMs => "(floor(extract(epoch from clock_timestamp()) * 1000))::bigint";

    public override string ForUpdate => " FOR UPDATE";

    public override string SerializeClaims(string table) => $"""
        INSERT INTO {table}_jobs (job, claimed_at)
        VALUES (@job, 0)
        ON CONFLICT (job) DO UPDATE
        SET claimed_at = {table}_jobs.claimed_at + 1
        """;

    public override IEnumerable<string> ExtraTables(string table) =>
    [
        $"""
        CREATE TABLE IF NOT EXISTS {table}_jobs (
          job        TEXT PRIMARY KEY,
          claimed_at BIGINT NOT NULL
        )
        """,
    ];

    /// <summary>Two instances creating a table at the same moment can trip over each other in the catalog.</summary>
    public override bool IsAlreadyCreated(DbException error) =>
        error is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation or PostgresErrorCodes.DuplicateTable or PostgresErrorCodes.DuplicateObject };
}
