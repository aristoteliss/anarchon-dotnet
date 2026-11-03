// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Sql;

/// <summary>
/// The tables of one backend, named after its validated table name. The Node.js package creates the
/// same tables with the same columns, so instances of both can share them.
/// </summary>
internal sealed class SqlTables(string table)
{
    /// <summary>One row per run: its attempt, the current token, its status and the lease end.</summary>
    public string Runs => table;

    /// <summary>One row per attempt: who, why, when, and how it ended (the history).</summary>
    public string Attempts => $"{table}_attempts";

    /// <summary>Name and value pairs; holds the store format.</summary>
    public string Meta => $"{table}_meta";

    /// <summary>The statements that create every table, each safe to run again.</summary>
    public IEnumerable<string> Create(SqlDialect dialect)
    {
        var bigint = dialect.BigInt;
        var key = dialect.KeyText;
        var text = dialect.Text;
        // An index lives in the table's schema, so its name drops the schema's dot.
        var index = $"{table.Replace('.', '_')}_status";

        return
        [
            dialect.CreateTable(Runs, $"""
              job          {key} NOT NULL,
              slot         {bigint} NOT NULL,
              attempt      INTEGER NOT NULL,
              max_attempts INTEGER NOT NULL,
              released     INTEGER NOT NULL DEFAULT 0,
              token        {text} NOT NULL,
              status       {key} NOT NULL,
              instance_id  {text} NOT NULL,
              lease_until  {bigint} NOT NULL,
              PRIMARY KEY (job, slot)
            """),
            dialect.CreateIndex(index, Runs, "job, status"),
            dialect.CreateTable(Attempts, $"""
              job         {key} NOT NULL,
              slot        {bigint} NOT NULL,
              attempt     INTEGER  NOT NULL,
              instance_id {text} NOT NULL,
              token       {text} NOT NULL,
              reason      {text} NOT NULL,
              manual      SMALLINT NOT NULL,
              claimed_at  {bigint} NOT NULL,
              status      {text},
              started_at  {bigint},
              finished_at {bigint},
              duration_ms {bigint},
              error       {text},
              PRIMARY KEY (job, slot, attempt)
            """),
            .. dialect.ExtraTables(table),
            dialect.CreateTable(Meta, $"""
              name  {key} PRIMARY KEY,
              value {text} NOT NULL
            """),
            dialect.InsertIfNew(Meta, "name, value", $"'format', '{StoreFormat.Current}'"),
        ];
    }
}
