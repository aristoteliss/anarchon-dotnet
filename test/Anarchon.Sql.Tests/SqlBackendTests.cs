// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Data.Common;
using System.Globalization;
using Anarchon.Backends;
using Microsoft.Data.Sqlite;

namespace Anarchon.Sql.Tests;

/// <summary>
/// What <see cref="SqlBackend"/> and <see cref="SqlDialect"/> promise to a database they are put
/// on: checked names, tables and store format, a start that is tried again, the dialect's hooks,
/// and the history's limits. The lease rules themselves are the shared suites in Embedded.cs.
/// </summary>
public sealed class SqlBackendTests
{
    [Theory]
    [InlineData("runs; DROP TABLE x")]
    [InlineData("1runs")]
    [InlineData("run-s")]
    [InlineData("a.b.c")]
    [InlineData("jobs.")]
    [InlineData("")]
    public void a_table_name_that_is_not_a_plain_identifier_is_refused(string table)
    {
        var error = Assert.Throws<ArgumentException>(() => new EmbeddedBackend("unused.db", table));

        Assert.Equal("table", error.ParamName);
    }

    [Theory]
    [InlineData("runs")]
    [InlineData("_cron_runs2")]
    [InlineData("jobs.cron_runs")]
    public void a_table_name_may_have_a_schema(string table)
    {
        var backend = new EmbeddedBackend("unused.db", table);

        Assert.Equal(("embedded", TimeSpan.MaxValue, true), (backend.Name, backend.DedupeWindow, backend.Runs is not null));
    }

    [Fact]
    public async Task start_creates_the_tables_and_records_the_store_format_once()
    {
        var file = Embedded.NewFile("tables");
        var backends = Enumerable.Range(0, 3).Select(_ => new EmbeddedBackend(file, "cron_runs")).ToList();

        await Task.WhenAll(backends.Select(backend => backend.InitAsync(CancellationToken.None)));
        await Task.WhenAll(backends.Select(backend => backend.CloseAsync()));

        var tables = await Embedded.ScalarAsync(file, "SELECT group_concat(name, ' ') FROM (SELECT name FROM sqlite_master WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%' ORDER BY name)");
        var formats = await Embedded.ScalarAsync(file, "SELECT group_concat(value) FROM cron_runs_meta WHERE name = 'format'");
        Assert.Equal("cron_runs cron_runs_attempts cron_runs_meta cron_runs_status", tables);
        Assert.Equal(StoreFormat.Current.ToString(CultureInfo.InvariantCulture), formats);
    }

    [Fact]
    public async Task a_store_format_newer_than_this_version_reads_is_refused()
    {
        var file = Embedded.NewFile("format");
        var first = new EmbeddedBackend(file);
        await first.InitAsync(CancellationToken.None);
        await first.CloseAsync();
        await Embedded.ScalarAsync(file, $"UPDATE runs_meta SET value = '{StoreFormat.Current + 1}' WHERE name = 'format'");

        var later = new EmbeddedBackend(file);
        var error = await Assert.ThrowsAsync<AnarchonException>(() => later.InitAsync(CancellationToken.None));
        await later.CloseAsync();

        Assert.Equal(AnarchonErrorCode.StoreFormat, error.Code);
        Assert.Contains($"The table \"runs\" holds data in store format {StoreFormat.Current + 1}", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task a_refused_server_stops_start_before_any_table_is_created_and_the_next_call_tries_again()
    {
        var file = Embedded.NewFile("refused");
        var backend = new EmbeddedBackend(file) { FailingChecks = 1 };

        var error = await Assert.ThrowsAsync<AnarchonException>(() => backend.InitAsync(CancellationToken.None));
        var tablesAfterRefusal = await Embedded.ScalarAsync(file, "SELECT count(*) FROM sqlite_master WHERE type = 'table'");
        await backend.InitAsync(CancellationToken.None);
        var won = await backend.ClaimAsync(Claim(1_000, "A"), CancellationToken.None);
        await backend.CloseAsync();

        Assert.Equal(AnarchonErrorCode.Server, error.Code);
        Assert.Equal(0L, tablesAfterRefusal);
        Assert.Equal((2, true), (backend.ServerChecks, won));
    }

    [Fact]
    public async Task a_claim_before_start_starts_the_backend_first()
    {
        var backend = new EmbeddedBackend(Embedded.NewFile("lazy"));

        var won = await backend.ClaimAsync(Claim(1_000, "A"), CancellationToken.None);
        var history = await backend.HistoryAsync("job", 10, CancellationToken.None);
        await backend.CloseAsync();

        Assert.True(won);
        Assert.Equal((1, 1), (backend.ServerChecks, history.Count));
    }

    [Fact]
    public async Task close_closes_what_the_backend_opened_and_the_backend_can_start_again()
    {
        var file = Embedded.NewFile("restart");
        var backend = new EmbeddedBackend(file);
        await backend.InitAsync(CancellationToken.None);
        Assert.True(await backend.ClaimAsync(Claim(1_000, "A"), CancellationToken.None));

        await backend.CloseAsync();
        await backend.InitAsync(CancellationToken.None);
        var won = await backend.ClaimAsync(Claim(2_000, "A"), CancellationToken.None);
        var history = await backend.HistoryAsync("job", 10, CancellationToken.None);
        await backend.CloseAsync();

        Assert.True(won);
        Assert.Equal((2, 2), (backend.Closes, backend.ServerChecks));
        Assert.Equal([2_000L, 1_000L], history.Select(record => record.Slot));
    }

    [Fact]
    public async Task a_dialect_needs_only_its_integer_type_and_its_clock()
    {
        // The example in SqlDialect's docs: every other member keeps its default.
        var backend = new EmbeddedBackend(Embedded.NewFile("minimal"), dialect: new MinimalDialect());
        var runs = backend.Runs;
        await backend.InitAsync(CancellationToken.None);

        var won = await runs.ClaimAsync(Run(1_000, "A", ClaimMode.New), CancellationToken.None);
        var taken = await runs.ClaimAsync(Run(1_000, "B", ClaimMode.New), CancellationToken.None);
        var renewed = await runs.HeartbeatAsync("job", 1_000, "A", TimeSpan.FromMinutes(1), CancellationToken.None);
        var finished = await runs.FinishAsync("job", 1_000, "A", RunStatus.Success, CancellationToken.None);
        var history = await backend.HistoryAsync("job", 10, CancellationToken.None);
        await backend.CloseAsync();

        Assert.Equal((new ClaimOutcome.Won(1), new ClaimOutcome.Running(1), true, true), (won, taken, renewed, finished));
        Assert.Equal(("A", RunReason.Scheduled), (history.Single().InstanceId, history.Single().Reason));
    }

    [Fact]
    public async Task every_claim_transaction_runs_the_dialects_statement_first_and_a_backup_checking_on_a_live_run_only_reads()
    {
        var file = Embedded.NewFile("turns");
        var dialect = new TurnsDialect();
        var backend = new EmbeddedBackend(file, dialect: dialect);
        var runs = backend.Runs;
        await backend.InitAsync(CancellationToken.None);

        await runs.ClaimAsync(Run(1_000, "A", ClaimMode.New), CancellationToken.None);
        await runs.ClaimAsync(Run(1_000, "A", ClaimMode.New), CancellationToken.None); // sent again
        await runs.ClaimAsync(Run(1_000, "B", ClaimMode.New), CancellationToken.None);
        var transactions = dialect.Transactions;
        var running = await runs.ClaimAsync(Run(1_000, "C", ClaimMode.Takeover), CancellationToken.None);
        var none = await runs.ClaimAsync(Run(9_000, "C", ClaimMode.Takeover), CancellationToken.None);
        var turns = await Embedded.ScalarAsync(file, "SELECT turns FROM runs_turns WHERE job = 'job'");
        await backend.CloseAsync();

        Assert.Equal((3, 3, 3L), (transactions, dialect.Transactions, turns));
        Assert.Equal((new ClaimOutcome.Running(1), new ClaimOutcome.None()), (running, none));
    }

    [Fact]
    public async Task a_table_another_instance_created_at_the_same_moment_does_not_stop_start_when_the_dialect_says_so()
    {
        var file = Embedded.NewFile("created");
        var first = new EmbeddedBackend(file, dialect: new RecreatingDialect(ignoreExisting: true));
        await first.InitAsync(CancellationToken.None);
        await first.CloseAsync();

        var tolerant = new EmbeddedBackend(file, dialect: new RecreatingDialect(ignoreExisting: true));
        await tolerant.InitAsync(CancellationToken.None);
        await tolerant.CloseAsync();
        var strict = new EmbeddedBackend(file, dialect: new RecreatingDialect(ignoreExisting: false));
        var error = await Assert.ThrowsAsync<SqliteException>(() => strict.InitAsync(CancellationToken.None));
        await strict.CloseAsync();

        Assert.Contains("already exists", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task claims_trim_the_history_to_its_size_keeping_the_newest()
    {
        var backend = new EmbeddedBackend(Embedded.NewFile("size"));
        await backend.InitAsync(CancellationToken.None);

        for (var slot = 1_000L; slot <= 5_000; slot += 1_000)
        {
            await backend.ClaimAsync(Claim(slot, "A") with { HistorySize = 2 }, CancellationToken.None);
        }

        var history = await backend.HistoryAsync("job", 10, CancellationToken.None);
        await backend.CloseAsync();

        // From the (HistorySize + 1)th newest on, as the Node.js package keeps them.
        Assert.Equal([5_000L, 4_000L, 3_000L], history.Select(record => record.Slot));
    }

    [Fact]
    public async Task a_result_with_a_history_ttl_drops_the_attempts_that_ended_before_it()
    {
        var backend = new EmbeddedBackend(Embedded.NewFile("ttl"));
        await backend.InitAsync(CancellationToken.None);
        var now = DateTimeOffset.UtcNow;
        await backend.ClaimAsync(Claim(1_000, "A"), CancellationToken.None);
        await backend.ClaimAsync(Claim(2_000, "B"), CancellationToken.None);

        await backend.SaveResultAsync("job", Result(1_000, "A", now), 100, null, CancellationToken.None);
        await backend.SaveResultAsync("job", Result(2_000, "B", now.AddHours(2)), 100, TimeSpan.FromHours(1), CancellationToken.None);
        var history = await backend.HistoryAsync("job", 10, CancellationToken.None);
        await backend.CloseAsync();

        Assert.Equal([(2_000L, "B", RunStatus.Success)], history.Select(record => (record.Slot, record.InstanceId, record.Status)));
    }

    [Fact]
    public async Task a_result_for_an_attempt_nobody_claimed_is_not_recorded()
    {
        var backend = new EmbeddedBackend(Embedded.NewFile("unclaimed"));
        await backend.InitAsync(CancellationToken.None);
        await backend.ClaimAsync(Claim(1_000, "A"), CancellationToken.None);

        await backend.SaveResultAsync("job", Result(1_000, "B", DateTimeOffset.UtcNow), 100, null, CancellationToken.None);
        var history = await backend.HistoryAsync("job", 10, CancellationToken.None);
        await backend.CloseAsync();

        Assert.Equal([("A", RunStatus.Running)], history.Select(record => (record.InstanceId, record.Status)));
    }

    private static ClaimInput Claim(long slot, string by) =>
        new() { Job = "job", Slot = slot, Token = Guid.NewGuid().ToString(), InstanceId = by, HistorySize = 100 };

    private static RunClaim Run(long slot, string by, ClaimMode mode) => new()
    {
        Job = "job",
        Slot = slot,
        Token = by,
        InstanceId = by,
        HistorySize = 100,
        Mode = mode,
        Reason = mode == ClaimMode.Takeover ? RunReason.Takeover : RunReason.Scheduled,
        Lease = TimeSpan.FromMinutes(1),
        Overlap = OverlapPolicy.Skip,
        MaxAttempts = 3,
    };

    private static StoredResult Result(long slot, string by, DateTimeOffset finishedAt) => new()
    {
        Slot = slot,
        By = by,
        Status = RunStatus.Success,
        StartedAt = finishedAt.AddMilliseconds(-5),
        FinishedAt = finishedAt,
        Duration = TimeSpan.FromMilliseconds(5),
    };

    /// <summary>The dialect in <see cref="SqlDialect"/>'s docs: its integer type and its clock, nothing else.</summary>
    private sealed class MinimalDialect : SqlDialect
    {
        public override string BigInt => "INTEGER";

        public override string NowMs => "CAST(ROUND((julianday('now') - 2440587.5) * 86400000.0) AS INTEGER)";
    }

    /// <summary>Counts, in a table of its own, the claims of each job that ran its statement.</summary>
    private sealed class TurnsDialect : EmbeddedDialect
    {
        public override IEnumerable<string> ExtraTables(string table) =>
            [$"CREATE TABLE IF NOT EXISTS {table}_turns (job TEXT PRIMARY KEY, turns INTEGER NOT NULL)"];

        public override string SerializeClaims(string table) =>
            $"INSERT INTO {table}_turns (job, turns) VALUES (@job, 1) ON CONFLICT (job) DO UPDATE SET turns = turns + 1";
    }

    /// <summary>Creates a table without IF NOT EXISTS, so a second start trips over the first one's table.</summary>
    private sealed class RecreatingDialect(bool ignoreExisting) : EmbeddedDialect
    {
        public override IEnumerable<string> ExtraTables(string table) => [$"CREATE TABLE {table}_extra (a INTEGER)"];

        public override bool IsAlreadyCreated(DbException error) =>
            ignoreExisting && error.Message.Contains("already exists", StringComparison.Ordinal);
    }
}
