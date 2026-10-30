// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Tests;

public sealed class HistoryTests
{
    private const long T = 1_791_633_600_000;

    [Fact]
    public void build_joins_each_claim_with_its_result_newest_first_one_record_per_attempt()
    {
        ClaimRecord[] claims =
        [
            Claim(T, "A", 1),
            Claim(T, "B", 2),
            Claim(T + 10_000, "C", 1),
        ];
        StoredResult[] results =
        [
            Result(T, "B", 2, RunStatus.Success),
            Result(T + 10_000, "C", 1, RunStatus.Failed, "TimeoutException: Timed out after 5000 ms"),
        ];

        var records = RunHistory.Build("report", claims, results, 10);

        Assert.Equal(
            [(T + 10_000, "C", 1, RunStatus.Failed), (T, "B", 2, RunStatus.Success), (T, "A", 1, RunStatus.Running)],
            records.Select(r => (r.Slot, r.InstanceId, r.Attempt, r.Status)));
        Assert.Equal("TimeoutException: Timed out after 5000 ms", records[0].Error);
        Assert.Equal(TimeSpan.FromMilliseconds(5), records[1].Duration);
        Assert.Null(records[2].FinishedAt);
        Assert.All(records, r => Assert.Equal("report", r.Job));
    }

    [Fact]
    public void build_keeps_the_newest_records_and_lets_a_backend_name_an_unfinished_attempt()
    {
        ClaimRecord[] claims = [Claim(T, "A", 1), Claim(T + 1_000, "A", 1), Claim(T + 2_000, "A", 1)];

        var records = RunHistory.Build("j", claims, [], 2, _ => RunStatus.Interrupted);

        Assert.Equal([T + 2_000, T + 1_000], records.Select(r => r.Slot));
        Assert.All(records, r => Assert.Equal(RunStatus.Interrupted, r.Status));
    }

    private static ClaimRecord Claim(long slot, string by, int attempt) =>
        new() { Slot = slot, By = by, At = DateTimeOffset.FromUnixTimeMilliseconds(slot + attempt), Attempt = attempt };

    private static StoredResult Result(long slot, string by, int attempt, RunStatus status, string? error = null) => new()
    {
        Slot = slot,
        By = by,
        Attempt = attempt,
        Status = status,
        StartedAt = DateTimeOffset.FromUnixTimeMilliseconds(slot),
        FinishedAt = DateTimeOffset.FromUnixTimeMilliseconds(slot + 5),
        Duration = TimeSpan.FromMilliseconds(5),
        Error = error,
    };
}
