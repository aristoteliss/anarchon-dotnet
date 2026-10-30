// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Specification;

/// <summary>
/// The checks every backend must pass: instances racing for the same runs get exactly one winner
/// each; a claim sent again with the same token still wins and another instance's claim loses;
/// history joins claims and results, newest first.
/// </summary>
public static class BackendContract
{
    private const int Racers = 3;
    private const int RaceRuns = 40;
    private const int HistorySize = 1_000;

    /// <summary>Claims go to slots this far ahead, so no check depends on when it runs.</summary>
    private static readonly TimeSpan Ahead = TimeSpan.FromMinutes(1);

    public static async Task RaceAsync(Func<IBackend> create)
    {
        await WithBackendsAsync(create, Racers, async backends =>
        {
            var first = FutureSlot();
            for (var run = 0; run < RaceRuns; run++)
            {
                var slot = first + (run * 1_000);
                var wins = await Task.WhenAll(backends.Select((backend, n) => backend.ClaimAsync(Claim(slot, $"i{n}"), CancellationToken.None)));
                Assert.True(wins.Count(won => won) == 1, $"run {run}: {wins.Count(won => won)} winners of one claim race");
            }
        });
    }

    public static async Task ResendAsync(Func<IBackend> create)
    {
        await WithBackendsAsync(create, 1, async backends =>
        {
            var backend = backends[0];
            var slot = FutureSlot();
            var mine = Claim(slot, "A");

            Assert.True(await backend.ClaimAsync(mine, CancellationToken.None), "a first claim");
            Assert.True(await backend.ClaimAsync(mine, CancellationToken.None), "the same claim sent again");
            Assert.False(await backend.ClaimAsync(Claim(slot, "B"), CancellationToken.None), "another instance's claim");
        });
    }

    public static async Task JoinHistoryAsync(Func<IBackend> create)
    {
        await WithBackendsAsync(create, 1, async backends =>
        {
            var backend = backends[0];
            var first = FutureSlot();
            var second = first + 1_000;
            foreach (var (slot, by) in new[] { (first, "A"), (second, "B") })
            {
                var input = Claim(slot, by);
                Assert.True(await backend.ClaimAsync(input, CancellationToken.None), $"the claim of {by}");
                await backend.RecordClaimAsync(input, CancellationToken.None);
            }

            var started = DateTimeOffset.FromUnixTimeMilliseconds(first);
            var result = new StoredResult { Slot = first, By = "A", Status = RunStatus.Failed, StartedAt = started, FinishedAt = started.AddMilliseconds(5), Duration = TimeSpan.FromMilliseconds(5), Error = "Exception: boom" };
            await backend.SaveResultAsync("job", result, HistorySize, null, CancellationToken.None);

            var records = await backend.HistoryAsync("job", 10, CancellationToken.None);
            Assert.Equal(
                [(second, "B", RunStatus.Running, null), (first, "A", RunStatus.Failed, "Exception: boom")],
                records.Select(r => (r.Slot, r.InstanceId, r.Status, r.Error)));
            Assert.Empty(await backend.HistoryAsync("never-ran", 10, CancellationToken.None));
        });
    }

    private static async Task WithBackendsAsync(Func<IBackend> create, int count, Func<IBackend[], Task> check)
    {
        var backends = Enumerable.Range(0, count).Select(_ => create()).ToArray();
        try
        {
            await Task.WhenAll(backends.Select(backend => backend.InitAsync(CancellationToken.None)));
            await Task.WhenAll(backends.Select(backend => backend.PrepareJobAsync("job", HistorySize, CancellationToken.None)));
            await check(backends);
        }
        finally
        {
            await Task.WhenAll(backends.Select(backend => backend.CloseAsync()));
        }
    }

    private static ClaimInput Claim(long slot, string instanceId) =>
        new() { Job = "job", Slot = slot, Token = Guid.NewGuid().ToString(), InstanceId = instanceId, HistorySize = HistorySize };

    private static long FutureSlot()
    {
        var ahead = DateTimeOffset.UtcNow.Add(Ahead).ToUnixTimeMilliseconds();
        return (ahead / 1_000 * 1_000) + 1_000;
    }
}
