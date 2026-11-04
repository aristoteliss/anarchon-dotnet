// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Specification;

namespace Anarchon.Sql.Tests;

public sealed class RequiredServersTests
{
    [Fact]
    public async Task tests_that_check_one_server_at_the_same_time_share_one_probe()
    {
        var probes = 0;
        using var secondProbe = new ManualResetEventSlim();
        Task<bool> IsUpAsync()
        {
            // A probe holds its thread until its first await, as opening a connection does.
            if (Interlocked.Increment(ref probes) > 1) secondProbe.Set();
            else secondProbe.Wait(TimeSpan.FromMilliseconds(500));
            return Task.FromResult(true);
        }

        var server = $"server_{Guid.NewGuid():N}";
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() => RequiredServers.RequireAsync(server, "A server", "nowhere", IsUpAsync))));

        Assert.Equal(1, probes);
    }
}
