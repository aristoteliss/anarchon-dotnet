// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Specification;

namespace Anarchon.Redis.Tests;

public sealed class ContractTests
{
    public static TheoryData<string, RedisStrategy> Cases
    {
        get
        {
            var cases = new TheoryData<string, RedisStrategy>();
            foreach (var server in new[] { Servers.Redis, Servers.Redis7, Servers.Valkey })
            {
                cases.Add(server.Name, RedisStrategy.Lease);
                cases.Add(server.Name, RedisStrategy.StreamId);
                cases.Add(server.Name, RedisStrategy.Script);
            }

            cases.Add(Servers.Redis.Name, RedisStrategy.Idmp);
            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task instances_racing_for_the_same_runs_get_exactly_one_winner_each(string server, RedisStrategy strategy) =>
        await BackendContract.RaceAsync(await FactoryAsync(server, strategy));

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task the_same_claim_sent_twice_still_wins_and_another_instance_loses(string server, RedisStrategy strategy) =>
        await BackendContract.ResendAsync(await FactoryAsync(server, strategy));

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task history_joins_claims_and_results_newest_first(string server, RedisStrategy strategy) =>
        await BackendContract.JoinHistoryAsync(await FactoryAsync(server, strategy));

    private static async Task<Func<Backends.IBackend>> FactoryAsync(string name, RedisStrategy strategy)
    {
        var server = await Servers.RequireAsync(name);
        var prefix = Servers.Prefix("contract");
        return () => new RedisBackend(new RedisBackendOptions { Configuration = server.Url, Strategy = strategy, Prefix = prefix });
    }
}
