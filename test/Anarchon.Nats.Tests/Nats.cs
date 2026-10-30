// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;
using Anarchon.Specification;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace Anarchon.Nats.Tests;

/// <summary>The test server, from <c>test/docker-compose.test.yml</c> (profile <c>brokers</c>) by default. NATS_URL overrides it.</summary>
public static class NatsServer
{
    public static readonly string Url = RequiredServers.Env("NATS_URL", "nats://localhost:4222");

    /// <summary>Skips the test when the server is down, or fails it when REQUIRE_SERVERS names "nats".</summary>
    public static Task RequireAsync() => RequiredServers.RequireAsync("nats", "NATS", Url, IsUpAsync);

    /// <summary>A stream and a subject prefix of their own for one test. Tests leave them behind.</summary>
    public static (string Stream, string Prefix) Scope(string test)
    {
        var scope = TestStore.NewScope(test);
        return ($"T_{scope}", $"test.{scope}.");
    }

    public static NatsBackend Backend(string stream, string prefix, NatsStrategy strategy, int historySize = 1_000) =>
        new(new NatsBackendOptions { ConnectionString = Url, Stream = stream, Prefix = prefix, Strategy = strategy, HistorySize = historySize });

    /// <summary>A connection of the test's own, to look at the server.</summary>
    public static async Task<(NatsConnection Connection, NatsJSContext Js)> ConnectAsync()
    {
        var connection = new NatsConnection(new NatsOpts { Url = Url });
        await connection.ConnectAsync();
        return (connection, new NatsJSContext(connection));
    }

    private static async Task<bool> IsUpAsync()
    {
        try
        {
            await using var connection = new NatsConnection(new NatsOpts { Url = Url, ConnectTimeout = TimeSpan.FromSeconds(1.5) });
            await connection.ConnectAsync();
            return connection.ConnectionState == NatsConnectionState.Open;
        }
        catch (NatsException)
        {
            return false;
        }
    }
}

/// <summary>The NATS server with the Lease strategy, for the shared tests: a scope is a stream and a prefix.</summary>
public sealed class NatsLeaseTestStore : TestStore
{
    public override Task RequireAsync() => NatsServer.RequireAsync();

    public override IBackend Create(string scope) => NatsServer.Backend($"T_{scope}", $"test.{scope}.", NatsStrategy.Lease);
}

public sealed class NatsLeaseRulesTests() : LeaseRulesTests(new NatsLeaseTestStore());

public sealed class NatsSchedulerLeaseTests() : SchedulerLeaseTests(new NatsLeaseTestStore());

public sealed class NatsContractTests
{
    public static TheoryData<NatsStrategy> Strategies => [NatsStrategy.MsgId, NatsStrategy.Lease];

    [Theory]
    [MemberData(nameof(Strategies))]
    public async Task instances_racing_for_the_same_runs_get_exactly_one_winner_each(NatsStrategy strategy) =>
        await BackendContract.RaceAsync(await FactoryAsync("race", strategy));

    [Theory]
    [MemberData(nameof(Strategies))]
    public async Task the_same_claim_sent_twice_still_wins_and_another_instance_loses(NatsStrategy strategy) =>
        await BackendContract.ResendAsync(await FactoryAsync("resend", strategy));

    [Theory]
    [MemberData(nameof(Strategies))]
    public async Task history_joins_claims_and_results_newest_first(NatsStrategy strategy) =>
        await BackendContract.JoinHistoryAsync(await FactoryAsync("history", strategy));

    private static async Task<Func<IBackend>> FactoryAsync(string test, NatsStrategy strategy)
    {
        await NatsServer.RequireAsync();
        var (stream, prefix) = NatsServer.Scope(test);
        return () => NatsServer.Backend(stream, prefix, strategy);
    }
}
