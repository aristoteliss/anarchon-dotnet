// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;
using Anarchon.Specification;
using StackExchange.Redis;

namespace Anarchon.Redis.Tests;

/// <summary>A server the tests run against: its name in REQUIRE_SERVERS, where it is, and what to call it.</summary>
public sealed record Server(string Name, string Url, string Label)
{
    public override string ToString() => Label;
}

/// <summary>
/// The test servers, from <c>test/docker-compose.test.yml</c> by default. A server that is not running
/// skips its tests, unless REQUIRE_SERVERS names it: then its tests fail.
/// </summary>
public static class Servers
{
    public static readonly Server Redis = new("redis", RequiredServers.Env("REDIS_URL", "localhost:6381"), "Redis 8.6");
    public static readonly Server Redis7 = new("redis-7", RequiredServers.Env("REDIS7_URL", "localhost:6380"), "Redis 7.0");
    public static readonly Server Valkey = new("valkey", RequiredServers.Env("VALKEY_URL", "localhost:6390"), "Valkey");

    /// <summary>Every server, as theory data.</summary>
    public static TheoryData<string> Names => [Redis.Name, Redis7.Name, Valkey.Name];

    public static Server Get(string name) => new[] { Redis, Redis7, Valkey }.Single(server => server.Name == name);

    /// <summary>Skips the test when the server is down, or fails it when REQUIRE_SERVERS names the server.</summary>
    public static async Task<Server> RequireAsync(string name)
    {
        var server = Get(name);
        await RequiredServers.RequireAsync(server.Name, server.Label, server.Url, () => IsUpAsync(server));
        return server;
    }

    /// <summary>A key prefix of its own for one test: tests never flush, they leave their keys behind.</summary>
    public static string Prefix(string test) => $"test:{test}:{Guid.NewGuid():N}:";

    /// <summary>A connection that may run admin commands such as SCRIPT FLUSH.</summary>
    public static Task<ConnectionMultiplexer> AdminAsync(Server server) =>
        ConnectionMultiplexer.ConnectAsync($"{server.Url},allowAdmin=true");

    private static async Task<bool> IsUpAsync(Server server)
    {
        try
        {
            await using var connection = await ConnectionMultiplexer.ConnectAsync($"{server.Url},connectTimeout=1500,connectRetry=0");
            return connection.IsConnected;
        }
        catch (RedisConnectionException)
        {
            return false;
        }
    }
}

/// <summary>One Redis server with the Lease strategy, for the shared tests: a scope is a key prefix.</summary>
public sealed class RedisStore(Server server) : TestStore
{
    public override Task RequireAsync() => Servers.RequireAsync(server.Name);

    public override IBackend Create(string scope) =>
        new RedisBackend(new RedisBackendOptions { Configuration = server.Url, Prefix = $"test:{scope}:" });
}
