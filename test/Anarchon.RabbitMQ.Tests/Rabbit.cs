// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Anarchon.Backends;
using Anarchon.Specification;
using RabbitMQ.Client;

namespace Anarchon.RabbitMQ.Tests;

/// <summary>
/// The test broker, from <c>test/docker-compose.test.yml</c> (profile <c>brokers</c>) by default.
/// RABBITMQ_URL and RABBITMQ_MANAGEMENT_URL override it.
/// </summary>
public static class Rabbit
{
    public static readonly string Url = RequiredServers.Env("RABBITMQ_URL", "amqp://localhost:5672");

    private static readonly string ManagementUrl = RequiredServers.Env("RABBITMQ_MANAGEMENT_URL", "http://localhost:15672");

    /// <summary>Skips the test when the broker is down, or fails it when REQUIRE_SERVERS names "rabbitmq".</summary>
    public static Task RequireAsync() => RequiredServers.RequireAsync("rabbitmq", "RabbitMQ", Url, IsUpAsync);

    /// <summary>A queue prefix of its own for one test: tests leave their queues behind (gates expire on their own).</summary>
    public static string Prefix(string test) => $"test.{TestStore.NewScope(test)}.";

    /// <summary>A backend on the test broker.</summary>
    public static RabbitMQBackend Backend(string prefix, TimeSpan? slotTtl = null) =>
        new(new RabbitMQBackendOptions { ConnectionString = Url, Prefix = prefix, SlotTtl = slotTtl ?? TimeSpan.FromMinutes(5) });

    /// <summary>A connection of the test's own, outside any backend.</summary>
    public static Task<IConnection> ConnectAsync() => new ConnectionFactory { Uri = new Uri(Url) }.CreateConnectionAsync();

    /// <summary>
    /// Closes, from the broker's side, the connection a backend with this prefix opened (it is named
    /// <c>anarchon:&lt;prefix&gt;</c>), as a broker restart or a network failure would.
    /// </summary>
    public static async Task CloseConnectionOfAsync(string prefix)
    {
        using var http = new HttpClient { BaseAddress = new Uri(ManagementUrl) };
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.ASCII.GetBytes("guest:guest")));
        var name = await WaitForConnectionAsync(http, $"anarchon:{prefix}");

        using var closed = await http.DeleteAsync($"api/connections/{Uri.EscapeDataString(name)}");
        closed.EnsureSuccessStatusCode();
        while (await FindConnectionAsync(http, $"anarchon:{prefix}") is not null)
        {
            await Task.Delay(100);
        }
    }

    private static async Task<string> WaitForConnectionAsync(HttpClient http, string clientName)
    {
        // The management API lists a new connection after a moment.
        for (var tries = 0; tries < 100; tries++)
        {
            if (await FindConnectionAsync(http, clientName) is { } name) return name;
            await Task.Delay(100);
        }

        throw new InvalidOperationException($"No connection named {clientName} on the broker");
    }

    private static async Task<string?> FindConnectionAsync(HttpClient http, string clientName)
    {
        var connections = await http.GetFromJsonAsync<JsonElement[]>("api/connections") ?? [];
        return connections
            .Where(c => c.TryGetProperty("client_properties", out var client) && client.TryGetProperty("connection_name", out var named) && named.GetString() == clientName)
            .Select(c => c.GetProperty("name").GetString())
            .FirstOrDefault();
    }

    private static async Task<bool> IsUpAsync()
    {
        try
        {
            await using var connection = await new ConnectionFactory { Uri = new Uri(Url), RequestedConnectionTimeout = TimeSpan.FromSeconds(1.5) }.CreateConnectionAsync();
            return connection.IsOpen;
        }
        catch (Exception error) when (error is IOException or global::RabbitMQ.Client.Exceptions.BrokerUnreachableException)
        {
            return false;
        }
    }
}

public sealed class RabbitMQContractTests
{
    [Fact]
    public async Task instances_racing_for_the_same_runs_get_exactly_one_winner_each() =>
        await BackendContract.RaceAsync(await FactoryAsync("race"));

    [Fact]
    public async Task the_same_claim_sent_twice_still_wins_and_another_instance_loses() =>
        await BackendContract.ResendAsync(await FactoryAsync("resend"));

    [Fact]
    public async Task history_joins_claims_and_results_newest_first() =>
        await BackendContract.JoinHistoryAsync(await FactoryAsync("history"));

    private static async Task<Func<IBackend>> FactoryAsync(string test)
    {
        await Rabbit.RequireAsync();
        var prefix = Rabbit.Prefix(test);
        return () => Rabbit.Backend(prefix);
    }
}
