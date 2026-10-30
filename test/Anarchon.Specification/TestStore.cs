// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Collections.Concurrent;
using Anarchon.Backends;

namespace Anarchon.Specification;

/// <summary>
/// A store the shared tests run on, such as Redis 8.6 or PostgreSQL. A backend's test project
/// derives from <see cref="LeaseRulesTests"/> and <see cref="SchedulerLeaseTests"/> with one of these.
/// </summary>
public abstract class TestStore
{
    /// <summary>Skips the test when the store is not running, or fails it when REQUIRE_SERVERS names it.</summary>
    public abstract Task RequireAsync();

    /// <summary>
    /// A backend in <paramref name="scope"/>: backends of one scope share runs, as instances of one
    /// service do; scopes never see each other's jobs.
    /// </summary>
    public abstract IBackend Create(string scope);

    /// <summary>
    /// A new scope for one test: lowercase letters, digits and underscores, so it fits a key prefix, a
    /// table name or a stream name. Tests never clean up; each leaves its scope behind.
    /// </summary>
    public static string NewScope(string test) => $"{test}_{Guid.NewGuid():N}"[..(test.Length + 13)];
}

/// <summary>
/// Skips a test whose server is not running, unless REQUIRE_SERVERS names the server: then the test
/// fails. CI names every server, so a server that failed to start fails the run instead of skipping.
/// </summary>
public static class RequiredServers
{
    private static readonly ConcurrentDictionary<string, Task<bool>> Probes = new();

    private static readonly HashSet<string> Required =
        [.. (Environment.GetEnvironmentVariable("REQUIRE_SERVERS") ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)];

    /// <summary>Probes the server once per run of the tests.</summary>
    /// <param name="name">Its name in REQUIRE_SERVERS and in test/docker-compose.test.yml.</param>
    /// <param name="label">What to call it, such as "PostgreSQL 16".</param>
    /// <param name="where">Its address, for the message.</param>
    /// <param name="isUp">Answers whether it is running; never throws.</param>
    public static async Task RequireAsync(string name, string label, string where, Func<Task<bool>> isUp)
    {
        if (await Probes.GetOrAdd(name, _ => isUp())) return;

        var reason = $"{label} is not running at {where}";
        if (Required.Contains(name)) Assert.Fail($"{reason}, and REQUIRE_SERVERS names it");
        Assert.Skip($"{reason}: start it with docker compose -f test/docker-compose.test.yml up -d --wait");
    }

    /// <summary>An environment variable, or the fallback when it is unset or empty.</summary>
    public static string Env(string name, string fallback) => Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : fallback;
}
