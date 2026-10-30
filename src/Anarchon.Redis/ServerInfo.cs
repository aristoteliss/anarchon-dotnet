// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Text.RegularExpressions;
using StackExchange.Redis;

namespace Anarchon.Redis;

/// <summary>Which server and version the backend talks to, from <c>INFO server</c>.</summary>
/// <remarks>
/// INFO is read inside a script: StackExchange.Redis sends INFO itself only on connections that allow
/// admin commands, and an app's connection usually doesn't.
/// </remarks>
internal sealed partial record ServerInfo(string Product, Version Version)
{
    private const string Info = "return redis.call('INFO', 'server')";

    public static async Task<ServerInfo> ReadAsync(IDatabase db)
    {
        var info = await ReadInfoAsync(db).ConfigureAwait(false);

        // Valkey also reports redis_version (7.2.4); its own version is valkey_version.
        var valkey = ValkeyVersion().Match(info);
        if (valkey.Success) return new ServerInfo("Valkey", ToVersion(valkey.Groups[1].Value));

        var redis = RedisVersion().Match(info);
        return new ServerInfo("Redis", redis.Success ? ToVersion(redis.Groups[1].Value) : new Version(0, 0));
    }

    /// <summary>The version Redis reports for itself; Valkey reports 7.2.4 here.</summary>
    public static async Task<Version> ReadRedisVersionAsync(IDatabase db)
    {
        var redis = RedisVersion().Match(await ReadInfoAsync(db).ConfigureAwait(false));
        return redis.Success ? ToVersion(redis.Groups[1].Value) : new Version(0, 0);
    }

    private static async Task<string> ReadInfoAsync(IDatabase db)
    {
        var reply = await db.ScriptEvaluateAsync(Info).ConfigureAwait(false);
        return reply.ToString();
    }

    public override string ToString() => $"{Product} {Version}";

    private static Version ToVersion(string text) => Version.TryParse(text, out var version) ? version : new Version(0, 0);

    [GeneratedRegex(@"valkey_version:([\d.]+)")]
    private static partial Regex ValkeyVersion();

    [GeneratedRegex(@"redis_version:([\d.]+)")]
    private static partial Regex RedisVersion();
}
