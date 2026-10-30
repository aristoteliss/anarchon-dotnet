// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;
using StackExchange.Redis;

namespace Anarchon.Redis;

/// <summary>One way to decide who runs each run, without leases. Three are chosen between by <see cref="RedisBackendOptions.Strategy"/>.</summary>
internal interface IClaimStrategy
{
    /// <summary>Checks once that the server supports this strategy.</summary>
    Task InitAsync(IDatabase db);

    /// <summary>Runs once per job stream before its first claim.</summary>
    Task PrepareStreamAsync(IDatabase db, RedisKey stream);

    /// <summary>true for exactly one instance per slot. Safe to repeat with the same token.</summary>
    Task<bool> ClaimAsync(IDatabase db, RedisKey stream, ClaimInput input);
}

/// <summary>
/// The entry ID is <c>&lt;slot&gt;-0</c>; the server refuses an ID that is not bigger than the stream's
/// last ID, so the first XADD for a slot wins and the others get an error. A run older than the
/// stream's newest entry can never be claimed, so runs are remembered forever.
/// </summary>
internal sealed class StreamIdStrategy : IClaimStrategy
{
    public Task InitAsync(IDatabase db) => Task.CompletedTask;

    public Task PrepareStreamAsync(IDatabase db, RedisKey stream) => Task.CompletedTask;

    public async Task<bool> ClaimAsync(IDatabase db, RedisKey stream, ClaimInput input)
    {
        var id = $"{input.Slot}-0";

        try
        {
            await db.StreamAddAsync(stream, StoredData.ClaimFields(input, manual: false), id, input.HistorySize, useApproximateMaxLength: true).ConfigureAwait(false);
            return true;
        }
        catch (RedisServerException error) when (error.Message.Contains("equal or smaller than the target stream top item", StringComparison.OrdinalIgnoreCase))
        {
            // Someone holds this slot, or a later one. It can be us, when the client sent the XADD
            // again after a dropped connection: the token tells.
            return await Claims.TokenAtAsync(db, stream, id).ConfigureAwait(false) == input.Token;
        }
    }
}

/// <summary>
/// Redis 8.6+: <c>XADD key IDMP &lt;job&gt; &lt;slot&gt; * ...</c>. Redis stores the first entry and answers
/// repeats with that entry's ID, so each instance reads the entry and compares tokens. Redis remembers
/// an idempotency key for <c>IdmpDuration</c>, and at most <c>IdmpMaxSize</c> of them.
/// </summary>
internal sealed class IdmpStrategy(long durationSec, int maxSize) : IClaimStrategy
{
    private static readonly Version Required = new(8, 6);

    public async Task InitAsync(IDatabase db)
    {
        var server = await ServerInfo.ReadAsync(db).ConfigureAwait(false);
        if (server.Product == "Redis" && server.Version >= Required) return;

        throw new AnarchonException(
            AnarchonErrorCode.Server,
            $"The Idmp strategy needs Redis 8.6 or newer (this server is {server}). " +
            "Use the StreamId or Script strategy instead; both work on any Redis 5+ and on Valkey.");
    }

    /// <summary>Creates the stream if needed and sets how long, and how many, idempotency keys it remembers.</summary>
    public async Task PrepareStreamAsync(IDatabase db, RedisKey stream)
    {
        var info = await StreamInfoAsync(db, stream).ConfigureAwait(false);
        if (info is null)
        {
            // XCFGSET needs an existing stream: create an empty one.
            await db.ExecuteAsync("XADD", stream, "MAXLEN", 0, "*", "init", 1).ConfigureAwait(false);
            info = await StreamInfoAsync(db, stream).ConfigureAwait(false) ?? [];
        }

        // Only set when different: XCFGSET with new values clears the stream's memory of seen IDs.
        var same = Setting(info, "idmp-duration") == durationSec && Setting(info, "idmp-maxsize") == maxSize;
        if (!same) await db.ExecuteAsync("XCFGSET", stream, "IDMP-DURATION", durationSec, "IDMP-MAXSIZE", maxSize).ConfigureAwait(false);
    }

    public async Task<bool> ClaimAsync(IDatabase db, RedisKey stream, ClaimInput input)
    {
        object[] fields = [.. StoredData.ClaimFields(input, manual: false).SelectMany(field => new object[] { field.Name, field.Value })];
        object[] args = [stream, "IDMP", input.Job, input.Slot, "MAXLEN", "~", input.HistorySize, "*", .. fields];
        var reply = await db.ExecuteAsync("XADD", args).ConfigureAwait(false);

        // A repeat returns the ID of the entry that was stored first, so compare tokens.
        return await Claims.TokenAtAsync(db, stream, reply.ToString()).ConfigureAwait(false) == input.Token;
    }

    private static long? Setting(Dictionary<string, RedisResult> info, string name) =>
        info.TryGetValue(name, out var value) && long.TryParse(value.ToString(), out var number) ? number : null;

    private static async Task<Dictionary<string, RedisResult>?> StreamInfoAsync(IDatabase db, RedisKey stream)
    {
        try
        {
            var reply = await db.ExecuteAsync("XINFO", "STREAM", stream).ConfigureAwait(false);
            return reply.ToDictionary();
        }
        catch (RedisServerException error) when (error.Message.Contains("no such key", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }
    }
}

/// <summary>
/// What Idmp does, on any Redis 5+ and on Valkey: one script looks at the entries of the last window;
/// if the slot is already there it returns that entry's token, otherwise it appends the claim and
/// returns ours. The caller won if the token that comes back is its own.
/// </summary>
internal sealed class ScriptStrategy(long windowMs) : IClaimStrategy
{
    private const string Claim = """
        local entries = redis.call('XRANGE', KEYS[1], ARGV[2], '+')
        for i = 1, #entries do
          local fields = entries[i][2]
          local slot, token, manual = nil, nil, nil
          for j = 1, #fields, 2 do
            local name = fields[j]
            if name == 'slot' then slot = fields[j + 1]
            elseif name == 'token' then token = fields[j + 1]
            elseif name == 'manual' then manual = fields[j + 1] end
          end
          if slot == ARGV[1] and manual == nil then return token end
        end
        redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[3], '*', unpack(ARGV, 5))
        return ARGV[4]
        """;

    /// <summary>Runs the script once, which also checks that the server runs scripts at all.</summary>
    public async Task InitAsync(IDatabase db) => await db.ScriptEvaluateAsync("return 1").ConfigureAwait(false);

    public Task PrepareStreamAsync(IDatabase db, RedisKey stream) => Task.CompletedTask;

    public async Task<bool> ClaimAsync(IDatabase db, RedisKey stream, ClaimInput input)
    {
        var oldestKept = Math.Max(0, input.Slot - windowMs);
        var fields = StoredData.ClaimFields(input, manual: false).SelectMany(field => new[] { (RedisValue)field.Name, field.Value });
        RedisValue[] args = [input.Slot.ToString(System.Globalization.CultureInfo.InvariantCulture), oldestKept, input.HistorySize, input.Token, .. fields];

        var stored = await db.ScriptEvaluateAsync(Claim, [stream], args).ConfigureAwait(false);
        return stored.ToString() == input.Token;
    }
}

internal static class Claims
{
    /// <summary>The token of the stream entry with this ID, or null when there is none.</summary>
    public static async Task<string?> TokenAtAsync(IDatabase db, RedisKey stream, string id)
    {
        var entries = await db.StreamRangeAsync(stream, id, id).ConfigureAwait(false);
        if (entries.Length == 0) return null;

        var token = entries[0]["token"];
        return token.IsNull ? null : token.ToString();
    }
}
