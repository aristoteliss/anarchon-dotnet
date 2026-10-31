// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Anarchon.Backends;
using StackExchange.Redis;

namespace Anarchon.Redis;

/// <summary>
/// How results, claims and run states look in Redis. Every Anarchon sharing the keys reads and writes
/// the same layout: field names and values stay as they are; new fields may be added.
/// </summary>
internal static class StoredData
{
    /// <summary>A result as one JSON member of the job's results sorted set.</summary>
    public static string ToJson(StoredResult result)
    {
        var json = new ResultJson(
            result.Slot,
            result.By,
            result.Attempt,
            RunText.ToText(result.Status),
            result.StartedAt.ToUnixTimeMilliseconds(),
            result.FinishedAt.ToUnixTimeMilliseconds(),
            (long)result.Duration.TotalMilliseconds,
            result.Error);
        return JsonSerializer.Serialize(json, StoredJsonContext.Default.ResultJson);
    }

    /// <summary>The results in a sorted set; members that don't parse are left out.</summary>
    public static IEnumerable<StoredResult> ToResults(RedisValue[] members) => members.Select(member => ToResult(member)).OfType<StoredResult>();

    /// <summary>The fields a claim-only strategy stores per claimed run: slot, by, token, at, and manual for a manual run.</summary>
    public static NameValueEntry[] ClaimFields(ClaimInput input, bool manual)
    {
        var at = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        NameValueEntry[] fields = [new("slot", input.Slot), new("by", input.InstanceId), new("token", input.Token), new("at", at)];
        return manual ? [.. fields, new("manual", 1)] : fields;
    }

    /// <summary>The claim of a stream entry; null for an entry without a slot or an instance, such as a stream's first empty entry.</summary>
    public static ClaimRecord? ToClaim(StreamEntry entry)
    {
        var slot = Field(entry, "slot");
        var by = Field(entry, "by");
        if (!long.TryParse(slot, NumberStyles.Integer, CultureInfo.InvariantCulture, out var slotMs) || string.IsNullOrEmpty(by)) return null;

        var at = long.TryParse(Field(entry, "at"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var atMs) ? atMs : slotMs;
        var reason = RunText.ToReason(Field(entry, "reason"));
        var attempt = int.TryParse(Field(entry, "attempt"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) && n > 0 ? n : 1;
        return new ClaimRecord
        {
            Slot = slotMs,
            By = by,
            At = DateTimeOffset.FromUnixTimeMilliseconds(at),
            IsManual = reason == RunReason.Manual || Field(entry, "manual") == "1",
            Attempt = attempt,
            Reason = reason,
        };
    }

    private static string? Field(StreamEntry entry, string name)
    {
        var value = entry[name];
        return value.IsNull ? null : value.ToString();
    }

    private static StoredResult? ToResult(RedisValue member)
    {
        var json = Parse(member);
        if (json?.By is null) return null;

        var status = RunText.ToStatus(json.Status);
        if (status is null or RunStatus.Running) return null;

        return new StoredResult
        {
            Slot = json.Slot,
            By = json.By,
            Attempt = json.Attempt is > 0 ? json.Attempt.Value : 1,
            Status = status.Value,
            StartedAt = DateTimeOffset.FromUnixTimeMilliseconds(json.StartedAt),
            FinishedAt = DateTimeOffset.FromUnixTimeMilliseconds(json.FinishedAt),
            Duration = TimeSpan.FromMilliseconds(json.DurationMs),
            Error = json.Error,
        };
    }

    private static ResultJson? Parse(RedisValue member)
    {
        try
        {
            return JsonSerializer.Deserialize((string?)member ?? "", StoredJsonContext.Default.ResultJson);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}

/// <summary>A result as stored: times in Unix milliseconds, the status in lowercase.</summary>
internal sealed record ResultJson(
    [property: JsonPropertyName("slot")] long Slot,
    [property: JsonPropertyName("by")] string? By,
    [property: JsonPropertyName("attempt")] int? Attempt,
    [property: JsonPropertyName("status")] string? Status,
    [property: JsonPropertyName("startedAt")] long StartedAt,
    [property: JsonPropertyName("finishedAt")] long FinishedAt,
    [property: JsonPropertyName("durationMs")] long DurationMs,
    [property: JsonPropertyName("error")] string? Error);

/// <summary>A run's state in the Lease strategy's hash: attempt, status and lease end (server ms). Other fields are left alone.</summary>
internal sealed record RunStateJson(
    [property: JsonPropertyName("a")] int Attempt,
    [property: JsonPropertyName("s")] string? Status,
    [property: JsonPropertyName("l")] double LeaseEnd);

[JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull, NumberHandling = JsonNumberHandling.AllowReadingFromString)]
[JsonSerializable(typeof(ResultJson))]
[JsonSerializable(typeof(RunStateJson))]
internal sealed partial class StoredJsonContext : JsonSerializerContext;
