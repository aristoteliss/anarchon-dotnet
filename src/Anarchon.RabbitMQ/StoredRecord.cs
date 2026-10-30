// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Text.Json;
using System.Text.Json.Serialization;
using Anarchon.Backends;

namespace Anarchon.RabbitMQ;

/// <summary>
/// One record of a job's history stream, as JSON: a claim (<c>type: "claim"</c>) or a result
/// (<c>type: "result"</c>). The Node.js package writes and reads the same records; fields this
/// version doesn't know are ignored.
/// </summary>
internal sealed record StoredRecord(
    [property: JsonPropertyName("type")] string? Type,
    [property: JsonPropertyName("slot")] long? Slot,
    [property: JsonPropertyName("by")] string? By,
    [property: JsonPropertyName("token")] string? Token = null,
    [property: JsonPropertyName("at")] long? At = null,
    [property: JsonPropertyName("manual")] bool? Manual = null,
    [property: JsonPropertyName("attempt")] int? Attempt = null,
    [property: JsonPropertyName("status")] string? Status = null,
    [property: JsonPropertyName("startedAt")] long? StartedAt = null,
    [property: JsonPropertyName("finishedAt")] long? FinishedAt = null,
    [property: JsonPropertyName("durationMs")] long? DurationMs = null,
    [property: JsonPropertyName("error")] string? Error = null)
{
    /// <summary>The record of a claim, stamped with this instance's clock.</summary>
    public static byte[] Claim(ClaimInput input, bool manual) =>
        ToJson(new StoredRecord("claim", input.Slot, input.InstanceId, input.Token, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), manual));

    /// <summary>The record of a result.</summary>
    public static byte[] Result(StoredResult result) => ToJson(new StoredRecord(
        "result",
        result.Slot,
        result.By,
        Attempt: result.Attempt,
        Status: RunText.ToText(result.Status),
        StartedAt: result.StartedAt.ToUnixTimeMilliseconds(),
        FinishedAt: result.FinishedAt.ToUnixTimeMilliseconds(),
        DurationMs: (long)result.Duration.TotalMilliseconds,
        Error: result.Error));

    /// <summary>A record read from the stream; null when the bytes aren't one.</summary>
    public static StoredRecord? Parse(ReadOnlySpan<byte> json)
    {
        try
        {
            return JsonSerializer.Deserialize(json, StoredRecordContext.Default.StoredRecord);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>The claim this record holds; null for another type or a record without a slot or an instance.</summary>
    public ClaimRecord? ToClaim()
    {
        if (Type != "claim" || Slot is not { } slot || string.IsNullOrEmpty(By)) return null;

        return new ClaimRecord { Slot = slot, By = By, At = DateTimeOffset.FromUnixTimeMilliseconds(At ?? slot), IsManual = Manual == true };
    }

    /// <summary>The result this record holds; null for another type, or a status that isn't an end.</summary>
    public StoredResult? ToResult()
    {
        if (Type != "result" || Slot is not { } slot || By is null) return null;

        var status = RunText.ToStatus(Status);
        if (status is null or RunStatus.Running) return null;

        return new StoredResult
        {
            Slot = slot,
            By = By,
            Attempt = Attempt is > 0 ? Attempt.Value : 1,
            Status = status.Value,
            StartedAt = DateTimeOffset.FromUnixTimeMilliseconds(StartedAt ?? 0),
            FinishedAt = DateTimeOffset.FromUnixTimeMilliseconds(FinishedAt ?? 0),
            Duration = TimeSpan.FromMilliseconds(DurationMs ?? 0),
            Error = Error,
        };
    }

    private static byte[] ToJson(StoredRecord record) => JsonSerializer.SerializeToUtf8Bytes(record, StoredRecordContext.Default.StoredRecord);
}

[JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull, NumberHandling = JsonNumberHandling.AllowReadingFromString)]
[JsonSerializable(typeof(StoredRecord))]
internal sealed partial class StoredRecordContext : JsonSerializerContext;
