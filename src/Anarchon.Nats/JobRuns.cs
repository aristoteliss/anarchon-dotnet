// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Anarchon.Nats;

/// <summary>
/// The value of a job's key in the bucket: <c>{ "runs": { "&lt;slot&gt;": run } }</c>, the same JSON as
/// the Node.js package writes. It is kept as a JSON tree and copied on every change, so fields a
/// newer version adds, to the value or to a run, survive the writes of an older one.
/// </summary>
internal sealed class JobRuns
{
    private readonly JsonObject _root;

    private JobRuns(JsonObject root) => _root = root;

    /// <summary>A job nobody claimed yet.</summary>
    public static JobRuns Empty => new(new JsonObject { ["runs"] = new JsonObject() });

    /// <summary>The runs, by slot.</summary>
    public IEnumerable<(long Slot, StoredRun Run)> Runs =>
        RunsObject().Select(pair => (long.Parse(pair.Key, CultureInfo.InvariantCulture), new StoredRun((JsonObject)pair.Value!)));

    /// <summary>Reads a stored value.</summary>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.Store"/>: the value is not the state of Anarchon runs.</exception>
    public static JobRuns Parse(string job, byte[] value)
    {
        JsonNode? node;
        try
        {
            node = JsonNode.Parse(value);
        }
        catch (JsonException)
        {
            node = null;
        }

        if (node is JsonObject root && root["runs"] is JsonObject) return new JobRuns(root);
        throw new AnarchonException(AnarchonErrorCode.Store, $"The NATS key \"{job}\" holds a value that is not the state of Anarchon runs");
    }

    /// <summary>The run of a slot; null when nobody claimed it, or it was trimmed.</summary>
    public StoredRun? Find(long slot) =>
        RunsObject()[slot.ToString(CultureInfo.InvariantCulture)] is JsonObject run ? new StoredRun(run) : null;

    /// <summary>A copy with one run set.</summary>
    public JobRuns With(long slot, StoredRun run)
    {
        var copy = (JsonObject)_root.DeepClone();
        ((JsonObject)copy["runs"]!)[slot.ToString(CultureInfo.InvariantCulture)] = run.ToNode();
        return new JobRuns(copy);
    }

    /// <summary>A copy that holds these runs only.</summary>
    public JobRuns WithRuns(IEnumerable<(long Slot, StoredRun Run)> runs)
    {
        var copy = (JsonObject)_root.DeepClone();
        var replaced = new JsonObject();
        foreach (var (slot, run) in runs)
        {
            replaced[slot.ToString(CultureInfo.InvariantCulture)] = run.ToNode();
        }

        copy["runs"] = replaced;
        return new JobRuns(copy);
    }

    public byte[] ToBytes() => JsonSerializer.SerializeToUtf8Bytes(_root);

    private JsonObject RunsObject() => (JsonObject)_root["runs"]!;
}

/// <summary>
/// One run's state, with the field names of the Redis Lease strategy: <c>a</c> attempt, <c>m</c> the
/// attempt limit it started with, <c>r</c> hand-overs, <c>t</c> the current attempt's token, <c>s</c>
/// status, <c>i</c> the runner, <c>l</c> the lease end on the server's clock (0 once released or ended).
/// </summary>
internal sealed class StoredRun(JsonObject raw)
{
    public int Attempt => (int)Number("a");

    public int? MaxAttempts => raw["m"] is null ? null : (int)Number("m");

    public int Released => (int)Number("r");

    public string Token => Text("t");

    public string Status => Text("s");

    public long LeaseEnd => Number("l");

    public bool IsRunning => Status == "running";

    /// <summary>The first attempt of a run, with a lease from now.</summary>
    public static StoredRun First(Anarchon.Backends.RunClaim claim, long now) => new(new JsonObject
    {
        ["a"] = 1,
        ["m"] = claim.MaxAttempts,
        ["t"] = claim.Token,
        ["s"] = "running",
        ["i"] = claim.InstanceId,
        ["l"] = now + (long)claim.Lease.TotalMilliseconds,
    });

    /// <summary>A copy with these fields set; every other field is kept.</summary>
    public StoredRun With(params (string Name, JsonNode? Value)[] fields)
    {
        var copy = (JsonObject)raw.DeepClone();
        foreach (var (name, value) in fields)
        {
            copy[name] = value;
        }

        return new StoredRun(copy);
    }

    public JsonObject ToNode() => (JsonObject)raw.DeepClone();

    private long Number(string name)
    {
        if (raw[name] is not JsonValue value) return 0;
        if (value.TryGetValue<long>(out var whole)) return whole;
        return value.TryGetValue<double>(out var real) ? (long)real : 0;
    }

    private string Text(string name) => raw[name] is JsonValue value && value.TryGetValue<string>(out var text) ? text : "";
}
