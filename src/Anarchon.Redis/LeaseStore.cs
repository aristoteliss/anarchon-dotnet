// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Globalization;
using System.Text.Json;
using Anarchon.Backends;
using StackExchange.Redis;

namespace Anarchon.Redis;

/// <summary>
/// The run store of the Lease strategy: claims, heartbeats and hand-overs as Lua scripts
/// (<see cref="LeaseScripts"/>). Per job, all under one hash tag so it also works on Redis Cluster:
/// <list type="bullet">
/// <item><c>{&lt;prefix&gt;&lt;job&gt;}:state</c>: hash, slot to the run's state as JSON</item>
/// <item><c>{&lt;prefix&gt;&lt;job&gt;}:active</c>: set of the slots still marked running</item>
/// <item><c>{&lt;prefix&gt;&lt;job&gt;}:slots</c>: sorted set of every slot in state, to trim the oldest</item>
/// <item><c>{&lt;prefix&gt;&lt;job&gt;}:runs</c>: stream, one entry per attempt (history)</item>
/// <item><c>{&lt;prefix&gt;&lt;job&gt;}:results</c>: sorted set, one result per attempt (history)</item>
/// </list>
/// </summary>
internal sealed class LeaseStore(Func<Task<IDatabase>> database, string prefix) : IRunStore
{
    /// <summary>A result's score is when it ended; this fraction per attempt orders attempts that end in the same millisecond.</summary>
    private const double AttemptsPerMs = 100;

    private const int MaxOrderedAttempts = 99;

    public async Task<ClaimOutcome> ClaimAsync(RunClaim claim, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(claim);
        var db = await database().ConfigureAwait(false);
        RedisKey[] keys = [Key(claim.Job, "state"), Key(claim.Job, "active"), Key(claim.Job, "slots"), Key(claim.Job, "runs")];

        var reply = await db.ScriptEvaluateAsync(LeaseScripts.Claim, keys, ClaimArgs(claim)).ConfigureAwait(false);
        return ToOutcome(reply);
    }

    public async Task<bool> HeartbeatAsync(string job, long slot, string token, TimeSpan lease, CancellationToken cancellationToken)
    {
        var db = await database().ConfigureAwait(false);
        var reply = await db.ScriptEvaluateAsync(LeaseScripts.Heartbeat, [Key(job, "state")], [slot, token, (long)lease.TotalMilliseconds]).ConfigureAwait(false);
        return (long)reply == 1;
    }

    public async Task<bool> FinishAsync(string job, long slot, string token, RunStatus status, CancellationToken cancellationToken)
    {
        var db = await database().ConfigureAwait(false);
        RedisKey[] keys = [Key(job, "state"), Key(job, "active")];
        var reply = await db.ScriptEvaluateAsync(LeaseScripts.Finish, keys, [slot, token, RunText.ToText(status)]).ConfigureAwait(false);
        return (long)reply == 1;
    }

    public async Task<bool> ReleaseAsync(string job, long slot, string token, CancellationToken cancellationToken)
    {
        var db = await database().ConfigureAwait(false);
        var reply = await db.ScriptEvaluateAsync(LeaseScripts.Release, [Key(job, "state")], [slot, token]).ConfigureAwait(false);
        return (long)reply == 1;
    }

    public async Task<IReadOnlyList<ActiveRun>> ActiveAsync(string job, CancellationToken cancellationToken)
    {
        var db = await database().ConfigureAwait(false);
        var reply = await db.ScriptEvaluateAsync(LeaseScripts.Active, [Key(job, "state"), Key(job, "active")]).ConfigureAwait(false);

        var rows = (RedisResult[]?)reply ?? [];
        return [.. rows.Select(ToActiveRun)];
    }

    /// <summary>Adds the attempt's result to the results sorted set and trims it by count and age.</summary>
    public async Task SaveResultAsync(string job, StoredResult result, int historySize, TimeSpan? historyTtl)
    {
        var db = await database().ConfigureAwait(false);
        var key = Key(job, "results");

        // Scored by when the attempt ended, so the age limit keeps a catch-up of an old slot.
        var ended = Math.Max(result.Slot, result.FinishedAt.ToUnixTimeMilliseconds());
        var score = ended + (Math.Min(result.Attempt, MaxOrderedAttempts) / AttemptsPerMs);
        await db.SortedSetAddAsync(key, StoredData.ToJson(result), score).ConfigureAwait(false);

        await db.SortedSetRemoveRangeByRankAsync(key, 0, -((historySize * 2L) + 1)).ConfigureAwait(false);
        if (historyTtl is { } ttl)
        {
            var oldest = result.FinishedAt.ToUnixTimeMilliseconds() - (long)ttl.TotalMilliseconds;
            await db.SortedSetRemoveRangeByScoreAsync(key, double.NegativeInfinity, oldest, Exclude.Stop).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Joins the attempts in the stream with their results. An attempt without a result is Running
    /// only while it is the current attempt with a live lease; otherwise Interrupted.
    /// </summary>
    public async Task<IReadOnlyList<RunRecord>> HistoryAsync(string job, int limit)
    {
        var db = await database().ConfigureAwait(false);
        var entries = await db.StreamRangeAsync(Key(job, "runs"), null, null, limit, Order.Descending).ConfigureAwait(false);
        var claims = entries.Select(StoredData.ToClaim).OfType<ClaimRecord>().ToList();
        if (claims.Count == 0) return [];

        var oldest = claims.Min(claim => claim.Slot);
        var members = await db.SortedSetRangeByScoreAsync(Key(job, "results"), oldest).ConfigureAwait(false);
        var (now, states) = await ReadStatesAsync(db, job, claims).ConfigureAwait(false);

        return RunHistory.Build(job, claims, StoredData.ToResults(members), limit, claim => PendingStatus(states.GetValueOrDefault(claim.Slot), claim.Attempt, now));
    }

    /// <summary>A key of one job. All share the hash tag <c>{&lt;prefix&gt;&lt;job&gt;}</c>, so Redis Cluster keeps them on one node.</summary>
    private RedisKey Key(string job, string part) => $"{{{prefix}{job}}}:{part}";

    /// <summary>The stored state of each claimed slot, read in one script call, with the server's clock.</summary>
    private async Task<(long Now, Dictionary<long, RunStateJson?> States)> ReadStatesAsync(IDatabase db, string job, List<ClaimRecord> claims)
    {
        var slots = claims.Select(claim => claim.Slot).Distinct().ToList();
        var reply = await db.ScriptEvaluateAsync(LeaseScripts.States, [Key(job, "state")], [.. slots.Select(slot => (RedisValue)slot)]).ConfigureAwait(false);

        var parts = (RedisResult[]?)reply ?? [];
        var now = parts.Length > 0 ? (long)parts[0] : 0;
        var states = slots.Select((slot, i) => (slot, state: ToState(parts.ElementAtOrDefault(i + 1))));
        return (now, states.ToDictionary(pair => pair.slot, pair => pair.state));
    }

    /// <summary>The CLAIM script's ARGV, in the order the script reads it.</summary>
    private static RedisValue[] ClaimArgs(RunClaim claim) =>
    [
        claim.Slot,
        claim.Token,
        claim.InstanceId,
        (long)claim.Lease.TotalMilliseconds,
        claim.Overlap == OverlapPolicy.Allow ? "allow" : "skip",
        claim.MaxAttempts,
        claim.HistorySize,
        claim.Mode == ClaimMode.Takeover ? "takeover" : "new",
        RunText.ToText(claim.Reason),
        (long)(claim.HistoryTtl?.TotalMilliseconds ?? 0),
        (long)(claim.MaxLate?.TotalMilliseconds ?? 0),
        (long)(claim.Keep?.TotalMilliseconds ?? 0),
    ];

    /// <summary>The CLAIM script's <c>{kind, value}</c> reply; a kind this version doesn't know reads as done.</summary>
    private static ClaimOutcome ToOutcome(RedisResult reply)
    {
        var parts = (RedisResult[]?)reply ?? [];
        var kind = parts.Length > 0 ? parts[0].ToString() : "";
        var value = parts.Length > 1 ? long.Parse(parts[1].ToString(), CultureInfo.InvariantCulture) : 0;

        return kind switch
        {
            "won" => new ClaimOutcome.Won((int)value),
            "running" => new ClaimOutcome.Running((int)value),
            "overlap" => new ClaimOutcome.Overlap(value),
            "none" => new ClaimOutcome.None(),
            "late" => new ClaimOutcome.Late(TimeSpan.FromMilliseconds(value)),
            _ => new ClaimOutcome.Done(),
        };
    }

    private static ActiveRun ToActiveRun(RedisResult row)
    {
        var parts = (RedisResult[]?)row ?? [];
        var slot = long.Parse(parts[0].ToString(), CultureInfo.InvariantCulture);
        var attempt = int.Parse(parts[1].ToString(), CultureInfo.InvariantCulture);
        return new ActiveRun(slot, attempt, parts[2].ToString() == "1");
    }

    private static RunStateJson? ToState(RedisResult? raw)
    {
        var text = raw?.ToString();
        if (string.IsNullOrEmpty(text)) return null;

        try
        {
            return JsonSerializer.Deserialize(text, StoredJsonContext.Default.RunStateJson);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>An attempt without a result is Running only while it is the current attempt with a live lease.</summary>
    private static RunStatus PendingStatus(RunStateJson? run, int attempt, long now)
    {
        if (run is null || run.Attempt != attempt) return RunStatus.Interrupted;
        if (run.Status == "running") return run.LeaseEnd >= now ? RunStatus.Running : RunStatus.Interrupted;
        return RunText.ToStatus(run.Status) ?? RunStatus.Interrupted;
    }
}
