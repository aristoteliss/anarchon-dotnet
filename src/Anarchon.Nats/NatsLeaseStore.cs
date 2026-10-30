// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Globalization;
using Anarchon.Backends;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;

namespace Anarchon.Nats;

/// <summary>
/// The run store of the Lease strategy, on the JetStream key-value bucket <c>&lt;stream&gt;_RUNS</c>.
/// </summary>
/// <remarks>
/// Each job has one key, <c>&lt;job&gt;</c>, whose value holds the state of the job's runs. A write can
/// say "only if the key is still at revision N", and JetStream refuses it when anyone wrote in
/// between. Every decision is one such write: read the job's runs, their revision and the server's
/// clock; decide as the Redis CLAIM script does; write at the revision read. When another instance
/// wrote first, read again and decide again, so of all the instances that claim one attempt, exactly
/// one writes it. Heartbeat, release and finish are the same read, decide and write, and change the
/// run only while it carries the caller's token. History stays in the backend's stream.
/// </remarks>
internal sealed class NatsLeaseStore(Func<CancellationToken, Task<JetStreamState>> state, string bucket, string prefix, int? replicas) : IRunStore
{
    /// <summary>How many times in a row a write may lose its race to another instance before the call fails.</summary>
    private const int WriteTries = 20;

    /// <summary>The key that records the store format. Job names have no dots, so it can't be a job's key.</summary>
    private const string FormatKey = "meta.format";

    private readonly Lock _gate = new();
    private Task<INatsKVStore>? _store;

    /// <summary>Opens or creates the bucket, checks the server reports its clock, and checks the store format.</summary>
    public async Task InitAsync(CancellationToken cancellationToken)
    {
        var store = await StoreAsync(cancellationToken).ConfigureAwait(false);
        await ServerNowAsync(cancellationToken).ConfigureAwait(false);

        var format = System.Text.Encoding.UTF8.GetBytes(StoreFormat.Current.ToString(CultureInfo.InvariantCulture));
        // A create that lost to another instance's create is fine: the key holds what both wrote.
        await store.TryCreateAsync(FormatKey, format, NatsRawSerializer<byte[]>.Default, cancellationToken).ConfigureAwait(false);
        var stored = await store.TryGetEntryAsync(FormatKey, serializer: NatsRawSerializer<byte[]>.Default, cancellationToken: cancellationToken).ConfigureAwait(false);
        var text = stored.Success && stored.Value.Value is { } bytes ? System.Text.Encoding.UTF8.GetString(bytes) : null;
        StoreFormat.Check(text, $"The NATS bucket \"{bucket}\"");
    }

    public async Task<ClaimOutcome> ClaimAsync(RunClaim claim, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(claim);

        for (var tries = 0; tries < WriteTries; tries++)
        {
            var (runs, revision, now) = await LoadAsync(claim.Job, cancellationToken).ConfigureAwait(false);
            var step = LeaseRules.Decide(runs, claim, now);
            if (step is not LeaseStep.Write write) return step.Outcome;

            // Another instance wrote first: decide again.
            if (!await SaveAsync(claim.Job, write.Runs, revision, cancellationToken).ConfigureAwait(false)) continue;

            // The attempt is stored: if recording it in history fails, the claim fails, nobody renews
            // the lease, and a backup takes the run over once it runs out.
            if (step.Outcome is ClaimOutcome.Won won) await RecordAttemptAsync(claim, won.Attempt, now, cancellationToken).ConfigureAwait(false);
            return step.Outcome;
        }

        throw LostRaces(claim.Job);
    }

    public Task<bool> HeartbeatAsync(string job, long slot, string token, TimeSpan lease, CancellationToken cancellationToken) =>
        ChangeAsync(job, slot, (run, now) => LeaseRules.Renew(run, token, lease, now), cancellationToken);

    public Task<bool> FinishAsync(string job, long slot, string token, RunStatus status, CancellationToken cancellationToken) =>
        ChangeAsync(job, slot, (run, _) => LeaseRules.End(run, token, status), cancellationToken);

    public Task<bool> ReleaseAsync(string job, long slot, string token, CancellationToken cancellationToken) =>
        ChangeAsync(job, slot, (run, now) => LeaseRules.Release(run, token, now), cancellationToken);

    public async Task<IReadOnlyList<ActiveRun>> ActiveAsync(string job, CancellationToken cancellationToken)
    {
        var (runs, _, now) = await LoadAsync(job, cancellationToken).ConfigureAwait(false);

        return [.. runs.Runs
            .Where(pair => pair.Run.IsRunning)
            .OrderBy(pair => pair.Slot)
            .Select(pair => new ActiveRun(pair.Slot, pair.Run.Attempt, pair.Run.LeaseEnd >= now))];
    }

    /// <summary>For history: the status of an attempt without a result, decided on the server's clock.</summary>
    public async Task<Func<ClaimRecord, RunStatus>> PendingStatusAsync(string job, CancellationToken cancellationToken)
    {
        var (runs, _, now) = await LoadAsync(job, cancellationToken).ConfigureAwait(false);
        return claim => LeaseRules.PendingStatus(runs.Find(claim.Slot), claim.Attempt, now);
    }

    /// <summary>Forgets the bucket handle: the backend closed the connection it belongs to.</summary>
    public void Close()
    {
        lock (_gate)
        {
            _store = null;
        }
    }

    /// <summary>Rewrites one run with what <paramref name="next"/> returns; false when the run is gone or <paramref name="next"/> refuses.</summary>
    private async Task<bool> ChangeAsync(string job, long slot, Func<StoredRun, long, StoredRun?> next, CancellationToken cancellationToken)
    {
        for (var tries = 0; tries < WriteTries; tries++)
        {
            var (runs, revision, now) = await LoadAsync(job, cancellationToken).ConfigureAwait(false);
            var changed = runs.Find(slot) is { } run ? next(run, now) : null;
            if (changed is null) return false;

            if (await SaveAsync(job, runs.With(slot, changed), revision, cancellationToken).ConfigureAwait(false)) return true;
        }

        throw LostRaces(job);
    }

    /// <summary>The job's runs and their revision (0: no key yet), read together with the server's clock.</summary>
    private async Task<(JobRuns Runs, ulong Revision, long Now)> LoadAsync(string job, CancellationToken cancellationToken)
    {
        var store = await StoreAsync(cancellationToken).ConfigureAwait(false);
        var entry = store.TryGetEntryAsync(job, serializer: NatsRawSerializer<byte[]>.Default, cancellationToken: cancellationToken).AsTask();
        var now = ServerNowAsync(cancellationToken);
        await Task.WhenAll(entry, now).ConfigureAwait(false);

        var found = await entry.ConfigureAwait(false);
        var clock = await now.ConfigureAwait(false);
        if (!found.Success || found.Value is not { Operation: NatsKVOperation.Put, Value: { } value } stored) return (JobRuns.Empty, 0, clock);
        return (JobRuns.Parse(job, value), stored.Revision, clock);
    }

    /// <summary>Writes the job's runs if nobody wrote since <paramref name="revision"/>; false when another instance did.</summary>
    private async Task<bool> SaveAsync(string job, JobRuns runs, ulong revision, CancellationToken cancellationToken)
    {
        var store = await StoreAsync(cancellationToken).ConfigureAwait(false);
        var value = runs.ToBytes();
        var written = revision == 0
            ? await store.TryCreateAsync(job, value, NatsRawSerializer<byte[]>.Default, cancellationToken).ConfigureAwait(false)
            : await store.TryUpdateAsync(job, value, revision, NatsRawSerializer<byte[]>.Default, cancellationToken).ConfigureAwait(false);
        return Saved(written);
    }

    /// <summary>Appends the won attempt to the job's history in the backend's stream.</summary>
    private async Task RecordAttemptAsync(RunClaim claim, int attempt, long now, CancellationToken cancellationToken)
    {
        var (_, js) = await state(cancellationToken).ConfigureAwait(false);
        // The ID makes a publish the client sends twice one record.
        var options = new NatsJSPubOpts { MsgId = $"{claim.Job}:{claim.Slot}:{attempt}" };
        var ack = await js.PublishAsync($"{prefix}{claim.Job}.claims", StoredRecord.WonAttempt(claim, attempt, now), NatsRawSerializer<byte[]>.Default, options, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (ack.Error is { } error) throw new NatsJSApiException(error);
    }

    /// <summary>The NATS server's clock in Unix ms, from the bucket's stream info: leases are decided on it, never on an instance's clock.</summary>
    private async Task<long> ServerNowAsync(CancellationToken cancellationToken)
    {
        var (_, js) = await state(cancellationToken).ConfigureAwait(false);
        var stream = await js.GetStreamAsync($"KV_{bucket}", cancellationToken: cancellationToken).ConfigureAwait(false);
        if (stream.Info.Ts is { } ts) return ts.ToUnixTimeMilliseconds();

        throw new AnarchonException(AnarchonErrorCode.Server, "The NATS Lease strategy needs nats-server 2.10 or newer, which reports its clock");
    }

    /// <summary>The bucket, opened once; a failed open is tried again by the next call.</summary>
    private Task<INatsKVStore> StoreAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (_store is null || _store.IsFaulted || _store.IsCanceled) _store = Task.Run(() => OpenBucketAsync(CancellationToken.None), CancellationToken.None);
            return _store.WaitAsync(cancellationToken);
        }
    }

    /// <summary>Opens the bucket, creating it on first use. An existing bucket keeps its own settings.</summary>
    private async Task<INatsKVStore> OpenBucketAsync(CancellationToken cancellationToken)
    {
        var (_, js) = await state(cancellationToken).ConfigureAwait(false);
        var kv = new NatsKVContext(js);
        try
        {
            return await kv.GetStoreAsync(bucket, cancellationToken).ConfigureAwait(false);
        }
        catch (NatsJSApiException error) when (error.Error.Code == 404)
        {
            // One value per key: a job's runs are rewritten in place, and older revisions serve nobody.
            var config = new NatsKVConfig(bucket) { History = 1, NumberOfReplicas = replicas ?? 1 };
            return await kv.CreateStoreAsync(config, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>true when the write landed; false when another instance wrote the key first; throws on any other failure.</summary>
    private static bool Saved(NatsResult<ulong> written)
    {
        if (written.Success) return true;
        if (written.Error is NatsKVCreateException or NatsKVWrongLastRevisionException) return false;
        throw written.Error;
    }

    private static AnarchonException LostRaces(string job) =>
        new(AnarchonErrorCode.Store, $"Job \"{job}\": other instances wrote its state {WriteTries} times in a row while this one tried to; try again");
}

/// <summary>A connection and its JetStream context, opened together.</summary>
internal sealed record JetStreamState(INatsConnection Connection, INatsJSContext Js);
