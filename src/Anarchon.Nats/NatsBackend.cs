// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Text.RegularExpressions;
using Anarchon.Backends;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Anarchon.Nats;

/// <summary>
/// NATS JetStream, on NATS.Net. With the default <see cref="NatsStrategy.MsgId"/>, every instance
/// publishes its claim with the header <c>Nats-Msg-Id: &lt;job&gt;:&lt;slot&gt;</c>; JetStream stores the first
/// one and answers repeats with <c>duplicate</c>, so the publisher knows at once whether it won.
/// <see cref="NatsStrategy.Lease"/> adds backup runners, retries, catch-up and no overlapping runs.
/// </summary>
/// <remarks>
/// <para>
/// <b>How each run gets exactly one runner (MsgId).</b> JetStream's message de-duplication: each
/// instance publishes its claim to <c>&lt;prefix&gt;&lt;job&gt;.claims</c> with the same message ID; within the
/// stream's duplicate window the stream stores the first and acknowledges the rest as duplicates. The
/// stream (its leader, in a cluster) orders the publishes, so exactly one is first. No lease: no
/// backup runners, no overlap protection and no catch-up.
/// </para>
/// <para>
/// <b>Lease.</b> A key-value bucket <c>&lt;stream&gt;_RUNS</c> holds each job's runs, and a write succeeds
/// only at the revision it read; a refused write is decided again. Leases run on the NATS server's clock.
/// </para>
/// <para>
/// Job names may only use letters, digits, <c>_</c> and <c>-</c>, since they become subjects. History
/// (claims and results) is kept in the stream, by count only.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// var scheduler = new Scheduler(new SchedulerOptions { Backend = new NatsBackend("nats://nats.internal:4222") });
/// </code>
/// </example>
/// <example>Leases: backup runners, no overlapping runs, catch-up and retries
/// <code>
/// var backend = new NatsBackend(new NatsBackendOptions { ConnectionString = natsUrl, Strategy = NatsStrategy.Lease });
/// </code>
/// </example>
public sealed partial class NatsBackend : IBackend
{
    private const string DefaultConnectionString = "nats://localhost:4222";

    private readonly NatsBackendOptions _options;
    private readonly NatsLeaseStore? _lease;
    private readonly Lock _gate = new();
    private Task<JetStreamState>? _state;
    private int _historySize;
    private int _streamHistorySize;

    /// <summary>A backend on the MsgId strategy, connecting to a NATS URL, on the stream <c>CRON</c>.</summary>
    /// <param name="connectionString">A NATS URL, or several separated by commas.</param>
    /// <example>
    /// <code>
    /// new NatsBackend(builder.Configuration.GetConnectionString("nats")!);
    /// </code>
    /// </example>
    public NatsBackend(string connectionString)
        : this(new NatsBackendOptions { ConnectionString = connectionString })
    {
    }

    /// <summary>Checks the options. Nothing connects until the scheduler starts.</summary>
    /// <param name="options">Where NATS is, the strategy, the stream and its limits.</param>
    /// <exception cref="ArgumentException">
    /// Both a connection string and a connection are given, the strategy is unknown, or with Lease the
    /// stream name can't name a bucket.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">The duplicate window, history size or replicas are not positive.</exception>
    /// <example>
    /// <code>
    /// new NatsBackend(new NatsBackendOptions { Connection = client.Connection, Stream = "BILLING_CRON", Replicas = 3 });
    /// </code>
    /// </example>
    public NatsBackend(NatsBackendOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        RefuseInvalid(options);

        _options = Copy(options);
        _historySize = options.HistorySize;
        if (Strategy == NatsStrategy.Lease) _lease = new NatsLeaseStore(StateAsync, $"{options.Stream}_RUNS", options.Prefix, options.Replicas);
    }

    /// <summary><c>nats</c>, or <c>nats:lease</c> with the Lease strategy.</summary>
    public string Name => Strategy == NatsStrategy.Lease ? "nats:lease" : "nats";

    /// <summary>The strategy this backend claims runs with.</summary>
    public NatsStrategy Strategy => _options.Strategy;

    /// <summary>MsgId: the stream's duplicate window. Lease: forever, since a run is remembered while a late claim could still ask for it.</summary>
    public TimeSpan DedupeWindow => _lease is null ? _options.DuplicateWindow : TimeSpan.MaxValue;

    /// <summary>The run store, with the Lease strategy only.</summary>
    public IRunStore? Runs => _lease;

    /// <summary>
    /// Refuses job names that can't be part of a subject, and an age limit on history, which the stream
    /// keeps by count only; and makes the stream keep at least the job's history size.
    /// </summary>
    /// <param name="job">The job's settings.</param>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.Unsupported"/>: the name has other characters, or the job sets <c>HistoryTtl</c>.</exception>
    public void CheckJob(JobLimits job)
    {
        ArgumentNullException.ThrowIfNull(job);
        if (!SimpleName().IsMatch(job.Name))
        {
            throw new AnarchonException(AnarchonErrorCode.Unsupported, $"Job \"{job.Name}\": with NATS, job names may only contain letters, digits, \"_\" and \"-\"");
        }

        if (job.HistoryTtl is not null)
        {
            throw new AnarchonException(AnarchonErrorCode.Unsupported, $"Job \"{job.Name}\": HistoryTtl is not supported by {Name}, which keeps history by count (HistorySize) only");
        }

        lock (_gate)
        {
            _historySize = Math.Max(_historySize, job.HistorySize);
        }
    }

    /// <summary>
    /// Connects, and creates the stream or brings its duplicate window and limits up to date. With
    /// Lease, also opens the bucket of runs and checks the server reports its clock (2.10+).
    /// </summary>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the backend is ready.</returns>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.Server"/>: JetStream is off, or the server is too old for Lease.</exception>
    public async Task InitAsync(CancellationToken cancellationToken)
    {
        await StateAsync(cancellationToken).ConfigureAwait(false);
        if (_lease is not null) await _lease.InitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>All jobs share the stream: makes it keep at least this job's history size per job.</summary>
    /// <param name="job">Not used.</param>
    /// <param name="historySize">About how many runs the job keeps.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the stream keeps enough.</returns>
    public async Task PrepareJobAsync(string job, int historySize, CancellationToken cancellationToken)
    {
        bool grown;
        lock (_gate)
        {
            _historySize = Math.Max(_historySize, historySize);
            grown = _streamHistorySize < _historySize;
        }

        if (!grown) return;
        var (_, js) = await StateAsync(cancellationToken).ConfigureAwait(false);
        await EnsureStreamAsync(js, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// MsgId: publishes the claim with the message ID <c>&lt;job&gt;:&lt;slot&gt;</c>. Won unless the
    /// acknowledgement says duplicate; a duplicate carrying this claim's token is its own retry.
    /// Lease: the plain one-winner claim, with a long lease and no backups.
    /// </summary>
    /// <param name="input">The run and who claims it.</param>
    /// <param name="cancellationToken">Stops waiting; the claim may still reach the server.</param>
    /// <returns>Whether this claim won the run.</returns>
    public async Task<bool> ClaimAsync(ClaimInput input, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        if (_lease is not null) return await _lease.ClaimWithoutBackupsAsync(input, cancellationToken).ConfigureAwait(false);

        var (_, js) = await StateAsync(cancellationToken).ConfigureAwait(false);
        var options = new NatsJSPubOpts { MsgId = $"{input.Job}:{input.Slot}" };
        var ack = await PublishAsync(js, Subject(input.Job, "claims"), StoredRecord.Claim(input, manual: false), options, cancellationToken).ConfigureAwait(false);
        if (!ack.Duplicate) return true;

        // A duplicate: normally another instance won. It is ours when the client sent the publish twice.
        return await IsOwnClaimAsync(js, ack.Seq, input.Token, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Records a manual run. Lease: claims a run of its own at the first free millisecond and returns
    /// that slot. MsgId: publishes the claim without a message ID, so it is never deduplicated.
    /// </summary>
    /// <param name="input">The manual run.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The slot used with Lease; null otherwise.</returns>
    public async Task<long?> RecordManualRunAsync(ClaimInput input, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        if (_lease is not null) return await _lease.ClaimManualRunAsync(input, cancellationToken).ConfigureAwait(false);

        var (_, js) = await StateAsync(cancellationToken).ConfigureAwait(false);
        await PublishAsync(js, Subject(input.Job, "claims"), StoredRecord.Claim(input, manual: true), null, cancellationToken).ConfigureAwait(false);
        return null;
    }

    /// <summary>Publishes the result to the job's results subject.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="result">How the attempt ended.</param>
    /// <param name="historySize">Not used: the stream keeps a count per subject.</param>
    /// <param name="historyTtl">Not used: refused by <see cref="CheckJob"/>.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the stream stored the result.</returns>
    public async Task SaveResultAsync(string job, StoredResult result, int historySize, TimeSpan? historyTtl, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(result);
        var (_, js) = await StateAsync(cancellationToken).ConfigureAwait(false);
        await PublishAsync(js, Subject(job, "results"), StoredRecord.Result(result), null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads the job's claims and results from the stream and joins them. With Lease, an attempt
    /// without a result is Running only while it is the current attempt with a live lease.
    /// </summary>
    /// <param name="job">The job's name.</param>
    /// <param name="limit">How many attempts to return at most.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The attempts, newest first; none for a job that never ran.</returns>
    public async Task<IReadOnlyList<RunRecord>> HistoryAsync(string job, int limit, CancellationToken cancellationToken)
    {
        var (_, js) = await StateAsync(cancellationToken).ConfigureAwait(false);
        var filter = $"{_options.Prefix}{job}.*";
        var count = await StreamHistory.CountAsync(js, _options.Stream, filter, cancellationToken).ConfigureAwait(false);
        if (count == 0) return [];

        var records = await StreamHistory.ReadAsync(js, _options.Stream, filter, count, cancellationToken).ConfigureAwait(false);
        var claims = records.Select(record => record.ToClaim()).OfType<ClaimRecord>();
        var results = records.Select(record => record.ToResult()).OfType<StoredResult>();
        var pending = _lease is null ? null : await _lease.PendingStatusAsync(job, cancellationToken).ConfigureAwait(false);
        return RunHistory.Build(job, claims, results, limit, pending);
    }

    /// <summary>Closes the connection if this backend opened it. A connection that was passed in stays open.</summary>
    /// <returns>A task that completes once the connection is closed.</returns>
    public async Task CloseAsync()
    {
        Task<JetStreamState>? state;
        lock (_gate)
        {
            (state, _state, _streamHistorySize) = (_state, null, 0);
        }

        _lease?.Close();
        if (state is null || _options.Connection is not null) return;

        try
        {
            var (connection, _) = await state.ConfigureAwait(false);
            await connection.DisposeAsync().ConfigureAwait(false);
        }
        catch (NatsException)
        {
            // It never connected: nothing to close.
        }
    }

    /// <summary>The connection and JetStream, opened once; a failed start is tried again by the next call.</summary>
    private Task<JetStreamState> StateAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (_state is null || _state.IsFaulted || _state.IsCanceled) _state = Task.Run(() => ConnectAsync(CancellationToken.None), CancellationToken.None);
            return _state.WaitAsync(cancellationToken);
        }
    }

    /// <summary>Connects (unless a connection was given), checks JetStream, and sets the stream up.</summary>
    private async Task<JetStreamState> ConnectAsync(CancellationToken cancellationToken)
    {
        var connection = _options.Connection ?? new NatsConnection(new NatsOpts
        {
            Url = _options.ConnectionString ?? DefaultConnectionString,
            Name = $"anarchon:{_options.Prefix}",
        });

        try
        {
            await connection.ConnectAsync().ConfigureAwait(false);
            var js = new NatsJSContext(connection);
            await RefuseWithoutJetStreamAsync(js, cancellationToken).ConfigureAwait(false);
            await EnsureStreamAsync(js, cancellationToken).ConfigureAwait(false);
            return new JetStreamState(connection, js);
        }
        catch (Exception) when (_options.Connection is null)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task EnsureStreamAsync(INatsJSContext js, CancellationToken cancellationToken)
    {
        int historySize;
        lock (_gate)
        {
            historySize = _historySize;
        }

        var settings = new StreamSettings(_options.Stream, $"{_options.Prefix}>", _options.DuplicateWindow, historySize, _options.Replicas);
        await JetStreamSetup.EnsureStreamAsync(js, settings, cancellationToken).ConfigureAwait(false);
        lock (_gate)
        {
            _streamHistorySize = Math.Max(_streamHistorySize, historySize);
        }
    }

    /// <summary>Whether the stored message at <paramref name="sequence"/> carries this claim's token.</summary>
    private async Task<bool> IsOwnClaimAsync(INatsJSContext js, ulong sequence, string token, CancellationToken cancellationToken)
    {
        try
        {
            var stream = await js.GetStreamAsync(_options.Stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            var stored = await stream.GetAsync(new StreamMsgGetRequest { Seq = sequence }, cancellationToken).ConfigureAwait(false);
            var record = StoredRecord.Parse(stored.Message.Data.Span);
            return record?.Token == token;
        }
        catch (NatsJSApiException)
        {
            return false; // the message is gone: another instance's claim, trimmed since
        }
    }

    private string Subject(string job, string kind) => $"{_options.Prefix}{job}.{kind}";

    private static async Task<PubAckResponse> PublishAsync(INatsJSContext js, string subject, byte[] record, NatsJSPubOpts? options, CancellationToken cancellationToken)
    {
        var ack = await js.PublishAsync(subject, record, NatsRawSerializer<byte[]>.Default, options, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (ack.Error is { } error) throw new NatsJSApiException(error);
        return ack;
    }

    private static async Task RefuseWithoutJetStreamAsync(NatsJSContext js, CancellationToken cancellationToken)
    {
        try
        {
            await js.GetAccountInfoAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (NatsException error)
        {
            throw new AnarchonException(AnarchonErrorCode.Server, $"The NATS backend needs JetStream enabled on the server (nats-server -js): {error.Message}", error);
        }
    }

    private static void RefuseInvalid(NatsBackendOptions options)
    {
        if (options.ConnectionString is not null && options.Connection is not null) throw new ArgumentException("Give NatsBackend a ConnectionString or a Connection, not both", nameof(options));
        if (!Enum.IsDefined(options.Strategy)) throw new ArgumentException($"Unknown strategy {options.Strategy}", nameof(options));
        if (options.DuplicateWindow < TimeSpan.FromMilliseconds(1)) throw new ArgumentOutOfRangeException(nameof(options), $"DuplicateWindow must be at least 1 ms (got {options.DuplicateWindow})");
        if (options.HistorySize < 1) throw new ArgumentOutOfRangeException(nameof(options), $"HistorySize must be at least 1 (got {options.HistorySize})");
        if (options.Replicas is < 1) throw new ArgumentOutOfRangeException(nameof(options), $"Replicas must be at least 1 (got {options.Replicas})");
        if (string.IsNullOrEmpty(options.Stream) || options.Prefix is null) throw new ArgumentException("Stream and Prefix must be set", nameof(options));

        if (options.Strategy == NatsStrategy.Lease && !SimpleName().IsMatch(options.Stream))
        {
            throw new ArgumentException($"With the Lease strategy, the stream name \"{options.Stream}\" may only contain letters, digits, \"_\" and \"-\" (it names the bucket \"<stream>_RUNS\")", nameof(options));
        }
    }

    /// <summary>A copy, so a later change to the caller's options object changes nothing here.</summary>
    private static NatsBackendOptions Copy(NatsBackendOptions options) => new()
    {
        Strategy = options.Strategy,
        ConnectionString = options.ConnectionString,
        Connection = options.Connection,
        Stream = options.Stream,
        Prefix = options.Prefix,
        DuplicateWindow = options.DuplicateWindow,
        HistorySize = options.HistorySize,
        Replicas = options.Replicas,
    };

    [GeneratedRegex("^[A-Za-z0-9_-]+$")]
    private static partial Regex SimpleName();
}
