// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Text;
using System.Text.RegularExpressions;
using Anarchon.Backends;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Anarchon.RabbitMQ;

/// <summary>
/// RabbitMQ 3.9+, on RabbitMQ.Client. Each run has a gate queue
/// <c>&lt;prefix&gt;&lt;job&gt;.slot.&lt;slot&gt;</c> that holds at most one message; every instance publishes
/// its claim there with publisher confirms, and RabbitMQ accepts the first one and refuses the rest.
/// History is kept in a stream queue <c>&lt;prefix&gt;&lt;job&gt;.history</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>How each run gets exactly one runner.</b> A bounded queue that refuses overflow, together with
/// publisher confirms:
/// </para>
/// <list type="number">
/// <item>Each instance declares the run's gate queue (<c>x-max-length: 1</c>,
/// <c>x-overflow: reject-publish</c>; the same arguments everywhere, so declaring is safe to repeat)
/// and publishes one message to it on a confirm channel.</item>
/// <item>The queue holds one message. RabbitMQ confirms (ack) the publish that filled it and refuses
/// (nack) every later one. The broker decides the order, so exactly one instance gets the ack.</item>
/// <item>Nobody consumes the gate queue: the message stays, so the run stays taken until the queue
/// expires <see cref="RabbitMQBackendOptions.SlotTtl"/> after it was last used.</item>
/// </list>
/// <para>
/// Claim only: this decides who runs each time. There are no leases, so no backup runners, no
/// overlap protection and no catch-up; for those use Redis, PostgreSQL or SQLite. Job names may only
/// use letters, digits, <c>_</c> and <c>-</c>, since they become queue names. History is kept by
/// count only. A claim whose answer is lost (the connection dropped mid-publish) fails, so that
/// instance skips the run rather than risk running it twice.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// var scheduler = new Scheduler(new SchedulerOptions { Backend = new RabbitMQBackend("amqp://rabbit.internal") });
/// </code>
/// </example>
public sealed partial class RabbitMQBackend : IBackend
{
    private const string DefaultConnectionString = "amqp://localhost";

    /// <summary>A claim and a result, with room to spare: sizes the history stream.</summary>
    private const int BytesPerRun = 1_024;

    private const long MinHistoryBytes = 100_000;
    private const long MinSegmentBytes = 50_000;
    private const long MaxSegmentBytes = 1_000_000;

    /// <summary>The claims this instance won, remembered so a claim sent again is still a win.</summary>
    private const int RememberedWins = 1_000;

    private const int MaxPrefetch = 2_000;

    /// <summary>3.9: the first version with stream queues, which hold the history.</summary>
    private static readonly Version MinVersion = new(3, 9);

    private readonly string _prefix;
    private readonly int _slotTtlMs;
    private readonly BrokerConnection _broker;
    private readonly Lock _gate = new();
    private readonly OrderedDictionary<string, string> _won = [];

    /// <summary>A backend that connects with an AMQP URI, with the queue prefix <c>cron.</c>.</summary>
    /// <param name="connectionString">An AMQP URI, such as <c>amqp://rabbit.internal</c>.</param>
    /// <exception cref="ArgumentException">The connection string is not an <c>amqp://</c> or <c>amqps://</c> URI.</exception>
    /// <example>
    /// <code>
    /// new RabbitMQBackend(builder.Configuration.GetConnectionString("rabbitmq")!);
    /// </code>
    /// </example>
    public RabbitMQBackend(string connectionString)
        : this(new RabbitMQBackendOptions { ConnectionString = connectionString })
    {
    }

    /// <summary>Checks the options. Nothing connects until the scheduler starts.</summary>
    /// <param name="options">Where RabbitMQ is, the queue prefix and how long runs are remembered.</param>
    /// <exception cref="ArgumentException">Both a connection string and a connection are given, or the connection string is not an AMQP URI.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="RabbitMQBackendOptions.SlotTtl"/> is not from 1 ms to about 24 days.</exception>
    /// <example>
    /// <code>
    /// new RabbitMQBackend(new RabbitMQBackendOptions { Connection = connection, Prefix = "billing.cron.", SlotTtl = TimeSpan.FromMinutes(15) });
    /// </code>
    /// </example>
    public RabbitMQBackend(RabbitMQBackendOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.ConnectionString is not null && options.Connection is not null)
        {
            throw new ArgumentException("Give RabbitMQBackend a ConnectionString or a Connection, not both", nameof(options));
        }

        if (options.SlotTtl < TimeSpan.FromMilliseconds(1) || options.SlotTtl.TotalMilliseconds > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(options), $"SlotTtl must be from 1 ms to {int.MaxValue} ms (got {options.SlotTtl})");
        }

        _prefix = options.Prefix ?? throw new ArgumentException("Prefix must not be null", nameof(options));
        _slotTtlMs = (int)options.SlotTtl.TotalMilliseconds;
        var factory = options.Connection is null ? Factory(options.ConnectionString ?? DefaultConnectionString, _prefix, nameof(options)) : null;
        _broker = new BrokerConnection(factory, options.Connection);
    }

    /// <summary><c>rabbitmq</c>.</summary>
    public string Name => "rabbitmq";

    /// <summary><see cref="RabbitMQBackendOptions.SlotTtl"/>: a gate queue, and with it the memory of a claimed run, expires this long after its last use.</summary>
    public TimeSpan DedupeWindow => TimeSpan.FromMilliseconds(_slotTtlMs);

    /// <summary>Refuses job names that can't be part of a queue name, and an age limit on history, which RabbitMQ keeps by count only.</summary>
    /// <param name="job">The job's settings.</param>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.Unsupported"/>: the name has other characters, or the job sets <c>HistoryTtl</c>.</exception>
    public void CheckJob(JobLimits job)
    {
        ArgumentNullException.ThrowIfNull(job);
        if (!SimpleName().IsMatch(job.Name))
        {
            throw new AnarchonException(AnarchonErrorCode.Unsupported, $"Job \"{job.Name}\": with RabbitMQ, job names may only contain letters, digits, \"_\" and \"-\"");
        }

        if (job.HistoryTtl is not null)
        {
            throw new AnarchonException(AnarchonErrorCode.Unsupported, $"Job \"{job.Name}\": HistoryTtl is not supported by {Name}, which keeps history by count (HistorySize) only");
        }
    }

    /// <summary>Connects and refuses a server older than RabbitMQ 3.9, the first with stream queues.</summary>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the backend is ready.</returns>
    /// <exception cref="AnarchonException"><see cref="AnarchonErrorCode.Server"/>: the server is older than 3.9.</exception>
    public async Task InitAsync(CancellationToken cancellationToken)
    {
        var connection = await _broker.ConnectionAsync(cancellationToken).ConfigureAwait(false);
        var product = ServerProperty(connection, "product") ?? "RabbitMQ";
        var version = ServerProperty(connection, "version") ?? "0";
        if (Version.TryParse(LeadingVersion().Match(version).Value, out var parsed) && parsed >= MinVersion) return;

        throw new AnarchonException(AnarchonErrorCode.Server, $"The RabbitMQ backend needs RabbitMQ 3.9 or newer for stream queues (this server is {product} {version})");
    }

    /// <summary>Declares the job's history stream queue, sized from <paramref name="historySize"/>.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="historySize">About how many runs the job keeps.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the stream exists.</returns>
    public async Task PrepareJobAsync(string job, int historySize, CancellationToken cancellationToken)
    {
        var bytes = Math.Max((long)historySize * BytesPerRun, MinHistoryBytes);
        // A tenth of the stream per segment, within RabbitMQ's sensible bounds.
        var segmentBytes = Math.Clamp(bytes / 10, MinSegmentBytes, MaxSegmentBytes);
        var arguments = new Dictionary<string, object?>
        {
            ["x-queue-type"] = "stream",
            ["x-max-length-bytes"] = bytes,
            ["x-stream-max-segment-size-bytes"] = segmentBytes,
        };

        await _broker.DeclareAsync(HistoryQueue(job), arguments, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Declares the run's gate queue and publishes the claim to it: won on ack (the queue was empty), lost on nack (it was full).</summary>
    /// <param name="input">The run and who claims it.</param>
    /// <param name="cancellationToken">Stops waiting; the claim may still reach the broker.</param>
    /// <returns>Whether this claim won the run.</returns>
    public async Task<bool> ClaimAsync(ClaimInput input, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        var queue = SlotQueue(input.Job, input.Slot);
        // The same claim sent again is still ours. It is remembered here: reading the gate's message
        // would take it off the queue for a moment, and another instance's claim could fill it then.
        if (IsWonHere(queue, input.Token)) return true;

        var gate = new Dictionary<string, object?>
        {
            ["x-queue-type"] = "classic",
            ["x-max-length"] = 1,
            ["x-overflow"] = "reject-publish",
            ["x-expires"] = _slotTtlMs,
        };
        await _broker.DeclareAsync(queue, gate, cancellationToken).ConfigureAwait(false);

        var won = await _broker.PublishAsync(queue, StoredRecord.Claim(input, manual: false), cancellationToken).ConfigureAwait(false);
        if (won) RememberWin(queue, input.Token);
        return won;
    }

    /// <summary>Appends the won claim to the history stream; the gate queue only decides the winner.</summary>
    /// <param name="input">The claim that won.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the broker stored the record.</returns>
    public Task RecordClaimAsync(ClaimInput input, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        return AppendAsync(input.Job, StoredRecord.Claim(input, manual: false), cancellationToken);
    }

    /// <summary>Appends a manual run to the history stream. No gate: manual runs are not deduplicated.</summary>
    /// <param name="input">The manual run.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>null: the run keeps its slot.</returns>
    public async Task<long?> RecordManualRunAsync(ClaimInput input, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        await AppendAsync(input.Job, StoredRecord.Claim(input, manual: true), cancellationToken).ConfigureAwait(false);
        return null;
    }

    /// <summary>Appends the result to the history stream, which keeps a size fixed when the job was prepared.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="result">How the attempt ended.</param>
    /// <param name="historySize">Not used: the stream is sized on <see cref="PrepareJobAsync"/>.</param>
    /// <param name="historyTtl">Not used: refused by <see cref="CheckJob"/>.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the broker stored the record.</returns>
    public Task SaveResultAsync(string job, StoredResult result, int historySize, TimeSpan? historyTtl, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(result);
        return AppendAsync(job, StoredRecord.Result(result), cancellationToken);
    }

    /// <summary>Reads the newest records of the job's history stream and joins claims with results.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="limit">How many attempts to return at most.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The attempts, newest first; none for a job that never ran.</returns>
    public async Task<IReadOnlyList<RunRecord>> HistoryAsync(string job, int limit, CancellationToken cancellationToken)
    {
        var connection = await _broker.ConnectionAsync(cancellationToken).ConfigureAwait(false);
        var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        await using (channel.ConfigureAwait(false))
        {
            try
            {
                return await ReadHistoryAsync(channel, job, limit, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationInterruptedException error) when (error.ShutdownReason?.ReplyCode == Constants.NotFound)
            {
                return []; // the job's stream doesn't exist: it never ran
            }
        }
    }

    /// <summary>Closes the confirm channel, and the connection if this backend opened it. A connection that was passed in stays open.</summary>
    /// <returns>A task that completes once they are closed.</returns>
    public Task CloseAsync() => _broker.CloseAsync();

    private async Task<IReadOnlyList<RunRecord>> ReadHistoryAsync(IChannel channel, string job, int limit, CancellationToken cancellationToken)
    {
        // Each run is a claim and a result: read twice the limit, and a few more for manual runs.
        var wanted = (2L * limit) + 20;
        await channel.BasicQosAsync(0, (ushort)Math.Min(wanted + 30, MaxPrefetch), global: false, cancellationToken).ConfigureAwait(false);
        var queue = HistoryQueue(job);
        var end = await HistoryStream.LastOffsetAsync(channel, queue, cancellationToken).ConfigureAwait(false);
        if (end is null) return [];

        var from = Math.Max(0, end.Value - wanted);
        var records = await HistoryStream.ReadAsync(channel, queue, from, end.Value, cancellationToken).ConfigureAwait(false);
        var claims = records.Select(record => record.ToClaim()).OfType<ClaimRecord>();
        var results = records.Select(record => record.ToResult()).OfType<StoredResult>();
        return RunHistory.Build(job, claims, results, limit);
    }

    private async Task AppendAsync(string job, byte[] record, CancellationToken cancellationToken)
    {
        var stored = await _broker.PublishAsync(HistoryQueue(job), record, cancellationToken).ConfigureAwait(false);
        if (!stored) throw new AnarchonException(AnarchonErrorCode.Store, $"RabbitMQ refused a history record for job \"{job}\"");
    }

    private bool IsWonHere(string queue, string token)
    {
        lock (_gate)
        {
            return _won.TryGetValue(queue, out var won) && won == token;
        }
    }

    /// <summary>Remembers a claim this instance won, forgetting the oldest beyond <see cref="RememberedWins"/>.</summary>
    private void RememberWin(string queue, string token)
    {
        lock (_gate)
        {
            _won[queue] = token;
            if (_won.Count > RememberedWins) _won.RemoveAt(0);
        }
    }

    private string SlotQueue(string job, long slot) => $"{_prefix}{job}.slot.{slot}";

    private string HistoryQueue(string job) => $"{_prefix}{job}.history";

    /// <summary>The factory of the backend's own connection, named after its prefix so operators can tell it apart.</summary>
    private static ConnectionFactory Factory(string connectionString, string prefix, string paramName)
    {
        // The message never repeats the connection string: it may hold a password.
        if (!Uri.TryCreate(connectionString, UriKind.Absolute, out var uri) || uri.Scheme is not ("amqp" or "amqps"))
        {
            throw new ArgumentException("The RabbitMQ connection string must be an amqp:// or amqps:// URI", paramName);
        }

        // Reconnecting is done on the next call, not in the background: a claim never waits on a recovery.
        return new ConnectionFactory { Uri = uri, ClientProvidedName = $"anarchon:{prefix}", AutomaticRecoveryEnabled = false };
    }

    private static string? ServerProperty(IConnection connection, string name) =>
        connection.ServerProperties?.TryGetValue(name, out var value) == true ? Text(value) : null;

    private static string? Text(object? value) => value switch
    {
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        _ => value?.ToString(),
    };

    [GeneratedRegex("^[A-Za-z0-9_-]+$")]
    private static partial Regex SimpleName();

    [GeneratedRegex(@"\d+\.\d+")]
    private static partial Regex LeadingVersion();
}
