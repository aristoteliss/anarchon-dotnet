// Copyright (C) 2026-present Aristotelis — see repository license.

using NATS.Client.Core;

namespace Anarchon.Nats;

/// <summary>
/// How the NATS backend picks the single runner of each run. Every instance of a service must use
/// the same strategy: the two don't see each other's claims.
/// </summary>
/// <example>
/// <code>
/// new NatsBackend(new NatsBackendOptions { ConnectionString = natsUrl, Strategy = NatsStrategy.Lease });
/// </code>
/// </example>
public enum NatsStrategy
{
    /// <summary>
    /// The default: every instance publishes its claim with the same <c>Nats-Msg-Id</c>, and
    /// JetStream keeps the first one. One winner per run, nothing more.
    /// </summary>
    MsgId,

    /// <summary>
    /// A JetStream key-value bucket <c>&lt;stream&gt;_RUNS</c> holds each job's runs, and a write succeeds
    /// only if nobody wrote since it was read. Adds no overlapping runs, backup runners, catch-up and
    /// retries. Needs nats-server 2.10 or newer.
    /// </summary>
    Lease,
}

/// <summary>Options of <see cref="NatsBackend"/>.</summary>
/// <example>A three-node cluster: the stream replicated, so its memory of claimed runs survives a node
/// <code>
/// new NatsBackend(new NatsBackendOptions
/// {
///     ConnectionString = "nats://n1:4222,nats://n2:4222,nats://n3:4222",
///     Stream = "BILLING_CRON",
///     Prefix = "billing.cron.",
///     Replicas = 3,
///     DuplicateWindow = TimeSpan.FromMinutes(10), // instances may claim up to 10 minutes late
/// });
/// </code>
/// </example>
public sealed class NatsBackendOptions
{
    /// <summary>How the single runner of each run is chosen. Default <see cref="NatsStrategy.MsgId"/>.</summary>
    public NatsStrategy Strategy { get; set; } = NatsStrategy.MsgId;

    /// <summary>
    /// A NATS URL, or several separated by commas, such as <c>nats://nats.internal:4222</c>. The backend
    /// connects with it and closes the connection on stop. Default <c>nats://localhost:4222</c>. Leave it
    /// out when you set <see cref="Connection"/>.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// A connection your app already has (<c>NatsClient.Connection</c>, or a <c>NatsConnection</c>).
    /// It is shared, and never closed by the scheduler. Leave <see cref="ConnectionString"/> out when you set it.
    /// </summary>
    public INatsConnection? Connection { get; set; }

    /// <summary>
    /// The JetStream stream, created on start if it doesn't exist. Default <c>CRON</c>. With
    /// <see cref="NatsStrategy.Lease"/> it names the bucket <c>&lt;stream&gt;_RUNS</c> too, so it may then only use
    /// letters, digits, <c>_</c> and <c>-</c>.
    /// </summary>
    public string Stream { get; set; } = "CRON";

    /// <summary>Subject prefix: subjects are <c>&lt;prefix&gt;&lt;job&gt;.claims</c> and <c>&lt;prefix&gt;&lt;job&gt;.results</c>. Default <c>cron.</c>.</summary>
    public string Prefix { get; set; } = "cron.";

    /// <summary>
    /// How long JetStream remembers a claimed run (the stream's duplicate window). Must be longer than
    /// every job's jitter plus its MaxLate. Default 5 minutes. With <see cref="NatsStrategy.Lease"/>, the
    /// bucket remembers runs instead, and this window only drops a history record sent twice.
    /// </summary>
    public TimeSpan DuplicateWindow { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// About how many runs to keep per job. All jobs share the stream, so it keeps the largest of this
    /// and every job's <c>HistorySize</c>, for each job. Default 1000.
    /// </summary>
    public int HistorySize { get; set; } = 1_000;

    /// <summary>
    /// Stream replicas in a NATS cluster; use 3 so the stream survives the loss of a server. A new
    /// stream gets 1 when this is not set. When it is set, an existing stream is updated to it; when it
    /// is not, an existing stream keeps its replicas. A new bucket of runs gets the same number.
    /// </summary>
    public int? Replicas { get; set; }
}
