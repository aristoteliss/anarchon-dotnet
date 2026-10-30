// Copyright (C) 2026-present Aristotelis — see repository license.

using RabbitMQ.Client;

namespace Anarchon.RabbitMQ;

/// <summary>Options of <see cref="RabbitMQBackend"/>.</summary>
/// <example>
/// <code>
/// new RabbitMQBackend(new RabbitMQBackendOptions
/// {
///     ConnectionString = builder.Configuration.GetConnectionString("rabbitmq"), // "amqps://user:password@rabbit.internal/vhost"
///     Prefix = "billing.cron.",
///     SlotTtl = TimeSpan.FromMinutes(15),
/// });
/// </code>
/// </example>
public sealed class RabbitMQBackendOptions
{
    /// <summary>
    /// An AMQP URI, such as <c>amqp://rabbit.internal</c> or <c>amqps://user:password@rabbit.internal/vhost</c>.
    /// The backend connects with it and closes the connection on stop. Default <c>amqp://localhost</c>.
    /// Leave it out when you set <see cref="Connection"/>.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// A connection your app already has. The backend opens its own channels on it, and never closes
    /// it. Leave <see cref="ConnectionString"/> out when you set it.
    /// </summary>
    public IConnection? Connection { get; set; }

    /// <summary>
    /// Prefix of every queue name. Default <c>cron.</c>. Gate queues are
    /// <c>&lt;prefix&gt;&lt;job&gt;.slot.&lt;slot&gt;</c>, history is <c>&lt;prefix&gt;&lt;job&gt;.history</c>.
    /// Instances that share jobs must use the same one.
    /// </summary>
    public string Prefix { get; set; } = "cron.";

    /// <summary>
    /// How long each run's gate queue lives after the last instance touched it. This is how long
    /// RabbitMQ remembers that a run was taken, so it must be longer than every job's jitter plus its
    /// MaxLate. Default 5 minutes.
    /// </summary>
    public TimeSpan SlotTtl { get; set; } = TimeSpan.FromMinutes(5);
}
