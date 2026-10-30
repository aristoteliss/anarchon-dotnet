// Copyright (C) 2026-present Aristotelis — see repository license.

using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Anarchon.RabbitMQ;

/// <summary>
/// The backend's connection and its confirm channel, opened once and opened again on the next call
/// after either closes. A connection the app passed in is used as it is, and never closed here.
/// </summary>
internal sealed class BrokerConnection(ConnectionFactory? factory, IConnection? connection)
{
    private readonly Lock _gate = new();
    private Task<IConnection>? _owned;
    private Task<IChannel>? _confirm;

    /// <summary>An open connection: the app's, or this backend's own.</summary>
    public async Task<IConnection> ConnectionAsync(CancellationToken cancellationToken)
    {
        if (connection is not null) return connection;

        var current = CurrentConnection();
        var open = await current.WaitAsync(cancellationToken).ConfigureAwait(false);
        if (open.IsOpen) return open;

        // Closed since it was opened: forget it, and connect again.
        await ForgetAsync(current).ConfigureAwait(false);
        return await CurrentConnection().WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Declares a queue on a channel of its own: a refused declaration closes the channel it was sent
    /// on. A queue that exists with other arguments (another instance's settings) is used as it is.
    /// </summary>
    public async Task DeclareAsync(string queue, IDictionary<string, object?> arguments, CancellationToken cancellationToken)
    {
        var open = await ConnectionAsync(cancellationToken).ConfigureAwait(false);
        var channel = await open.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        await using (channel.ConfigureAwait(false))
        {
            try
            {
                await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false, arguments, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (OperationInterruptedException error) when (error.ShutdownReason?.ReplyCode == Constants.PreconditionFailed)
            {
                // Declared earlier with other arguments: the queue works the same.
            }
        }
    }

    /// <summary>
    /// Publishes a persistent JSON message to a queue and waits for the broker's answer: true when it
    /// stored the message (ack), false when it refused it (nack), such as a full gate queue.
    /// </summary>
    public async Task<bool> PublishAsync(string queue, byte[] body, CancellationToken cancellationToken)
    {
        var channel = await ConfirmChannelAsync(cancellationToken).ConfigureAwait(false);
        var properties = new BasicProperties { Persistent = true, ContentType = "application/json" };
        try
        {
            await channel.BasicPublishAsync(exchange: "", routingKey: queue, mandatory: false, properties, body, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (PublishException error) when (!error.IsReturn)
        {
            return false;
        }
    }

    /// <summary>Closes the confirm channel, and the connection if this backend opened it. The next call opens them again.</summary>
    public async Task CloseAsync()
    {
        Task<IChannel>? confirm;
        Task<IConnection>? owned;
        lock (_gate)
        {
            (confirm, owned) = (_confirm, _owned);
            (_confirm, _owned) = (null, null);
        }

        if (confirm is not null) await CloseQuietlyAsync(confirm).ConfigureAwait(false);
        if (owned is not null) await CloseQuietlyAsync(owned).ConfigureAwait(false);
    }

    /// <summary>The channel claims and history records are published on, with publisher confirms.</summary>
    private async Task<IChannel> ConfirmChannelAsync(CancellationToken cancellationToken)
    {
        var current = CurrentConfirmChannel();
        var channel = await current.WaitAsync(cancellationToken).ConfigureAwait(false);
        if (channel.IsOpen) return channel;

        // Closed by the broker or with its connection: open another one. Only the caller that
        // forgets the closed channel disposes it.
        bool forgotten;
        lock (_gate)
        {
            forgotten = _confirm == current;
            if (forgotten) _confirm = null;
        }

        if (forgotten) await channel.DisposeAsync().ConfigureAwait(false);
        return await CurrentConfirmChannel().WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private Task<IConnection> CurrentConnection()
    {
        lock (_gate)
        {
            if (_owned is null || _owned.IsFaulted || _owned.IsCanceled) _owned = Task.Run(ConnectAsync);
            return _owned;
        }
    }

    private Task<IChannel> CurrentConfirmChannel()
    {
        lock (_gate)
        {
            if (_confirm is null || _confirm.IsFaulted || _confirm.IsCanceled) _confirm = Task.Run(OpenConfirmChannelAsync);
            return _confirm;
        }
    }

    private async Task<IConnection> ConnectAsync() =>
        await factory!.CreateConnectionAsync(CancellationToken.None).ConfigureAwait(false);

    private async Task<IChannel> OpenConfirmChannelAsync()
    {
        var open = await ConnectionAsync(CancellationToken.None).ConfigureAwait(false);
        var options = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
        return await open.CreateChannelAsync(options, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Forgets a connection that closed, unless another caller already replaced it, and disposes it.</summary>
    private async Task ForgetAsync(Task<IConnection> closed)
    {
        lock (_gate)
        {
            if (_owned != closed) return;
            (_owned, _confirm) = (null, null);
        }

        await CloseQuietlyAsync(closed).ConfigureAwait(false);
    }

    private static async Task CloseQuietlyAsync<T>(Task<T> opening)
        where T : IAsyncDisposable
    {
        try
        {
            var opened = await opening.ConfigureAwait(false);
            await opened.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception error) when (error is OperationInterruptedException or IOException)
        {
            // It never opened, or it is already closed: nothing left to close.
        }
    }
}
