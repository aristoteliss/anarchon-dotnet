// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Anarchon.RabbitMQ;

/// <summary>
/// Reads a job's history stream queue. A stream is read by consuming it from an offset; consuming
/// from <c>last</c> delivers its newest chunk, which tells where the stream ends.
/// </summary>
internal static class HistoryStream
{
    private const string OffsetArgument = "x-stream-offset";

    /// <summary>How often the reader checks whether the deliveries stopped.</summary>
    private static readonly TimeSpan Poll = TimeSpan.FromMilliseconds(10);

    /// <summary>The newest chunk has fully arrived once no delivery came for this long.</summary>
    private static readonly TimeSpan QuietAfterDelivery = TimeSpan.FromMilliseconds(60);

    /// <summary>A stream that delivered nothing for this long is empty.</summary>
    private static readonly TimeSpan EmptyAfter = TimeSpan.FromMilliseconds(300);

    /// <summary>A read stops after this long, with what arrived, even if the last record never came.</summary>
    private static readonly TimeSpan ReadLimit = TimeSpan.FromSeconds(5);

    /// <summary>The offset of the newest record, or null when the stream is empty.</summary>
    public static async Task<long?> LastOffsetAsync(IChannel channel, string queue, CancellationToken cancellationToken)
    {
        var newest = new NewestDelivery();
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, delivery) =>
        {
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, cancellationToken).ConfigureAwait(false);
            newest.Record(OffsetOf(delivery));
        };

        var tag = await ConsumeAsync(channel, queue, "last", consumer, cancellationToken).ConfigureAwait(false);
        await newest.WaitForQuietAsync(cancellationToken).ConfigureAwait(false);
        await channel.BasicCancelAsync(tag, noWait: false, cancellationToken).ConfigureAwait(false);
        return newest.Offset;
    }

    /// <summary>The records from offset <paramref name="from"/> up to and including <paramref name="end"/>.</summary>
    public static async Task<List<StoredRecord>> ReadAsync(IChannel channel, string queue, long from, long end, CancellationToken cancellationToken)
    {
        var records = new ConcurrentQueue<StoredRecord>();
        var reachedEnd = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, delivery) =>
        {
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, cancellationToken).ConfigureAwait(false);
            if (StoredRecord.Parse(delivery.Body.Span) is { } record) records.Enqueue(record);
            if (OffsetOf(delivery) >= end) reachedEnd.TrySetResult();
        };

        var tag = await ConsumeAsync(channel, queue, from, consumer, cancellationToken).ConfigureAwait(false);
        try
        {
            await reachedEnd.Task.WaitAsync(ReadLimit, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // A record went missing (trimmed meanwhile): use what arrived.
        }

        await channel.BasicCancelAsync(tag, noWait: false, cancellationToken).ConfigureAwait(false);
        return [.. records];
    }

    private static Task<string> ConsumeAsync(IChannel channel, string queue, object offset, AsyncEventingBasicConsumer consumer, CancellationToken cancellationToken) =>
        channel.BasicConsumeAsync(
            queue,
            autoAck: false,
            consumerTag: "",
            noLocal: false,
            exclusive: false,
            arguments: new Dictionary<string, object?> { [OffsetArgument] = offset },
            consumer,
            cancellationToken);

    private static long OffsetOf(BasicDeliverEventArgs delivery)
    {
        var headers = delivery.BasicProperties.Headers;
        return headers is not null && headers.TryGetValue(OffsetArgument, out var offset) ? Convert.ToInt64(offset, CultureInfo.InvariantCulture) : -1;
    }

    /// <summary>The newest offset delivered so far, and when the last delivery came.</summary>
    private sealed class NewestDelivery
    {
        private readonly Lock _gate = new();
        private readonly long _started = Stopwatch.GetTimestamp();
        private long? _offset;
        private long _lastAt;

        public long? Offset
        {
            get
            {
                lock (_gate)
                {
                    return _offset;
                }
            }
        }

        public void Record(long offset)
        {
            lock (_gate)
            {
                _offset = Math.Max(_offset ?? -1, offset);
                _lastAt = Stopwatch.GetTimestamp();
            }
        }

        /// <summary>Waits until no delivery came for <see cref="QuietAfterDelivery"/>, or none at all for <see cref="EmptyAfter"/>.</summary>
        public async Task WaitForQuietAsync(CancellationToken cancellationToken)
        {
            while (!IsQuiet())
            {
                await Task.Delay(Poll, cancellationToken).ConfigureAwait(false);
            }
        }

        private bool IsQuiet()
        {
            lock (_gate)
            {
                return _offset is null
                    ? Stopwatch.GetElapsedTime(_started) >= EmptyAfter
                    : Stopwatch.GetElapsedTime(_lastAt) >= QuietAfterDelivery;
            }
        }
    }
}
