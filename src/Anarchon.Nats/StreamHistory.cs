// Copyright (C) 2026-present Aristotelis — see repository license.

using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Anarchon.Nats;

/// <summary>Reads a job's records from the backend's stream: the subjects <c>&lt;prefix&gt;&lt;job&gt;.*</c>.</summary>
internal static class StreamHistory
{
    private const int MaxBatch = 1_000;

    /// <summary>A fetch waits this long for messages the server says it holds.</summary>
    private static readonly TimeSpan FetchWait = TimeSpan.FromSeconds(5);

    /// <summary>The server forgets the reading consumer this long after its last use, if deleting it failed.</summary>
    private static readonly TimeSpan ForgetAfter = TimeSpan.FromSeconds(30);

    /// <summary>How many messages the subjects under the filter hold, so each fetch asks for exactly what is there.</summary>
    public static async Task<long> CountAsync(INatsJSContext js, string stream, string filter, CancellationToken cancellationToken)
    {
        var info = await js.GetStreamAsync(stream, new StreamInfoRequest { SubjectsFilter = filter }, cancellationToken).ConfigureAwait(false);
        return info.Info.State.Subjects?.Values.Sum() ?? 0;
    }

    /// <summary>Reads the messages under the filter through a consumer of their own, deleted afterwards.</summary>
    public static async Task<List<StoredRecord>> ReadAsync(INatsJSContext js, string stream, string filter, long count, CancellationToken cancellationToken)
    {
        // Named: the server matches an unnamed consumer's create request against the filter, and refuses it.
        var config = new ConsumerConfig($"anarchon_history_{Guid.NewGuid():N}")
        {
            FilterSubject = filter,
            AckPolicy = ConsumerConfigAckPolicy.None,
            DeliverPolicy = ConsumerConfigDeliverPolicy.All,
            InactiveThreshold = ForgetAfter,
            MemStorage = true,
        };
        var consumer = await js.CreateOrUpdateConsumerAsync(stream, config, cancellationToken).ConfigureAwait(false);
        try
        {
            return await FetchAllAsync(consumer, count, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // The server would keep the consumer until its inactive threshold.
            await js.DeleteConsumerAsync(stream, consumer.Info.Name, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private static async Task<List<StoredRecord>> FetchAllAsync(INatsJSConsumer consumer, long count, CancellationToken cancellationToken)
    {
        var records = new List<StoredRecord>();
        var remaining = count;
        while (remaining > 0)
        {
            var (received, caughtUp) = await FetchBatchAsync(consumer, (int)Math.Min(remaining, MaxBatch), records, cancellationToken).ConfigureAwait(false);
            if (received == 0) break;

            remaining = caughtUp ? 0 : remaining - received;
        }

        return records;
    }

    /// <summary>Reads one batch; caught up once the server has nothing newer.</summary>
    private static async Task<(int Received, bool CaughtUp)> FetchBatchAsync(INatsJSConsumer consumer, int size, List<StoredRecord> records, CancellationToken cancellationToken)
    {
        var options = new NatsJSFetchOpts { MaxMsgs = size, Expires = FetchWait };
        var (received, caughtUp) = (0, false);
        await foreach (var message in consumer.FetchAsync(options, NatsRawSerializer<byte[]>.Default, cancellationToken).ConfigureAwait(false))
        {
            received++;
            if (message.Data is { } data && StoredRecord.Parse(data) is { } record) records.Add(record);
            if (message.Metadata?.NumPending == 0) caughtUp = true;
        }

        return (received, caughtUp);
    }
}
