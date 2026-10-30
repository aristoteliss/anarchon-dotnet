// Copyright (C) 2026-present Aristotelis — see repository license.

// Sending notifications on a stack that has RabbitMQ or NATS but no Redis or SQL database for the
// scheduler. A broker picks one winner per run, but holds no lease: no backup takes over a run whose
// instance died, no catch-up, no overlap protection. The job is written so that doesn't hurt.
//
//   docker compose -f test/docker-compose.test.yml --profile brokers up -d   # or your own servers
//   BACKEND=nats STRATEGY=msg-id dotnet run --project examples/BrokerNotifications   # NATS at nats://localhost:4222
//   BACKEND=rabbitmq dotnet run --project examples/BrokerNotifications              # RabbitMQ at amqp://localhost:5672
//
// The approach, and why:
//   send everything outstanding   each run sends all notifications not sent yet, not "the ones of
//                                 this run". A run that is lost (its instance crashed) or fails only
//                                 delays them until the next run.
//   an idempotency key            each notification goes out with its id, so the provider drops a
//                                 repeat if an instance died between sending and marking it sent
//   Jitter                        spreads the runs over the instances
//   MaxLate < the broker's memory the broker remembers a claimed run for SlotTtl (RabbitMQ) or
//                                 DuplicateWindow (NATS), 5 min by default. A later claim could win
//                                 the same run again, so Anarchon refuses a job whose MaxLate plus
//                                 Jitter is not shorter
//   HistorySize                   brokers keep history by count; HistoryTtl is refused
//   no lease options              Overlap, MaxAttempts, CatchUp, Lease and Heartbeat are refused with
//                                 a broker backend: it can't honour them
//
// Three instances run for 20 s; the app adds a notification every second, and one run fails
// halfway. Every notification is still sent exactly once.

using System.Collections.Concurrent;
using Anarchon;
using static Anarchon.Examples.Example;

// Stands in for an outbox table the app writes to.
var outbox = new ConcurrentDictionary<int, DateTimeOffset?>();
// Stands in for the mail provider: it drops a repeated idempotency key.
var delivered = new ConcurrentDictionary<int, bool>();
var runs = 0;
var startedAt = DateTimeOffset.UtcNow;

var instances = new[] { "api-1", "api-2", "api-3" }.Select(Instance).ToList();
Say($"backend: {instances[0].Backend.Name}");
await Task.WhenAll(instances.Select(s => s.StartAsync()));

for (var id = 1; id <= 20; id++)
{
    outbox[id] = null;
    await Task.Delay(TimeSpan.FromSeconds(1));
}

await Task.Delay(TimeSpan.FromSeconds(3.5));
var history = (await instances[0].HistoryAsync("send-notifications", 50)).Where(run => run.StartedAt >= startedAt).ToList();
await Task.WhenAll(instances.Select(s => s.StopAsync()));

var unsent = outbox.Count(pair => pair.Value is null);
Say($"{outbox.Count} notifications, {delivered.Count} delivered, {unsent} not sent yet");
Say($"{history.Count} runs, on {history.Select(run => run.InstanceId).Distinct().Count()} instances");

// One instance of the service, sending the outbox every 3 s.
Scheduler Instance(string id)
{
    var scheduler = new Scheduler(new SchedulerOptions { Backend = Backend("notify"), InstanceId = id });
    scheduler.Every("send-notifications", TimeSpan.FromSeconds(3), ctx => SendOutstandingAsync(ctx.InstanceId), new()
    {
        Jitter = TimeSpan.FromMilliseconds(500),
        MaxLate = TimeSpan.FromMinutes(1),
        HistorySize = 100,
        Timeout = TimeSpan.FromSeconds(2.5),
    });
    scheduler.Failed += (_, run) => Say($"{run.InstanceId}: run failed ({run.Error}); the next run sends the rest");
    return scheduler;
}

Task SendOutstandingAsync(string instanceId)
{
    var failHalfway = Interlocked.Increment(ref runs) == 4;
    var pending = outbox.Where(pair => pair.Value is null).Select(pair => pair.Key).Order().ToList();
    for (var i = 0; i < pending.Count; i++)
    {
        if (failHalfway && i == pending.Count / 2) throw new IOException("mail provider: connection reset");
        delivered[pending[i]] = true; // the provider keys on the id
        outbox[pending[i]] = DateTimeOffset.UtcNow;
    }

    Say($"{instanceId}: sent {pending.Count} notification(s)");
    return Task.CompletedTask;
}
