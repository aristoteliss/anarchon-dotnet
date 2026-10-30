// Copyright (C) 2026-present Aristotelis — see repository license.

// A SaaS where every tenant gets the same jobs, on its own schedule: a digest at 08:00 in the
// tenant's time zone, a usage sync with a token fresh for each run, and a schedule that depends on
// the tenant's plan. Tenants come and go while the service runs.
//
//   dotnet run --project examples/Tenants                  # a SQLite file in the temp folder
//   BACKEND=redis dotnet run --project examples/Tenants    # see examples/Shared/Example.cs
//
// The approach, and why:
//   "<tenant>-<job>" names   one job per tenant and kind: history, RunNowAsync and Remove work per tenant
//   TimeZone per job         "0 8 * * *" means 08:00 where the tenant is, summer and winter
//   Data, a value            what the handler needs about the tenant (its id, its plan)
//   Data, a provider         called when each run starts: the token is valid at that moment, not
//                            when the service started
//   CatchUp                  a digest missed while every instance was down is still sent (here up to
//                            2 h late); a frequent sync is never caught up (zero)
//   Remove                   a tenant that leaves stops having jobs, on this instance
//   RunNowAsync(name, data)  a preview run for one tenant, with data for that run only
//
// Run the same code on every instance, with the same tenants: each run still happens once. With
// attributes, Register(object, new RegisterOptions { Prefix, TimeZone, Defaults }) does the same:
// see examples/Attributes. Real life: the digest is "0 8 * * *" and the sync hourly; here they run
// every few seconds.

using System.Globalization;
using Anarchon;
using static Anarchon.Examples.Example;

Tenant[] tenants = [new("acme", "Europe/Athens", "pro"), new("globex", "America/New_York", "free")];

await using var scheduler = new Scheduler(new SchedulerOptions { Backend = Backend("tenants"), InstanceId = "saas-1" });
foreach (var tenant in tenants)
{
    AddTenant(scheduler, tenant);
}

await scheduler.StartAsync();
Say("jobs per tenant:");
PrintJobs(scheduler);

await Task.Delay(TimeSpan.FromSeconds(9));
Say("initech signs up: its jobs are added while the scheduler runs");
AddTenant(scheduler, new Tenant("initech", "Asia/Tokyo", "pro"));

await Task.Delay(TimeSpan.FromSeconds(6));
Say("a support agent previews acme's digest: a manual run with data for that run only");
var preview = await scheduler.RunNowAsync("acme-digest", new Dictionary<string, object?> { ["preview"] = true });
Say($"preview run: {preview.Status}, manual: {preview.IsManual}");

await Task.Delay(TimeSpan.FromSeconds(3));
Say("globex leaves: its jobs are removed");
RemoveTenant(scheduler, "globex");
PrintJobs(scheduler);

await Task.Delay(TimeSpan.FromSeconds(8));
await scheduler.StopAsync();

static void AddTenant(Scheduler scheduler, Tenant tenant)
{
    var zone = TimeZoneInfo.FindSystemTimeZoneById(tenant.TimeZone);
    // Real life: daily for pro, weekly for free.
    var expression = tenant.Plan == "pro" ? "*/8 * * * * *" : "*/16 * * * * *";
    scheduler.Cron($"{tenant.Id}-digest", expression, ctx => SendDigestAsync(ctx, zone), new()
    {
        TimeZone = zone,
        Data = new Dictionary<string, object?> { ["tenant"] = tenant.Id, ["plan"] = tenant.Plan },
        CatchUp = TimeSpan.FromHours(2), // a digest missed during a deploy is still sent (the default is 5 min)
    });

    scheduler.Every($"{tenant.Id}-sync-usage", TimeSpan.FromSeconds(5), ctx =>
    {
        Say($"{ctx.Name}: synced with token {ctx.GetData<Dictionary<string, object?>>()["token"]}");
        return Task.CompletedTask;
    }, new()
    {
        Data = new JobDataProvider(async _ => new Dictionary<string, object?> { ["tenant"] = tenant.Id, ["token"] = await IssueTokenAsync(tenant.Id) }),
        Timeout = TimeSpan.FromSeconds(4),
        CatchUp = TimeSpan.Zero, // the next sync is moments away
    });
}

static Task SendDigestAsync(JobContext ctx, TimeZoneInfo zone)
{
    var data = ctx.GetData<IReadOnlyDictionary<string, object?>>();
    var local = TimeZoneInfo.ConvertTime(ctx.ScheduledAt, zone).ToString("HH:mm:ss", CultureInfo.InvariantCulture);
    var preview = data.GetValueOrDefault("preview") is true ? "PREVIEW of the " : "";
    var why = ctx.Reason == RunReason.Scheduled ? "" : $" ({ctx.Reason})";
    Say($"{ctx.Name}: {preview}{data["plan"]} digest, {local} in {zone.Id}{why}");
    return Task.CompletedTask;
}

static void RemoveTenant(Scheduler scheduler, string id)
{
    foreach (var job in scheduler.GetJobs().Where(job => job.Name.StartsWith($"{id}-", StringComparison.Ordinal)))
    {
        scheduler.Remove(job.Name);
    }
}

static void PrintJobs(Scheduler scheduler)
{
    foreach (var job in scheduler.GetJobs())
    {
        Say($"  {job.Name,-20} {job.Schedule,-46} next {job.NextRunAt:O}");
    }
}

// Stands in for an auth service: a short-lived token per tenant.
static async Task<string> IssueTokenAsync(string tenant)
{
    await Task.Delay(20);
    return $"{tenant}-{Random.Shared.Next(0x1000000):x6}";
}

/// <summary>Stands in for the tenants table.</summary>
internal sealed record Tenant(string Id, string TimeZone, string Plan);
