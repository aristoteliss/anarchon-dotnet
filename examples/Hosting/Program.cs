// Copyright (C) 2026-present Aristotelis — see repository license.

// A worker service: the jobs are methods marked with [Cron], [Every] and [At], each run gets its
// own service scope, and the host starts and stops the scheduler.
//
//   dotnet run --project examples/Hosting
//
// The Redis connection string is ConnectionStrings:redis (appsettings.json, or the environment
// variable ConnectionStrings__redis).

using Anarchon;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddScoped<IReportStore, ConsoleReportStore>();
builder.Services
    .AddAnarchon(options => options.TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Athens"))
    .UseRedis(options =>
    {
        options.Configuration = builder.Configuration.GetConnectionString("redis");
        options.Prefix = "example:hosting:";
    })
    .AddJobs<ReportJobs>();

var app = builder.Build();
var scheduler = app.Services.GetRequiredService<Scheduler>();
scheduler.Failed += (_, run) => app.Services.GetRequiredService<ILogger<Program>>().LogError(run.Cause, "{Job} failed", run.Job);

await app.RunAsync();

/// <summary>The jobs of this service. A new object, from a new scope, runs each job.</summary>
internal sealed class ReportJobs(IReportStore store, ILogger<ReportJobs> logger)
{
    [Every("10s", Name = "refresh-cache", Timeout = "8s")]
    public async Task RefreshCacheAsync(JobContext ctx)
    {
        logger.LogInformation("refreshing the cache for {Due:HH:mm:ss} on {Instance}", ctx.ScheduledAt, ctx.InstanceId);
        await Task.Delay(200, ctx.CancellationToken);
    }

    [Cron("0 8 * * 1-5", Name = "weekday-digest")]
    public Task SendDigestAsync(JobContext ctx) => store.SaveAsync($"digest for {ctx.ScheduledAt:yyyy-MM-dd}", ctx.CancellationToken);

    [Cron("*/30 * * * * *", Name = "half-minute-report", MaxAttempts = 2)]
    public Task ReportAsync(JobContext ctx) => store.SaveAsync($"report {ctx.Slot} (attempt {ctx.Attempt}, {ctx.Reason})", ctx.CancellationToken);
}

internal interface IReportStore
{
    Task SaveAsync(string report, CancellationToken cancellationToken);
}

internal sealed class ConsoleReportStore(ILogger<ConsoleReportStore> logger) : IReportStore
{
    public Task SaveAsync(string report, CancellationToken cancellationToken)
    {
        logger.LogInformation("saved {Report}", report);
        return Task.CompletedTask;
    }
}
