// Copyright (C) 2026-present Aristotelis — see repository license.

// An ASP.NET Core API on top of the scheduler: list the jobs, read their history, run one now.
//
//   dotnet run --project examples/HttpApi                  # a SQLite file in the temp folder
//   BACKEND=redis dotnet run --project examples/HttpApi    # see examples/Shared/Example.cs
//
//   curl localhost:3000/jobs                      jobs on this instance and their next run
//   curl localhost:3000/jobs/sync/runs?limit=5    latest runs from all instances
//   curl localhost:3000/jobs/sync/last            the latest run
//   curl -X POST localhost:3000/jobs/sync/run     run it now, on this instance
//
// Start it twice (PORT=3001 for the second) to see runs spread over both instances in /runs.
// Every request is also in examples/HttpApi/HttpApi.http, to send from Visual Studio, Rider or VS Code.

using System.Text.Json.Serialization;
using Anarchon;
using static Anarchon.Examples.Example;

var port = Environment.GetEnvironmentVariable("PORT") ?? "3000";
var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://localhost:{port}");
builder.Services.ConfigureHttpJsonOptions(options => options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));
builder.Services
    .AddAnarchon(options => options.InstanceId = $"api-{port}")
    .UseBackend(_ => Backend("http"));

var app = builder.Build();
var scheduler = app.Services.GetRequiredService<Scheduler>();
scheduler
    .Every("sync", TimeSpan.FromSeconds(10), _ => Task.Delay(300))
    .Cron("digest", "0 */2 * * *", _ => Task.CompletedTask, new() { TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Athens") });
scheduler.Succeeded += (_, run) => Say($"{run.Job} ran here in {run.Duration?.TotalMilliseconds:0} ms");

// An unknown job name or a bad limit throws: answer 400 with the message.
app.Use(async (context, next) =>
{
    try
    {
        await next(context);
    }
    catch (Exception error) when (error is ArgumentException or AnarchonException)
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
        await context.Response.WriteAsJsonAsync(new { error = error.Message });
    }
});

app.MapGet("/jobs", () => scheduler.GetJobs());
app.MapGet("/jobs/{name}/runs", (string name, int? limit) => scheduler.HistoryAsync(name, limit ?? 20));
app.MapGet("/jobs/{name}/last", (string name) => scheduler.LastRunAsync(name));
// Answers when the job has finished, with its record.
app.MapPost("/jobs/{name}/run", (string name) => scheduler.RunNowAsync(name));

app.Lifetime.ApplicationStarted.Register(() => Say($"API on http://localhost:{port}/jobs ({scheduler.Backend.Name})"));
await app.RunAsync();
