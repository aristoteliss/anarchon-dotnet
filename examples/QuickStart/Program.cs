// Copyright (C) 2026-present Aristotelis — see repository license.

// Three real processes share one job and one store, and the process running the job is taken away
// three times.
//
//   dotnet run --project examples/QuickStart                  # a SQLite file in the temp folder
//   BACKEND=nats dotnet run --project examples/QuickStart     # or redis, postgres: see examples/Shared/Example.cs
//
// 1. Graceful stop: the runner is told to stop (as on a deploy). It hands the run over, and another
//    process starts the same run again as attempt 2 within two heartbeats.
// 2. Crash: the next runner is killed (Process.Kill, like kill -9). Nothing is handed over; another
//    process starts the same run again as attempt 2 once the lease runs out.
// 3. Downtime: every process is killed while the next run falls due. A new process with the default
//    CatchUp (5 min) runs it when it starts (reason CatchUp).
//
// It takes about 40 s. The lease is 2 s so the takeover is quick to watch; the default is 30 s. One
// SQLite file coordinates processes on one machine only: use Redis, PostgreSQL or NATS across machines.

using Anarchon.Examples;

if (args is ["instance", var id, var job, var offsetMs])
{
    await Instance.RunAsync(id, job, long.Parse(offsetMs, System.Globalization.CultureInfo.InvariantCulture));
    return;
}

await new Demo().RunAsync();
