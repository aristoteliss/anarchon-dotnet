// Copyright (C) 2026-present Aristotelis — see repository license.

using Microsoft.Extensions.Hosting;

namespace Anarchon.Hosting;

/// <summary>
/// Starts a scheduler with the host and stops it on shutdown. A scheduler that can't reach its store
/// fails the host's start, as a missing database would.
/// </summary>
internal sealed class SchedulerHostedService(Scheduler scheduler) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) => scheduler.StartAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) => scheduler.StopAsync(cancellationToken);
}
