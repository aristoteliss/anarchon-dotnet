// Copyright (C) 2026-present Aristotelis — see repository license.

using Microsoft.Extensions.Logging;

namespace Anarchon.Scheduling;

/// <summary>Everything the scheduler logs, under the category <c>Anarchon.Scheduler</c>.</summary>
internal static partial class Log
{
    [LoggerMessage(1, LogLevel.Debug, "{Job} @ {Due:O}: started on {Instance} (attempt {Attempt}, {Reason})")]
    public static partial void Started(ILogger logger, string job, DateTimeOffset due, string instance, int attempt, RunReason reason);

    [LoggerMessage(2, LogLevel.Debug, "{Job} @ {Due:O}: finished in {DurationMs} ms")]
    public static partial void Succeeded(ILogger logger, string job, DateTimeOffset due, double durationMs);

    [LoggerMessage(3, LogLevel.Warning, "{Job} @ {Due:O}: failed after {DurationMs} ms: {Error}")]
    public static partial void Failed(ILogger logger, string job, DateTimeOffset due, double durationMs, string? error, Exception cause);

    [LoggerMessage(4, LogLevel.Debug, "{Job} @ {Due:O}: interrupted after {DurationMs} ms (handed over: {HandedOver})")]
    public static partial void Interrupted(ILogger logger, string job, DateTimeOffset due, double durationMs, bool handedOver);

    [LoggerMessage(5, LogLevel.Debug, "{Job} @ {Due:O}: skipped ({Reason})")]
    public static partial void Skipped(ILogger logger, string job, DateTimeOffset due, SkipReason reason);

    [LoggerMessage(6, LogLevel.Warning, "{Job} @ {Due:O}: attempt {Attempt} was taken over by a backup")]
    public static partial void Lost(ILogger logger, string job, DateTimeOffset due, int attempt);

    [LoggerMessage(7, LogLevel.Error, "Error in {Subject}: {Message}")]
    public static partial void Unhandled(ILogger logger, string subject, string message, Exception error);

    [LoggerMessage(8, LogLevel.Error, "A {Event} handler threw")]
    public static partial void HandlerThrew(ILogger logger, string @event, Exception error);

    [LoggerMessage(9, LogLevel.Warning, "Closing {Backend} failed")]
    public static partial void CloseFailed(ILogger logger, string backend, Exception error);
}
