// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon;

/// <summary>
/// A failure that Anarchon reports with a stable <see cref="Code"/>. Check the code rather than the
/// message: codes stay the same across releases, messages may be reworded.
/// </summary>
/// <remarks>
/// A value that can't be used (a negative duration, an invalid cron expression, an empty job name)
/// throws <see cref="ArgumentException"/> or <see cref="ArgumentOutOfRangeException"/> instead, as
/// .NET APIs do. Everything else Anarchon refuses is an <see cref="AnarchonException"/>.
/// </remarks>
/// <example>
/// <code>
/// try
/// {
///     await scheduler.RunNowAsync(name);
/// }
/// catch (AnarchonException e) when (e.Code == AnarchonErrorCode.UnknownJob)
/// {
///     return Results.NotFound();
/// }
/// </code>
/// </example>
public sealed class AnarchonException : Exception
{
    /// <summary>Creates an exception with a code and a message.</summary>
    /// <param name="code">What kind of problem it is.</param>
    /// <param name="message">What went wrong, naming the job, option or object.</param>
    /// <param name="innerException">The error that caused it, if any.</param>
    public AnarchonException(AnarchonErrorCode code, string message, Exception? innerException = null)
        : base(message, innerException)
    {
        Code = code;
    }

    /// <summary>What kind of problem it is. Stable across releases.</summary>
    public AnarchonErrorCode Code { get; }
}

/// <summary>
/// The kinds of <see cref="AnarchonException"/>. New codes may be added in a minor release: keep a
/// default branch when you switch over them.
/// </summary>
/// <example>
/// <code>
/// scheduler.Error += (_, e) =>
/// {
///     if (e.Error is AnarchonException { Code: AnarchonErrorCode.Store }) metrics.Increment("scheduler.store_refused");
/// };
/// </code>
/// </example>
public enum AnarchonErrorCode
{
    /// <summary>A valid option the backend or strategy can't honour, such as a lease option on a backend without leases.</summary>
    Unsupported = 1,

    /// <summary>Job settings that could let a late instance run a job twice on this backend.</summary>
    UnsafeJob,

    /// <summary>The server is too old, or lacks a feature the backend needs.</summary>
    Server,

    /// <summary>A call the scheduler's state doesn't allow, such as <c>RunNowAsync()</c> before <c>StartAsync()</c>.</summary>
    State,

    /// <summary>A job name that is already registered on this scheduler.</summary>
    JobExists,

    /// <summary>A job name that is not registered on this scheduler.</summary>
    UnknownJob,

    /// <summary>The store refused a write.</summary>
    Store,

    /// <summary>The store holds data that a newer version of Anarchon wrote.</summary>
    StoreFormat,
}
