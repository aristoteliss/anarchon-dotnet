// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.Backends;

/// <summary>
/// The text a backend stores for a run's status and reason. Every Anarchon sharing a store, the
/// Node.js package included, writes and reads these values, so they never change.
/// </summary>
/// <example>
/// <code>
/// command.Parameters.Add(new NpgsqlParameter("status", RunText.ToText(RunStatus.Success))); // "success"
/// var status = RunText.ToStatus(reader.GetString(0)) ?? RunStatus.Interrupted;
/// </code>
/// </example>
public static class RunText
{
    /// <summary>The stored text of a status: <c>running</c>, <c>success</c>, <c>failed</c> or <c>interrupted</c>.</summary>
    /// <param name="status">The status.</param>
    /// <returns>Its text.</returns>
    public static string ToText(RunStatus status) => status switch
    {
        RunStatus.Success => "success",
        RunStatus.Failed => "failed",
        RunStatus.Interrupted => "interrupted",
        _ => "running",
    };

    /// <summary>The status a stored text names.</summary>
    /// <param name="text">The stored text.</param>
    /// <returns>The status, or null for a text this version doesn't know.</returns>
    public static RunStatus? ToStatus(string? text) => text switch
    {
        "running" => RunStatus.Running,
        "success" => RunStatus.Success,
        "failed" => RunStatus.Failed,
        "interrupted" => RunStatus.Interrupted,
        _ => null,
    };

    /// <summary>The stored text of a reason: <c>scheduled</c>, <c>manual</c>, <c>catch-up</c> or <c>takeover</c>.</summary>
    /// <param name="reason">The reason.</param>
    /// <returns>Its text.</returns>
    public static string ToText(RunReason reason) => reason switch
    {
        RunReason.Manual => "manual",
        RunReason.CatchUp => "catch-up",
        RunReason.Takeover => "takeover",
        _ => "scheduled",
    };

    /// <summary>The reason a stored text names.</summary>
    /// <param name="text">The stored text.</param>
    /// <returns>The reason, or null for a text this version doesn't know.</returns>
    public static RunReason? ToReason(string? text) => text switch
    {
        "scheduled" => RunReason.Scheduled,
        "manual" => RunReason.Manual,
        "catch-up" => RunReason.CatchUp,
        "takeover" => RunReason.Takeover,
        _ => null,
    };
}
