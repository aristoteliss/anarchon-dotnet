// Copyright (C) 2026-present Aristotelis — see repository license.

using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Anarchon.Nats;

/// <summary>The stream settings the backend relies on.</summary>
/// <param name="Name">The stream.</param>
/// <param name="Subjects">The subjects it must hold, such as <c>cron.&gt;</c>.</param>
/// <param name="DuplicateWindow">How long it recognizes a repeated message ID.</param>
/// <param name="PerSubject">How many messages it keeps per subject.</param>
/// <param name="Replicas">Its replicas; null leaves an existing stream's replicas alone.</param>
internal sealed record StreamSettings(string Name, string Subjects, TimeSpan DuplicateWindow, long PerSubject, int? Replicas);

/// <summary>Creates the backend's stream, or brings the settings it relies on up to date.</summary>
internal static class JetStreamSetup
{
    private const int CreateTries = 3;
    private const int NotFound = 404;

    /// <summary>Creates the stream or updates it; tries again when another instance created it at the same moment.</summary>
    public static async Task EnsureStreamAsync(INatsJSContext js, StreamSettings wanted, CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            if (await UpdateAsync(js, wanted, cancellationToken).ConfigureAwait(false)) return;

            try
            {
                var config = new StreamConfig(wanted.Name, [wanted.Subjects])
                {
                    DuplicateWindow = wanted.DuplicateWindow,
                    MaxMsgsPerSubject = wanted.PerSubject,
                    NumReplicas = wanted.Replicas ?? 1,
                };
                await js.CreateStreamAsync(config, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (NatsJSApiException) when (attempt < CreateTries)
            {
                // Another instance created it at the same moment: check its settings.
            }
        }
    }

    /// <summary>Brings an existing stream's settings up to date; false when there is no stream yet.</summary>
    private static async Task<bool> UpdateAsync(INatsJSContext js, StreamSettings wanted, CancellationToken cancellationToken)
    {
        INatsJSStream stream;
        try
        {
            stream = await js.GetStreamAsync(wanted.Name, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (NatsJSApiException error) when (error.Error.Code == NotFound)
        {
            return false;
        }

        var config = stream.Info.Config;
        var subjects = config.Subjects ?? [];
        var same = config.DuplicateWindow == wanted.DuplicateWindow
            && config.MaxMsgsPerSubject == wanted.PerSubject
            && (wanted.Replicas is null || config.NumReplicas == wanted.Replicas)
            && subjects.Contains(wanted.Subjects);
        if (same) return true;

        config.DuplicateWindow = wanted.DuplicateWindow;
        config.MaxMsgsPerSubject = wanted.PerSubject;
        config.NumReplicas = wanted.Replicas ?? config.NumReplicas;
        config.Subjects = [.. subjects.Union([wanted.Subjects])];
        await js.UpdateStreamAsync(config, cancellationToken).ConfigureAwait(false);
        return true;
    }
}
