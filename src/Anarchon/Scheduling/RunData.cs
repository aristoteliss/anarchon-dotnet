// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.Scheduling;

/// <summary>The data of one run, built from its sources at the moment the run starts.</summary>
internal static class RunData
{
    /// <summary>
    /// Every source, from the most general to the most specific, on top of the one before. A source
    /// that is a <see cref="JobDataProvider"/> is called for its value.
    /// </summary>
    public static async ValueTask<object?> ResolveAsync(IEnumerable<object?> sources, JobContext run)
    {
        object? data = null;

        foreach (var source in sources)
        {
            var value = source is JobDataProvider provider ? await provider(run).ConfigureAwait(false) : source;
            data = Merge(data, value);
        }

        return data;
    }

    /// <summary>More specific data on top of more general data: two dictionaries are merged, anything else replaces.</summary>
    private static object? Merge(object? general, object? specific)
    {
        if (specific is null) return general;
        if (general is not IReadOnlyDictionary<string, object?> under) return specific;
        if (specific is not IReadOnlyDictionary<string, object?> over) return specific;

        var merged = new Dictionary<string, object?>(under);
        foreach (var (key, value) in over)
        {
            merged[key] = value;
        }

        return merged;
    }
}
