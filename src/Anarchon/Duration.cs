// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace Anarchon;

/// <summary>
/// Reads and writes durations such as <c>"10s"</c>, <c>"35m"</c> or <c>"1h30m"</c>: the form job
/// attributes and configuration files take.
/// </summary>
/// <example>
/// <code>
/// var timeout = Duration.Parse(configuration["Jobs:ExportTimeout"] ?? "5m");
/// scheduler.Every("export", TimeSpan.FromHours(1), ExportAsync, new() { Timeout = timeout });
/// </code>
/// </example>
public static partial class Duration
{
    private static readonly (string Unit, long Ms)[] Units =
    [
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("m", 60_000),
        ("s", 1_000),
    ];

    /// <summary>
    /// Reads milliseconds (<c>"10000"</c>) or a duration string: <c>"10s"</c>, <c>"10m"</c>,
    /// <c>"1h30m"</c>, <c>"2d"</c>, <c>"500ms"</c>, <c>"1.5h"</c>.
    /// </summary>
    /// <param name="value">The text to read. Units: <c>d</c>, <c>h</c>, <c>m</c>, <c>s</c>, <c>ms</c>.</param>
    /// <returns>The duration, a positive whole number of milliseconds.</returns>
    /// <exception cref="ArgumentException">The text is not a duration, or it is not positive.</exception>
    /// <example>
    /// <code>
    /// Duration.Parse("1h30m"); // 01:30:00
    /// Duration.Parse("250");   // 250 ms
    /// </code>
    /// </example>
    public static TimeSpan Parse(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var text = string.Concat(value.Where(c => !char.IsWhiteSpace(c))).ToLowerInvariant();

        var total = IsDigits(text) ? ParseNumber(value, text) : SumParts(value, text);
        var ms = Math.Round(total);
        if (ms <= 0)
        {
            throw new ArgumentException($"Invalid duration \"{value}\": it must be at least 1 ms", nameof(value));
        }

        return TimeSpan.FromMilliseconds(ms);
    }

    /// <summary>Writes a duration the way <see cref="Parse"/> reads it, largest units first.</summary>
    /// <param name="value">The duration; anything under a millisecond is dropped.</param>
    /// <returns>Text such as <c>"1h30m"</c> or <c>"1m30s500ms"</c>.</returns>
    /// <example>
    /// <code>
    /// Duration.Format(TimeSpan.FromMinutes(90)); // "1h30m"
    /// logger.LogInformation("next run in {Wait}", Duration.Format(next - DateTimeOffset.UtcNow));
    /// </code>
    /// </example>
    public static string Format(TimeSpan value)
    {
        var rest = (long)value.TotalMilliseconds;
        var text = new StringBuilder();

        foreach (var (unit, size) in Units)
        {
            if (rest < size) continue;
            text.Append(CultureInfo.InvariantCulture, $"{rest / size}{unit}");
            rest %= size;
        }

        if (rest > 0 || text.Length == 0) text.Append(CultureInfo.InvariantCulture, $"{rest}ms");
        return text.ToString();
    }

    private static bool IsDigits(string text) => text.Length > 0 && text.All(char.IsAsciiDigit);

    private static double ParseNumber(string value, string text)
    {
        if (!long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var ms))
        {
            throw Invalid(value);
        }

        return ms;
    }

    /// <summary>The milliseconds of every amount and unit pair, such as <c>1h</c> and <c>30m</c> in <c>"1h30m"</c>.</summary>
    private static double SumParts(string value, string text)
    {
        var parts = Part().Matches(text);
        var covered = parts.Sum(part => part.Length);
        if (parts.Count == 0 || covered != text.Length) throw Invalid(value);

        double total = 0;
        foreach (Match part in parts)
        {
            var amount = double.Parse(part.Groups["amount"].Value, CultureInfo.InvariantCulture);
            total += amount * UnitMs(part.Groups["unit"].Value);
        }

        return total;
    }

    private static long UnitMs(string unit) => unit switch
    {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        _ => 86_400_000,
    };

    private static ArgumentException Invalid(string value) =>
        new($"Invalid duration \"{value}\" (examples: \"10s\", \"35m\", \"1h30m\")", nameof(value));

    [GeneratedRegex(@"(?<amount>\d+(?:\.\d+)?)(?<unit>ms|s|m|h|d)", RegexOptions.CultureInvariant)]
    private static partial Regex Part();
}
