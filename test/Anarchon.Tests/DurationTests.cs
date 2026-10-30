// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.Tests;

public sealed class DurationTests
{
    [Theory]
    [InlineData("10000", 10_000)]
    [InlineData("10s", 10_000)]
    [InlineData("35m", 2_100_000)]
    [InlineData("1h30m", 5_400_000)]
    [InlineData("1h 30m", 5_400_000)]
    [InlineData("2d", 172_800_000)]
    [InlineData("500ms", 500)]
    [InlineData("1.5h", 5_400_000)]
    [InlineData(" 10S ", 10_000)]
    public void parse_reads_milliseconds_and_duration_strings(string text, long ms) =>
        Assert.Equal(TimeSpan.FromMilliseconds(ms), Duration.Parse(text));

    [Theory]
    [InlineData("")]
    [InlineData("ten seconds")]
    [InlineData("10x")]
    [InlineData("h")]
    [InlineData("0s")]
    [InlineData("-5s")]
    public void parse_refuses_what_is_not_a_positive_duration(string text) =>
        Assert.Throws<ArgumentException>(() => Duration.Parse(text));

    [Theory]
    [InlineData(5_400_000, "1h30m")]
    [InlineData(90_500, "1m30s500ms")]
    [InlineData(0, "0ms")]
    [InlineData(86_400_000, "1d")]
    public void format_writes_what_parse_reads(long ms, string text)
    {
        Assert.Equal(text, Duration.Format(TimeSpan.FromMilliseconds(ms)));
        if (ms > 0) Assert.Equal(TimeSpan.FromMilliseconds(ms), Duration.Parse(text));
    }
}
