// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Tests;

/// <summary>The helpers backends share: the plain and the manual claim on a lease store, and the stored text.</summary>
public sealed class BackendHelpersTests
{
    [Fact]
    public async Task a_manual_run_on_a_lease_store_takes_the_first_free_millisecond_from_its_slot_with_one_attempt_and_no_overlap_check()
    {
        var runs = new RecordingRuns();

        var first = await runs.ClaimManualRunAsync(Input(1_000), CancellationToken.None);
        var second = await runs.ClaimManualRunAsync(Input(1_000), CancellationToken.None);

        Assert.Equal((1_000L, 1_001L), (first, second));
        Assert.All(runs.Claims, claim => Assert.Equal((RunReason.Manual, 1, OverlapPolicy.Allow, TimeSpan.FromDays(1)), (claim.Reason, claim.MaxAttempts, claim.Overlap, claim.Lease)));
    }

    [Fact]
    public async Task a_manual_run_fails_with_a_store_error_when_ten_milliseconds_in_a_row_are_taken()
    {
        var runs = new RecordingRuns { AllTaken = true };

        var error = await Assert.ThrowsAsync<AnarchonException>(() => runs.ClaimManualRunAsync(Input(1_000), CancellationToken.None));

        Assert.Equal(AnarchonErrorCode.Store, error.Code);
        Assert.Equal(10, runs.Claims.Count);
    }

    [Fact]
    public async Task the_plain_claim_on_a_lease_store_wins_only_on_won_and_claims_a_scheduled_run_without_backups()
    {
        var runs = new RecordingRuns();

        var won = await runs.ClaimWithoutBackupsAsync(Input(1_000), CancellationToken.None);
        var lost = await runs.ClaimWithoutBackupsAsync(Input(1_000), CancellationToken.None);

        Assert.Equal((true, false), (won, lost));
        Assert.All(runs.Claims, claim => Assert.Equal((ClaimMode.New, RunReason.Scheduled, 1), (claim.Mode, claim.Reason, claim.MaxAttempts)));
        await Assert.ThrowsAsync<ArgumentNullException>(() => runs.ClaimWithoutBackupsAsync(null!, CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentNullException>(() => runs.ClaimManualRunAsync(null!, CancellationToken.None));
    }

    [Fact]
    public void the_stored_text_of_every_status_and_reason_reads_back_and_unknown_text_reads_as_null()
    {
        foreach (var status in Enum.GetValues<RunStatus>())
        {
            Assert.Equal(status, RunText.ToStatus(RunText.ToText(status)));
        }

        foreach (var reason in Enum.GetValues<RunReason>())
        {
            Assert.Equal(reason, RunText.ToReason(RunText.ToText(reason)));
        }

        Assert.Equal(["running", "success", "failed", "interrupted"], new[] { RunStatus.Running, RunStatus.Success, RunStatus.Failed, RunStatus.Interrupted }.Select(RunText.ToText));
        Assert.Equal(["scheduled", "manual", "catch-up", "takeover"], new[] { RunReason.Scheduled, RunReason.Manual, RunReason.CatchUp, RunReason.Takeover }.Select(RunText.ToText));
        Assert.Equal((null, null), (RunText.ToStatus("paused"), RunText.ToReason(null)));
    }

    private static ClaimInput Input(long slot) => new() { Job = "job", Slot = slot, Token = Guid.NewGuid().ToString(), InstanceId = "A", HistorySize = 10 };

    /// <summary>A lease store in memory that records every claim: the first claim of a slot wins.</summary>
    private sealed class RecordingRuns : IRunStore
    {
        private readonly HashSet<long> _taken = [];

        public bool AllTaken { get; init; }

        public List<RunClaim> Claims { get; } = [];

        public Task<ClaimOutcome> ClaimAsync(RunClaim claim, CancellationToken cancellationToken)
        {
            Claims.Add(claim);
            var free = !AllTaken && _taken.Add(claim.Slot);
            return Task.FromResult<ClaimOutcome>(free ? new ClaimOutcome.Won(1) : new ClaimOutcome.Running(1));
        }

        public Task<bool> HeartbeatAsync(string job, long slot, string token, TimeSpan lease, CancellationToken cancellationToken) => Task.FromResult(true);

        public Task<bool> FinishAsync(string job, long slot, string token, RunStatus status, CancellationToken cancellationToken) => Task.FromResult(true);

        public Task<bool> ReleaseAsync(string job, long slot, string token, CancellationToken cancellationToken) => Task.FromResult(true);

        public Task<IReadOnlyList<ActiveRun>> ActiveAsync(string job, CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<ActiveRun>>([]);
    }
}
