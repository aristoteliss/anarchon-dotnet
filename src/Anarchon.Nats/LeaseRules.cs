// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Nats;

/// <summary>What a claim does with the job's runs: answer without writing, or write the runs and then answer.</summary>
internal abstract record LeaseStep(ClaimOutcome Outcome)
{
    /// <summary>Answer without writing.</summary>
    public sealed record Answer(ClaimOutcome Outcome) : LeaseStep(Outcome);

    /// <summary>Write <paramref name="Runs"/> at the revision read, then answer.</summary>
    public sealed record Write(JobRuns Runs, ClaimOutcome Outcome) : LeaseStep(Outcome);
}

/// <summary>
/// The lease rules on a job's runs, decided on the server's clock: the rules of the Redis CLAIM script,
/// in its order, as the Node.js package applies them to the same JSON.
/// </summary>
internal static class LeaseRules
{
    /// <summary>Decides a claim.</summary>
    public static LeaseStep Decide(JobRuns job, RunClaim claim, long now)
    {
        var run = job.Find(claim.Slot);
        if (run is null) return claim.Mode == ClaimMode.Takeover ? new LeaseStep.Answer(new ClaimOutcome.None()) : Start(job, claim, now);

        // Our own claim, sent again.
        if (run.IsRunning && run.Token == claim.Token) return new LeaseStep.Answer(new ClaimOutcome.Won(run.Attempt));
        if (!run.IsRunning) return new LeaseStep.Answer(new ClaimOutcome.Done());
        if (run.LeaseEnd >= now) return new LeaseStep.Answer(new ClaimOutcome.Running(run.Attempt));

        return TakeOver(job, run, claim, now);
    }

    /// <summary>Renews the lease, if the run still carries the token and its lease was not released.</summary>
    public static StoredRun? Renew(StoredRun run, string token, TimeSpan lease, long now)
    {
        // A released lease (l = 0) stays released: a heartbeat sent before the release can arrive after it.
        var renewable = run.Token == token && run.IsRunning && run.LeaseEnd != 0;
        return renewable ? run.Renewed(now + (long)lease.TotalMilliseconds) : null;
    }

    /// <summary>Ends the run, if it still carries the token.</summary>
    public static StoredRun? End(StoredRun run, string token, RunStatus status) =>
        run.Token == token ? run.Ended(status) : null;

    /// <summary>Ends a live lease now and counts a hand-over, if the run still carries the token.</summary>
    public static StoredRun? Release(StoredRun run, string token, long now)
    {
        // Only a live lease can be released; a runner whose lease already ran out counts as having died.
        var live = run.Token == token && run.IsRunning && run.LeaseEnd >= now;
        return live ? run.HandedOver() : null;
    }

    /// <summary>A run nobody claimed yet: refused when too late or overlapping a live run, else attempt 1.</summary>
    private static LeaseStep Start(JobRuns job, RunClaim claim, long now)
    {
        // A claim that arrives too late (delayed, or queued while the server was unreachable) must not
        // start a stale run, or one so old that it may have been trimmed already.
        var lateMs = now - claim.Slot;
        var maxLateMs = (long)(claim.MaxLate?.TotalMilliseconds ?? 0);
        if (maxLateMs > 0 && lateMs > maxLateMs) return new LeaseStep.Answer(new ClaimOutcome.Late(TimeSpan.FromMilliseconds(lateMs)));

        var live = LiveSlot(job, now);
        if (claim.Overlap == OverlapPolicy.Skip && live is { } slot) return new LeaseStep.Answer(new ClaimOutcome.Overlap(slot));

        var started = EndDeadRuns(job, claim.Slot, now).With(claim.Slot, StoredRun.First(claim, now));
        var keepMs = (long)(claim.Keep?.TotalMilliseconds ?? 0);
        return new LeaseStep.Write(Trim(started, now, keepMs), new ClaimOutcome.Won(1));
    }

    /// <summary>The runner stopped renewing its lease before finishing: it died, froze, or handed the run over.</summary>
    private static LeaseStep.Write TakeOver(JobRuns job, StoredRun run, RunClaim claim, long now)
    {
        // A run keeps the attempt limit it started with (manual runs: 1); hand-overs don't count.
        var limit = run.MaxAttempts ?? claim.MaxAttempts;
        if (run.Attempt - run.Released >= limit)
        {
            return new LeaseStep.Write(job.With(claim.Slot, run.Interrupted()), new ClaimOutcome.Done());
        }

        var attempt = run.Attempt + 1;
        var next = run.TakenOver(claim, attempt, limit, now);
        return new LeaseStep.Write(job.With(claim.Slot, next), new ClaimOutcome.Won(attempt));
    }

    /// <summary>The earliest run of the job that a live runner holds; null when there is none.</summary>
    private static long? LiveSlot(JobRuns job, long now)
    {
        var live = job.Runs.Where(pair => pair.Run.IsRunning && pair.Run.LeaseEnd >= now).Select(pair => pair.Slot).ToList();
        return live.Count > 0 ? live.Min() : null;
    }

    /// <summary>Older runs whose runner died stop waiting for a backup once a newer run starts.</summary>
    private static JobRuns EndDeadRuns(JobRuns job, long slot, long now) => job.WithRuns(job.Runs.Select(pair =>
    {
        var dead = pair.Run.IsRunning && pair.Run.LeaseEnd < now && pair.Slot < slot;
        return dead ? (pair.Slot, pair.Run.Interrupted()) : pair;
    }));

    /// <summary>
    /// Forgets finished runs no claim can ask about any more: a new claim later than its MaxLate is
    /// refused, and <c>keepMs</c> covers the MaxLate of every claim of the job.
    /// </summary>
    private static JobRuns Trim(JobRuns job, long now, long keepMs) =>
        job.WithRuns(job.Runs.Where(pair => pair.Run.IsRunning || now - pair.Slot <= keepMs));
}
