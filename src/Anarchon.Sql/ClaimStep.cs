// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Sql;

/// <summary>A run's row, as a claim reads it inside its transaction.</summary>
internal sealed record RunRow(int Attempt, int MaxAttempts, int Released, string Token, string Status, long LeaseUntil);

/// <summary>
/// What a claim does with the run's row, decided on the database's clock: the rules of the Redis
/// CLAIM script, in its order.
/// </summary>
internal abstract record ClaimStep
{
    private ClaimStep()
    {
    }

    /// <summary>Answer without writing.</summary>
    public sealed record Answer(ClaimOutcome Outcome) : ClaimStep;

    /// <summary>The run has used up its attempts: end it.</summary>
    public sealed record GiveUp : ClaimStep;

    /// <summary>The runner stopped renewing its lease (it died, froze, or handed the run over): take the next attempt.</summary>
    public sealed record TakeOver(int Attempt) : ClaimStep;

    /// <summary>Nobody has claimed this run: start it.</summary>
    public sealed record Start : ClaimStep;

    /// <summary>Decides a claim from the run's row (null when nobody claimed it) and the database's clock.</summary>
    public static ClaimStep Decide(RunRow? run, RunClaim claim, long now)
    {
        if (run is null) return claim.Mode == ClaimMode.Takeover ? new Answer(new ClaimOutcome.None()) : new Start();

        var running = run.Status == "running";
        // Our own claim, sent again.
        if (running && run.Token == claim.Token) return new Answer(new ClaimOutcome.Won(run.Attempt));
        if (!running) return new Answer(new ClaimOutcome.Done());
        if (run.LeaseUntil >= now) return new Answer(new ClaimOutcome.Running(run.Attempt));

        // Hand-overs on stop don't count against the run's attempts.
        var used = run.Attempt - run.Released;
        return used >= run.MaxAttempts ? new GiveUp() : new TakeOver(run.Attempt + 1);
    }
}
