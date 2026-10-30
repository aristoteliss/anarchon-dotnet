// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;

namespace Anarchon.Scheduling;

/// <summary>
/// The lease of one run this instance executes, renewed every heartbeat until the run's end is stored
/// or the run is released. Calls <c>onLost</c> once when the store says another instance has the run,
/// or when no renewal was confirmed within the lease while the handler runs: by then the lease may
/// have run out on the store, and a backup may be taking the run over.
/// </summary>
internal sealed class HeldLease
{
    /// <summary>The first retry of a finish the store didn't answer; each retry waits twice as long, up to a heartbeat.</summary>
    private const long FirstFinishRetryMs = 200;

    private readonly ISchedulerCore _core;
    private readonly IRunStore _runs;
    private readonly LeasedRun _leased;
    private readonly Action _onLost;
    private readonly Lock _gate = new();
    private readonly ITimer _heartbeat;
    private ITimer? _deadline;
    private ITimer? _giveUp;
    private bool _lost;
    private bool _ending;
    private bool _stopped;

    /// <summary>When the last renewal the store confirmed was sent (the claim, at first).</summary>
    private long _confirmedAt;

    public HeldLease(ISchedulerCore core, IRunStore runs, LeasedRun leased, Action onLost)
    {
        _core = core;
        _runs = runs;
        _leased = leased;
        _onLost = onLost;
        _confirmedAt = leased.ClaimedAt;

        var heartbeat = TimeSpan.FromMilliseconds(core.HeartbeatMs);
        lock (_gate)
        {
            _heartbeat = core.Time.CreateTimer(_ => Renew(), null, heartbeat, heartbeat);
            ArmDeadline();
        }
    }

    private bool IsEnding
    {
        get
        {
            lock (_gate)
            {
                return _ending;
            }
        }
    }

    public void Stop()
    {
        lock (_gate)
        {
            _stopped = true;
            _heartbeat.Dispose();
            _deadline?.Dispose();
            _giveUp?.Dispose();
        }
    }

    /// <summary>Stores how the run ended.</summary>
    public async Task FinishAsync(RunStatus status)
    {
        var (job, slot, token, _, _, _, _, stop) = _leased;
        lock (_gate)
        {
            _ending = true;
            // Keep renewing while the end is stored, also while stop waits: if the store is briefly
            // unreachable, a backup must not run a job that already completed again. But for one
            // lease at most: a finish the store never answers must not hold the job's lease for good.
            if (!_stopped) _giveUp = _core.Time.CreateTimer(_ => _heartbeat.Dispose(), null, TimeSpan.FromMilliseconds(_core.LeaseMs), Timeout.InfiniteTimeSpan);
        }

        for (var delayMs = FirstFinishRetryMs; !IsLost() && !_core.IsIdle && stop == _core.StopToken; delayMs = Math.Min(delayMs * 2, _core.HeartbeatMs))
        {
            try
            {
                var stored = await _runs.FinishAsync(job.Name, slot, token, status, CancellationToken.None).ConfigureAwait(false);
                if (!stored) MarkLost();
                break;
            }
            catch (Exception error)
            {
                _core.OnError(error, job.Name, slot);
                await Task.Delay(TimeSpan.FromMilliseconds(delayMs), _core.Time).ConfigureAwait(false);
            }
        }

        Stop();
    }

    /// <summary>Hands the run to a backup now. true if the store released it.</summary>
    public async Task<bool> ReleaseAsync()
    {
        var (job, slot, token, _, _, _, _, _) = _leased;
        lock (_gate)
        {
            _ending = true;
        }

        Stop();
        if (IsLost()) return false;

        try
        {
            return await _runs.ReleaseAsync(job.Name, slot, token, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            _core.OnError(error, job.Name, slot);
            return false; // the lease runs out on its own, then a backup takes over
        }
    }

    private void Renew() => _ = RenewAsync(_core.Now());

    private async Task RenewAsync(long sentAt)
    {
        var (job, slot, token, _, _, _, _, _) = _leased;

        try
        {
            var renewed = await _runs.HeartbeatAsync(job.Name, slot, token, TimeSpan.FromMilliseconds(_core.LeaseMs), CancellationToken.None).ConfigureAwait(false);
            if (renewed) Confirm(sentAt);
            // Once finish or release is sent, a refused heartbeat is expected: both check the token themselves.
            else if (!IsEnding) MarkLost();
        }
        catch (Exception error)
        {
            _core.OnError(error, job.Name, slot);
        }
    }

    /// <summary>Heartbeats overlap and may be answered out of order: only a later send moves the deadline.</summary>
    private void Confirm(long sentAt)
    {
        lock (_gate)
        {
            if (sentAt <= _confirmedAt) return;
            _confirmedAt = sentAt;
            ArmDeadline();
        }
    }

    /// <summary>The handler is still running when a lease passes with no confirmed renewal: the run is lost here.</summary>
    private void ArmDeadline()
    {
        if (_stopped) return;

        var remainingMs = Math.Max(0, _confirmedAt + _core.LeaseMs - _core.Now());
        var due = TimeSpan.FromMilliseconds(remainingMs);
        if (_deadline is null) _deadline = _core.Time.CreateTimer(_ => OnDeadline(), null, due, Timeout.InfiniteTimeSpan);
        else _deadline.Change(due, Timeout.InfiniteTimeSpan);
    }

    private void OnDeadline()
    {
        if (!IsEnding) MarkLost();
    }

    private bool IsLost()
    {
        lock (_gate)
        {
            return _lost;
        }
    }

    private void MarkLost()
    {
        lock (_gate)
        {
            if (_lost) return;
            _lost = true;
        }

        Stop(); // nothing left to renew
        _onLost();
    }
}
