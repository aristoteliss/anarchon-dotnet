// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Diagnostics.CodeAnalysis;
using Anarchon.Backends;

namespace Anarchon.Scheduling;

/// <summary>Why a run's token was cancelled. The first cause wins.</summary>
internal enum AbortCause
{
    None,
    Stop,
    Lost,
    Timeout,
}

/// <summary>How one execution is tracked while it runs.</summary>
[SuppressMessage("Reliability", "CA1001", Justification = "The token source has no timer to free, and a late Abort() must never meet a disposed source.")]
internal sealed class Execution
{
    private readonly CancellationTokenSource _cts = new();
    private int _abortedBy;
    private readonly CancellationTokenRegistration _onStop;

    public Execution(int attempt, RunReason reason, CancellationToken parent)
    {
        Attempt = attempt;
        Reason = reason;
        Parent = parent;
        _onStop = parent.Register(static state => ((Execution)state!).Abort(AbortCause.Stop), this);
    }

    /// <summary>1 for the first try; 2 and more when this execution is a backup's takeover.</summary>
    public int Attempt { get; }

    public RunReason Reason { get; }

    /// <summary>The stop token of the lifecycle that created this run.</summary>
    public CancellationToken Parent { get; }

    /// <summary>The run's token, handed to the handler.</summary>
    public CancellationToken Token => _cts.Token;

    /// <summary>The data given to RunNowAsync() for this one run.</summary>
    public object? Data { get; init; }

    public AbortCause AbortedBy => (AbortCause)Volatile.Read(ref _abortedBy);

    /// <summary>Lease backends: marks the run ended in the store, before the result is stored.</summary>
    public Func<RunStatus, Task>? Finish { get; set; }

    /// <summary>Lease backends: gives an interrupted run to a backup now. true if it was handed over.</summary>
    public Func<Task<bool>>? Release { get; set; }

    /// <summary>Cancels the run's token and remembers why. Later calls do nothing, so the first cause wins.</summary>
    public void Abort(AbortCause cause)
    {
        if (Interlocked.CompareExchange(ref _abortedBy, (int)cause, (int)AbortCause.None) != (int)AbortCause.None) return;
        _cts.Cancel();
    }

    /// <summary>Stops listening for the scheduler's stop once the run has ended.</summary>
    public void Detach() => _onStop.Dispose();
}

/// <summary>
/// Runs one job this instance already owns: calls the handler, applies the timeout, decides how the
/// run ended, stores the result and raises the events. Claiming the run is the runners' part.
/// </summary>
internal sealed class Executor(ISchedulerCore core)
{
    private const int MaxErrorLength = 2_000;

    /// <summary>
    /// Runs the handler to its end and returns the run's record. Never throws: a handler that throws
    /// becomes a Failed or Interrupted record. <paramref name="claimed"/> is false for a manual run that
    /// was never claimed because the scheduler stopped first.
    /// </summary>
    public async Task<RunRecord> ExecuteAsync(Job job, long slot, Execution run, bool claimed = true)
    {
        var startedAt = core.Now();
        var started = !run.Token.IsCancellationRequested;
        if (!started) run.Detach();
        var failure = started ? await StartAsync(job, slot, run).ConfigureAwait(false) : Stopping();
        var status = StatusOf(run, failure);

        // After stop has returned, nothing is written any more (the lease runs out on its own).
        var stopped = !claimed || core.IsIdle || run.Parent != core.StopToken;
        var handedOver = !stopped && await EndAsync(run, status).ConfigureAwait(false);

        var finishedAt = core.Now();
        var result = new StoredResult
        {
            Slot = slot,
            By = core.InstanceId,
            Attempt = run.Attempt,
            Status = status,
            StartedAt = DateTimeOffset.FromUnixTimeMilliseconds(startedAt),
            FinishedAt = DateTimeOffset.FromUnixTimeMilliseconds(finishedAt),
            Duration = TimeSpan.FromMilliseconds(finishedAt - startedAt),
            Error = failure is null ? null : Describe(failure),
        };
        if (!stopped) await SaveAsync(job, result).ConfigureAwait(false);

        var record = ToRecord(job, run, result);
        if (started) Report(record, failure, handedOver);
        return record;
    }

    /// <summary>What a run that never started reports: the scheduler stopped before its handler was called.</summary>
    private static OperationCanceledException Stopping() => new("The scheduler is stopping");

    /// <summary>Counts the run as running here, raises Started and calls the handler. Returns what it threw.</summary>
    private Task<Exception?> StartAsync(Job job, long slot, Execution run)
    {
        job.EnterRun();
        core.OnStarted(new RunStartEvent
        {
            Job = job.Name,
            Slot = slot,
            InstanceId = core.InstanceId,
            IsManual = run.Reason == RunReason.Manual,
            Attempt = run.Attempt,
            Reason = run.Reason,
        });

        return CallHandlerAsync(job, slot, run);
    }

    /// <summary>Calls the job's handler with its timeout. Returns what it threw, or null when it returned.</summary>
    private async Task<Exception?> CallHandlerAsync(Job job, long slot, Execution run)
    {
        try
        {
            var context = new JobContext(job.Name, slot, core.InstanceId, run.Attempt, run.Reason, run.Token);
            // On the thread pool, so a handler that blocks can still time out; the data is resolved
            // inside the run, so a provider that throws or hangs fails or times out the run.
            var work = Task.Run(async () =>
            {
                context.Data = await RunData.ResolveAsync([core.SchedulerData, job.Data, run.Data], context).ConfigureAwait(false);
                await job.Handler(context).ConfigureAwait(false);
            });

            await WithinAsync(work, job.TimeoutMs, run).ConfigureAwait(false);
            return null;
        }
        catch (Exception error)
        {
            return error;
        }
        finally
        {
            run.Detach();
            job.ExitRun();
        }
    }

    /// <summary>Waits for the handler; when the timeout passes first, cancels the run and throws a <see cref="TimeoutException"/>.</summary>
    private async Task WithinAsync(Task work, long? timeoutMs, Execution run)
    {
        if (timeoutMs is not { } ms)
        {
            await work.ConfigureAwait(false);
            return;
        }

        try
        {
            await work.WaitAsync(TimeSpan.FromMilliseconds(ms), core.Time).ConfigureAwait(false);
        }
        catch (TimeoutException) when (!work.IsCompleted)
        {
            run.Abort(AbortCause.Timeout);
            _ = work.ContinueWith(static task => task.Exception, TaskScheduler.Default); // it may still fail; that is already reported
            throw new TimeoutException($"Timed out after {ms} ms");
        }
    }

    /// <summary>Stores the result. A store that fails is reported as an Error event.</summary>
    private async Task SaveAsync(Job job, StoredResult result)
    {
        try
        {
            await core.EnsureReadyAsync().ConfigureAwait(false);
            await core.Backend.SaveResultAsync(job.Name, result, job.HistorySize, job.HistoryTtl, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            core.OnError(error, job.Name, result.Slot);
        }
    }

    private RunRecord ToRecord(Job job, Execution run, StoredResult result) => new()
    {
        Job = job.Name,
        Slot = result.Slot,
        InstanceId = core.InstanceId,
        IsManual = run.Reason == RunReason.Manual,
        Attempt = run.Attempt,
        Reason = run.Reason,
        Status = result.Status,
        StartedAt = result.StartedAt,
        FinishedAt = result.FinishedAt,
        Duration = result.Duration,
        Error = result.Error,
    };

    /// <summary>Raises how the run ended.</summary>
    private void Report(RunRecord record, Exception? failure, bool handedOver)
    {
        if (failure is null) core.OnSucceeded(record);
        else if (record.Status == RunStatus.Interrupted) core.OnInterrupted(new RunInterruptedEvent(record, failure, handedOver));
        else core.OnFailed(new RunFailureEvent(record, failure));
    }

    /// <summary>A job that throws after stop or a takeover cancelled it didn't fail: it was interrupted.</summary>
    private static RunStatus StatusOf(Execution run, Exception? failure)
    {
        if (failure is null) return RunStatus.Success;
        return run.AbortedBy is AbortCause.Stop or AbortCause.Lost ? RunStatus.Interrupted : RunStatus.Failed;
    }

    /// <summary>Marks the run ended in the store, or hands an interrupted run to a backup. true if it was handed over.</summary>
    private static async Task<bool> EndAsync(Execution run, RunStatus status)
    {
        // Stopped mid-run: give the run to another instance now. Manual runs are never retried.
        var handOver = status == RunStatus.Interrupted && run.AbortedBy == AbortCause.Stop && run.Reason != RunReason.Manual;
        if (handOver && run.Release is not null) return await run.Release().ConfigureAwait(false);

        if (run.Finish is not null) await run.Finish(status).ConfigureAwait(false);
        return false;
    }

    /// <summary>The error as stored and shown in history: <c>Type: message</c>, cut at 2000 characters.</summary>
    public static string Describe(Exception error)
    {
        var text = $"{error.GetType().Name}: {error.Message}";
        return text.Length > MaxErrorLength ? $"{text[..MaxErrorLength]}…" : text;
    }
}
