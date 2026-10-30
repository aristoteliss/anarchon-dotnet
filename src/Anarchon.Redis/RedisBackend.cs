// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Backends;
using StackExchange.Redis;

namespace Anarchon.Redis;

/// <summary>
/// Redis or Valkey, on StackExchange.Redis.
/// </summary>
/// <remarks>
/// <para>
/// <b>How each run gets exactly one runner.</b> Redis executes commands and Lua scripts one at a
/// time, so a single command or script that both checks "is this run taken?" and takes it is atomic:
/// of all the instances that send it for the same run, exactly one sees "not taken". No lock, no
/// leader. The strategies differ in which command that is (see <see cref="RedisStrategy"/>).
/// </para>
/// <para>
/// Every claim carries a random token. After the atomic step, an instance won if the stored token is
/// its own, which also makes a claim safe to send twice. Only <see cref="RedisStrategy.Lease"/> goes
/// further than "who runs it": the winner renews its lease while the job runs, so the others can
/// tell a dead runner from a slow one and take over.
/// </para>
/// <para>
/// Keys per job with StreamId, Idmp and Script: a stream <c>&lt;prefix&gt;&lt;job&gt;:runs</c> (one entry
/// per claimed run), a stream <c>&lt;prefix&gt;&lt;job&gt;:manual</c> (one entry per manual run) and a sorted
/// set <c>&lt;prefix&gt;&lt;job&gt;:results</c>. With Lease, the keys share the hash tag
/// <c>{&lt;prefix&gt;&lt;job&gt;}</c>.
/// </para>
/// </remarks>
/// <example>The Lease strategy, keys under <c>cron:</c>
/// <code>
/// var scheduler = new Scheduler(new SchedulerOptions { Backend = new RedisBackend("localhost:6379") });
/// </code>
/// </example>
/// <example>A connection your app already has; stop leaves it open
/// <code>
/// var redis = await ConnectionMultiplexer.ConnectAsync(redisUrl);
/// var backend = new RedisBackend(new RedisBackendOptions { Connection = redis, Prefix = "billing:cron:" });
/// </code>
/// </example>
public sealed class RedisBackend : IBackend
{
    private const int MaxIdmpSize = 10_000;
    private const string DefaultConfiguration = "localhost:6379";
    private static readonly Version LeaseRedis = new(5, 0);
    private static readonly TimeSpan DefaultWindow = TimeSpan.FromMinutes(5);

    private readonly string _prefix;
    private readonly string? _configuration;
    private readonly IConnectionMultiplexer? _connection;
    private readonly int _database;
    private readonly long _idmpDurationSec;
    private readonly int _idmpMaxSize;
    private readonly long _scriptWindowMs;
    private readonly LeaseStore? _lease;
    private readonly Lock _gate = new();
    private Task<IConnectionMultiplexer>? _owned;
    private Task<IClaimStrategy>? _strategy;

    /// <summary>A backend on the Lease strategy, connecting with a StackExchange.Redis configuration string.</summary>
    /// <param name="configuration">Such as <c>localhost:6379</c> or <c>cache:6380,password=...,ssl=true</c>.</param>
    /// <example>
    /// <code>
    /// new RedisBackend(builder.Configuration.GetConnectionString("redis")!);
    /// </code>
    /// </example>
    public RedisBackend(string configuration)
        : this(new RedisBackendOptions { Configuration = configuration })
    {
    }

    /// <summary>Checks the options. Nothing connects until the scheduler starts.</summary>
    /// <param name="options">Where Redis is, the strategy and the key prefix.</param>
    /// <exception cref="ArgumentException">Both a configuration and a connection are given, or a strategy's setting is given to another strategy, or out of range.</exception>
    /// <example>
    /// <code>
    /// new RedisBackend(new RedisBackendOptions { Configuration = redisUrl, Strategy = RedisStrategy.Idmp, IdmpDuration = TimeSpan.FromMinutes(15) });
    /// </code>
    /// </example>
    public RedisBackend(RedisBackendOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Configuration is not null && options.Connection is not null)
        {
            throw new ArgumentException("Give RedisBackend a Configuration or a Connection, not both", nameof(options));
        }

        RefuseOtherStrategySettings(options);
        _idmpDurationSec = (long)(options.IdmpDuration ?? DefaultWindow).TotalSeconds;
        _idmpMaxSize = options.IdmpMaxSize ?? 1_000;
        _scriptWindowMs = (long)(options.ScriptWindow ?? DefaultWindow).TotalMilliseconds;
        RefuseOutOfRange(options);

        Strategy = options.Strategy;
        _prefix = options.Prefix ?? throw new ArgumentException("Prefix must not be null", nameof(options));
        _configuration = options.Configuration;
        _connection = options.Connection;
        _database = options.Database;
        if (Strategy == RedisStrategy.Lease) _lease = new LeaseStore(DatabaseAsync, _prefix);
    }

    /// <summary><c>redis:</c> and the strategy, such as <c>redis:lease</c>.</summary>
    public string Name => $"redis:{StrategyName}";

    /// <summary>The strategy this backend claims runs with.</summary>
    public RedisStrategy Strategy { get; }

    /// <summary>Forever for Lease and StreamId; <see cref="RedisBackendOptions.IdmpDuration"/> for Idmp; <see cref="RedisBackendOptions.ScriptWindow"/> for Script.</summary>
    public TimeSpan DedupeWindow => Strategy switch
    {
        RedisStrategy.Idmp => TimeSpan.FromSeconds(_idmpDurationSec),
        RedisStrategy.Script => TimeSpan.FromMilliseconds(_scriptWindowMs),
        _ => TimeSpan.MaxValue,
    };

    /// <summary>The run store, with the Lease strategy only.</summary>
    public IRunStore? Runs => _lease;

    private string StrategyName => Strategy switch
    {
        RedisStrategy.StreamId => "stream-id",
        RedisStrategy.Idmp => "idmp",
        RedisStrategy.Script => "script",
        _ => "lease",
    };

    /// <summary>Idmp and Script remember a limited number of runs: refuses a job whose late claims could outlive that memory.</summary>
    /// <param name="job">The job's settings.</param>
    public void CheckJob(JobLimits job)
    {
        ArgumentNullException.ThrowIfNull(job);
        var neededMs = (long)(job.Jitter + job.MaxLate).TotalMilliseconds;
        var minGapMs = job.MinGap == TimeSpan.MaxValue ? long.MaxValue : (long)job.MinGap.TotalMilliseconds;

        // The server must still remember a run while late instances may try to claim it. (Lease needs
        // no such check: it never forgets a run that a late claim could still ask for.)
        if (Strategy == RedisStrategy.Idmp && Covers(_idmpMaxSize, minGapMs) <= neededMs)
        {
            throw Unsafe(job, $"IdmpMaxSize ({_idmpMaxSize}) runs × {minGapMs} ms between runs must cover Jitter + MaxLate ({neededMs} ms)");
        }

        if (Strategy == RedisStrategy.Script && job.HistoryTtl is { } ttl && ttl.TotalMilliseconds <= _scriptWindowMs)
        {
            throw Unsafe(job, $"HistoryTtl must be longer than the Script strategy's window ({_scriptWindowMs} ms)");
        }

        if (Strategy == RedisStrategy.Script && Covers(job.HistorySize, minGapMs) <= neededMs)
        {
            throw Unsafe(job, $"HistorySize ({job.HistorySize}) runs × {minGapMs} ms between runs must cover Jitter + MaxLate ({neededMs} ms) for the Script strategy");
        }
    }

    /// <summary>Connects, checks the server against the strategy, and checks the keys' store format.</summary>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the backend is ready.</returns>
    public async Task InitAsync(CancellationToken cancellationToken)
    {
        var db = await DatabaseAsync().ConfigureAwait(false);
        // A server the strategy can't run on is refused before anything is written to it.
        if (_lease is not null) await RefuseOldRedisAsync(db).ConfigureAwait(false);
        else await GetStrategyAsync().ConfigureAwait(false);

        var marker = $"{_prefix}format";
        await db.StringSetAsync(marker, StoreFormat.Current, null, When.NotExists).ConfigureAwait(false);
        var format = await db.StringGetAsync(marker).ConfigureAwait(false);
        StoreFormat.Check(format.ToString(), $"The keys under \"{_prefix}\"");
    }

    /// <summary>Idmp sets up the job's stream here; the other strategies need nothing.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="historySize">Not used.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the job is set up.</returns>
    public async Task PrepareJobAsync(string job, int historySize, CancellationToken cancellationToken)
    {
        if (_lease is not null) return;

        var strategy = await GetStrategyAsync().ConfigureAwait(false);
        var db = await DatabaseAsync().ConfigureAwait(false);
        await strategy.PrepareStreamAsync(db, StreamKey(job)).ConfigureAwait(false);
    }

    /// <summary>The plain one-winner claim. With Lease the scheduler claims through <see cref="Runs"/> instead.</summary>
    /// <param name="input">The run and who claims it.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>Whether this claim won the run.</returns>
    public async Task<bool> ClaimAsync(ClaimInput input, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        if (_lease is not null) return await _lease.ClaimWithoutBackupsAsync(input, cancellationToken).ConfigureAwait(false);

        var strategy = await GetStrategyAsync().ConfigureAwait(false);
        var db = await DatabaseAsync().ConfigureAwait(false);
        return await strategy.ClaimAsync(db, StreamKey(input.Job), input).ConfigureAwait(false);
    }

    /// <summary>
    /// Records a manual run. Lease: claims a run of its own at the first free millisecond and returns
    /// that slot. Other strategies: appends an entry to the job's stream of manual runs, which claims
    /// never read.
    /// </summary>
    /// <param name="input">The manual run.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The slot used with Lease; null otherwise.</returns>
    public async Task<long?> RecordManualRunAsync(ClaimInput input, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        if (_lease is not null) return await _lease.ClaimManualRunAsync(input, cancellationToken).ConfigureAwait(false);

        // A server-clock ID in the claim stream would refuse StreamId claims of a run already due.
        var db = await DatabaseAsync().ConfigureAwait(false);
        await db.StreamAddAsync(ManualKey(input.Job), StoredData.ClaimFields(input, manual: true), null, input.HistorySize, useApproximateMaxLength: true).ConfigureAwait(false);
        return null;
    }

    /// <summary>Adds the result to the job's sorted set and trims it by count and age.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="result">How the attempt ended.</param>
    /// <param name="historySize">About how many runs to keep.</param>
    /// <param name="historyTtl">Drop records older than this.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes once the result is stored.</returns>
    public async Task SaveResultAsync(string job, StoredResult result, int historySize, TimeSpan? historyTtl, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(result);
        if (_lease is not null)
        {
            await _lease.SaveResultAsync(job, result, historySize, historyTtl).ConfigureAwait(false);
            return;
        }

        var db = await DatabaseAsync().ConfigureAwait(false);
        var finishedAt = result.FinishedAt.ToUnixTimeMilliseconds();
        await db.SortedSetAddAsync(ResultsKey(job), StoredData.ToJson(result), Math.Max(result.Slot, finishedAt)).ConfigureAwait(false);
        await db.SortedSetRemoveRangeByRankAsync(ResultsKey(job), 0, -(historySize + 1L)).ConfigureAwait(false);
        if (historyTtl is { } ttl) await TrimByAgeAsync(db, job, result, (long)ttl.TotalMilliseconds).ConfigureAwait(false);
    }

    /// <summary>Joins the claims and manual runs in the streams with the results in the sorted set.</summary>
    /// <param name="job">The job's name.</param>
    /// <param name="limit">How many attempts to return at most.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>The attempts, newest first.</returns>
    public async Task<IReadOnlyList<RunRecord>> HistoryAsync(string job, int limit, CancellationToken cancellationToken)
    {
        if (_lease is not null) return await _lease.HistoryAsync(job, limit).ConfigureAwait(false);

        var db = await DatabaseAsync().ConfigureAwait(false);
        var runs = await db.StreamRangeAsync(StreamKey(job), null, null, limit, Order.Descending).ConfigureAwait(false);
        var manual = await db.StreamRangeAsync(ManualKey(job), null, null, limit, Order.Descending).ConfigureAwait(false);
        var claims = runs.Concat(manual).Select(StoredData.ToClaim).OfType<ClaimRecord>().ToList();
        if (claims.Count == 0) return [];

        var oldest = claims.Min(claim => claim.Slot);
        var members = await db.SortedSetRangeByScoreAsync(ResultsKey(job), oldest).ConfigureAwait(false);
        return RunHistory.Build(job, claims, StoredData.ToResults(members), limit);
    }

    /// <summary>Closes the connection if this backend opened it. A connection that was passed in stays open.</summary>
    /// <returns>A task that completes once the connection is closed.</returns>
    public async Task CloseAsync()
    {
        Task<IConnectionMultiplexer>? owned;
        lock (_gate)
        {
            owned = _owned;
            _owned = null;
            _strategy = null;
        }

        if (owned is null) return;

        try
        {
            var connection = await owned.ConfigureAwait(false);
            await connection.CloseAsync().ConfigureAwait(false);
            await connection.DisposeAsync().ConfigureAwait(false);
        }
        catch (RedisConnectionException)
        {
            // It never connected: nothing to close.
        }
    }

    private Task<IDatabase> DatabaseAsync() => DatabaseOfAsync();

    private async Task<IDatabase> DatabaseOfAsync()
    {
        var connection = _connection ?? await ConnectAsync().ConfigureAwait(false);
        return connection.GetDatabase(_database);
    }

    /// <summary>Connects once; a failed connection is tried again on the next call.</summary>
    private Task<IConnectionMultiplexer> ConnectAsync()
    {
        lock (_gate)
        {
            if (_owned is null || _owned.IsFaulted || _owned.IsCanceled) _owned = Connect(_configuration ?? DefaultConfiguration);
            return _owned;
        }
    }

    private static async Task<IConnectionMultiplexer> Connect(string configuration) =>
        await ConnectionMultiplexer.ConnectAsync(configuration).ConfigureAwait(false);

    /// <summary>Builds the strategy once per connection, checking that the server supports it.</summary>
    private Task<IClaimStrategy> GetStrategyAsync()
    {
        lock (_gate)
        {
            if (_strategy is null || _strategy.IsFaulted || _strategy.IsCanceled) _strategy = CreateStrategyAsync();
            return _strategy;
        }
    }

    private async Task<IClaimStrategy> CreateStrategyAsync()
    {
        IClaimStrategy strategy = Strategy switch
        {
            RedisStrategy.Idmp => new IdmpStrategy(_idmpDurationSec, _idmpMaxSize),
            RedisStrategy.Script => new ScriptStrategy(_scriptWindowMs),
            _ => new StreamIdStrategy(),
        };

        var db = await DatabaseAsync().ConfigureAwait(false);
        await strategy.InitAsync(db).ConfigureAwait(false);
        return strategy;
    }

    /// <summary>Drops results and claims older than the age limit (XTRIM MINID needs Redis 6.2+; older servers keep the count limit only).</summary>
    private async Task TrimByAgeAsync(IDatabase db, string job, StoredResult result, long ttlMs)
    {
        var finishedAt = result.FinishedAt.ToUnixTimeMilliseconds();
        await db.SortedSetRemoveRangeByScoreAsync(ResultsKey(job), double.NegativeInfinity, finishedAt - ttlMs, Exclude.Stop).ConfigureAwait(false);

        var oldestClaim = Math.Min(result.Slot, result.StartedAt.ToUnixTimeMilliseconds()) - ttlMs;
        foreach (var key in new[] { StreamKey(job), ManualKey(job) })
        {
            try
            {
                await db.ExecuteAsync("XTRIM", key, "MINID", oldestClaim).ConfigureAwait(false);
            }
            catch (RedisServerException)
            {
                // Redis before 6.2: the count limit still applies.
            }
        }
    }

    private RedisKey StreamKey(string job) => $"{_prefix}{job}:runs";

    private RedisKey ManualKey(string job) => $"{_prefix}{job}:manual";

    private RedisKey ResultsKey(string job) => $"{_prefix}{job}:results";

    /// <summary>Refuses a server older than Redis 5 for the Lease strategy (streams arrived in Redis 5).</summary>
    private static async Task RefuseOldRedisAsync(IDatabase db)
    {
        var version = await ServerInfo.ReadRedisVersionAsync(db).ConfigureAwait(false);
        if (version >= LeaseRedis) return;

        throw new AnarchonException(AnarchonErrorCode.Server, $"The Lease strategy needs Redis 5 or newer (this server is {version})");
    }

    private static void RefuseOtherStrategySettings(RedisBackendOptions options)
    {
        var strategy = options.Strategy;
        string? misplaced = null;
        if (options.ScriptWindow is not null && strategy != RedisStrategy.Script) misplaced = nameof(options.ScriptWindow);
        if (options.IdmpDuration is not null && strategy != RedisStrategy.Idmp) misplaced = nameof(options.IdmpDuration);
        if (options.IdmpMaxSize is not null && strategy != RedisStrategy.Idmp) misplaced = nameof(options.IdmpMaxSize);
        if (misplaced is null) return;

        throw new ArgumentException($"The {strategy} strategy has no setting {misplaced}", nameof(options));
    }

    private void RefuseOutOfRange(RedisBackendOptions options)
    {
        if (_idmpDurationSec is < 1 or > 86_400) throw new ArgumentOutOfRangeException(nameof(options), $"IdmpDuration must be from 1 second to 1 day (got {options.IdmpDuration})");
        if (_idmpMaxSize is < 1 or > MaxIdmpSize) throw new ArgumentOutOfRangeException(nameof(options), $"IdmpMaxSize must be from 1 to {MaxIdmpSize} (got {_idmpMaxSize})");
        if (_scriptWindowMs < 1) throw new ArgumentOutOfRangeException(nameof(options), $"ScriptWindow must be at least 1 ms (got {options.ScriptWindow})");
        if (!Enum.IsDefined(options.Strategy)) throw new ArgumentOutOfRangeException(nameof(options), $"Unknown strategy {options.Strategy}");
    }

    private static long Covers(long runs, long gapMs) => gapMs > long.MaxValue / Math.Max(runs, 1) ? long.MaxValue : runs * gapMs;

    private static AnarchonException Unsafe(JobLimits job, string reason) => new(AnarchonErrorCode.UnsafeJob, $"Job \"{job.Name}\": {reason}");
}
