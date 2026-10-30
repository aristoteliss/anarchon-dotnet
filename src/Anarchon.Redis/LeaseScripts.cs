// Copyright (C) 2026-present Aristotelis — see repository license.

namespace Anarchon.Redis;

/// <summary>
/// The Lua scripts of the Lease strategy. They are the stored data's protocol: the same text runs in
/// every Anarchon that shares the keys, so change them only as the compatibility rules allow.
/// </summary>
/// <remarks>
/// Every decision about a run is one script, and Redis runs a script to the end before anything
/// else, so two instances can never both act on the same state. Lease ends use the Redis server's
/// clock, so clock differences between instances don't matter. The token in each run fences an old
/// runner out: after a takeover nothing it sends changes the run.
/// </remarks>
internal static class LeaseScripts
{
    private const string Now = """
        if redis.replicate_commands then pcall(redis.replicate_commands) end
        local t = redis.call('TIME')
        local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

        """;

    /// <summary>
    /// KEYS: state, active, slots, runs.
    /// ARGV: slot, token, by, leaseMs, overlap, maxAttempts, keep, mode, reason, ttlMs, maxLateMs, keepMs.
    /// </summary>
    public const string Claim = Now + """
        local state, active, slots, runs = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
        local slot, token, by = ARGV[1], ARGV[2], ARGV[3]
        local leaseMs, overlap, maxAttempts = tonumber(ARGV[4]), ARGV[5], tonumber(ARGV[6])
        local keep, mode, reason = tonumber(ARGV[7]), ARGV[8], ARGV[9]
        local ttl, maxLate, keepMs = tonumber(ARGV[10]) or 0, tonumber(ARGV[11]) or 0, tonumber(ARGV[12]) or 0

        local attempt, limit, released = 1, maxAttempts, 0
        -- A run starts from what is stored, so fields a newer version wrote survive a takeover by this one.
        local stored = {}
        local raw = redis.call('HGET', state, slot)
        if raw then
          local run = cjson.decode(raw)
          stored = run
          if run.t == token and run.s == 'running' then return {'won', run.a} end -- our own claim, sent again
          if run.s ~= 'running' then return {'done', run.a} end
          if run.l >= now then return {'running', run.a} end
          -- The runner stopped renewing its lease before finishing: it died, froze, or handed the run over.
          -- A run keeps the attempt limit it was started with (manual runs: 1); hand-overs don't count.
          limit, released = run.m or maxAttempts, run.r or 0
          if run.a - released >= limit then
            run.s = 'interrupted'
            redis.call('HSET', state, slot, cjson.encode(run))
            redis.call('SREM', active, slot)
            return {'done', run.a}
          end
          attempt = run.a + 1
        elseif mode == 'takeover' then
          return {'none', 0}
        else
          -- A claim that arrives too late (delayed, or queued while the server was unreachable) must not
          -- start a stale run, or one so old that it may have been trimmed already.
          local late = now - tonumber(slot)
          if maxLate > 0 and late > maxLate then return {'late', late} end
          for _, other in ipairs(redis.call('SMEMBERS', active)) do
            local o = redis.call('HGET', state, other)
            local run = o and cjson.decode(o)
            if not run or run.s ~= 'running' then
              redis.call('SREM', active, other)
            elseif run.l >= now then
              if overlap == 'skip' then return {'overlap', other} end
            elseif tonumber(other) < tonumber(slot) then
              -- An older run died and a newer one is starting: stop trying to finish the old one.
              run.s = 'interrupted'
              redis.call('HSET', state, other, cjson.encode(run))
              redis.call('SREM', active, other)
            end
          end
        end

        local run = stored
        run.a, run.m, run.t, run.s, run.i, run.l = attempt, limit, token, 'running', by, now + leaseMs
        if released > 0 then run.r = released end
        redis.call('HSET', state, slot, cjson.encode(run))
        redis.call('SADD', active, slot)
        redis.call('ZADD', slots, slot, slot)
        -- Keep the newest runs only, but never forget a run that is still in progress, or one young
        -- enough that a late claim could still ask for it.
        local extra = redis.call('ZCARD', slots) - keep
        if extra > 0 then
          for _, old in ipairs(redis.call('ZRANGE', slots, 0, extra - 1)) do
            if redis.call('SISMEMBER', active, old) == 0 and now - tonumber(old) > keepMs then
              redis.call('HDEL', state, old)
              redis.call('ZREM', slots, old)
            end
          end
        end
        redis.call('XADD', runs, 'MAXLEN', '~', keep, '*', 'slot', slot, 'attempt', attempt, 'by', by, 'token', token, 'at', now, 'reason', reason)
        -- History older than ttl goes too (XTRIM MINID needs Redis 6.2+; older servers keep the count limit only).
        if ttl > 0 then pcall(redis.call, 'XTRIM', runs, 'MINID', now - ttl) end
        return {'won', attempt}

        """;

    /// <summary>KEYS: state. ARGV: slot, token, leaseMs.</summary>
    public const string Heartbeat = Now + """
        local raw = redis.call('HGET', KEYS[1], ARGV[1])
        if not raw then return 0 end
        local run = cjson.decode(raw)
        -- A released lease (l = 0) stays released: a heartbeat sent before the release can arrive after it.
        if run.t ~= ARGV[2] or run.s ~= 'running' or run.l == 0 then return 0 end
        run.l = now + tonumber(ARGV[3])
        redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(run))
        return 1

        """;

    /// <summary>
    /// KEYS: state. ARGV: slot, token. Ends a live lease now, so a backup can take the run over. (A
    /// lease that already ran out can't be released: that runner counts as having died.)
    /// </summary>
    public const string Release = Now + """
        local raw = redis.call('HGET', KEYS[1], ARGV[1])
        if not raw then return 0 end
        local run = cjson.decode(raw)
        if run.t ~= ARGV[2] or run.s ~= 'running' or run.l < now then return 0 end
        run.l = 0
        run.r = (run.r or 0) + 1
        redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(run))
        return 1

        """;

    /// <summary>KEYS: state, active. ARGV: slot, token, status.</summary>
    public const string Finish = """

        local raw = redis.call('HGET', KEYS[1], ARGV[1])
        if not raw then return 0 end
        local run = cjson.decode(raw)
        if run.t ~= ARGV[2] then return 0 end
        run.s = ARGV[3]
        run.l = 0
        redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(run))
        redis.call('SREM', KEYS[2], ARGV[1])
        return 1

        """;

    /// <summary>KEYS: state, active. Returns {slot, attempt, alive} for every unfinished run.</summary>
    public const string Active = Now + """
        local out = {}
        for _, slot in ipairs(redis.call('SMEMBERS', KEYS[2])) do
          local raw = redis.call('HGET', KEYS[1], slot)
          if raw then
            local run = cjson.decode(raw)
            if run.s == 'running' then table.insert(out, { slot, run.a, run.l >= now and 1 or 0 }) end
          end
        end
        return out

        """;

    /// <summary>KEYS: state. ARGV: slots... Returns the server time, then each slot's state (or an empty string).</summary>
    public const string States = Now + """
        local out = { now }
        for i = 1, #ARGV do out[i + 1] = redis.call('HGET', KEYS[1], ARGV[i]) or '' end
        return out

        """;
}
