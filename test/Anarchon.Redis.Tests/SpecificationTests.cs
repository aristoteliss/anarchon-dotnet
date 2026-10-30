// Copyright (C) 2026-present Aristotelis — see repository license.

using Anarchon.Specification;

namespace Anarchon.Redis.Tests;

// The lease rules on every server; schedulers racing on Redis 8.6.

public sealed class RedisLeaseRulesTests() : LeaseRulesTests(new RedisStore(Servers.Redis));

public sealed class Redis7LeaseRulesTests() : LeaseRulesTests(new RedisStore(Servers.Redis7));

public sealed class ValkeyLeaseRulesTests() : LeaseRulesTests(new RedisStore(Servers.Valkey));

public sealed class RedisSchedulerLeaseTests() : SchedulerLeaseTests(new RedisStore(Servers.Redis));
