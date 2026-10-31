# Contributing

Bug reports and questions go to the [issue tracker](https://github.com/aristoteliss/anarchon-dotnet/issues).
For a bug, use the bug report form: it asks for the versions, the backend and a minimal
reproduction. Never paste credentials or connection strings.

Before a larger change, open an issue to agree on it first.

## Setup

You need the .NET 10 SDK and Docker.

```sh
docker compose -f test/docker-compose.test.yml up -d --wait   # Redis 8.6, Redis 7.0, Valkey, PostgreSQL 16
docker compose -f test/docker-compose.test.yml --profile brokers up -d --wait   # also RabbitMQ and NATS
```

The tests run against real servers. A server that isn't running skips its tests, so name the
servers a run must have: `REQUIRE_SERVERS=redis,redis-7,valkey,postgres,rabbitmq,nats dotnet test`. The README's
[Tests](README.md#tests) section lists the servers, their addresses and the variables that
override them.

## Before a pull request

```sh
dotnet format --verify-no-changes   # formatting and code style (.editorconfig); dotnet format fixes it
dotnet build                        # warnings are errors, analyzers included
dotnet test                         # every test; read the skip count
```

- Fix a bug by first adding a test that reproduces it.
- A change to behavior or to the public API needs its documentation (XML docs, the README, and
  the pages in [anarchon-docs](https://github.com/aristoteliss/anarchon-docs), checked out at
  `docs/` as a submodule; open that pull request there) and an entry in `CHANGELOG.md`.
- A change to the public API is recorded in the project's `PublicAPI.Unshipped.txt`: the build
  fails until it is (`dotnet format analyzers --diagnostics RS0016` records additions). Review that
  diff: removing or renaming anything users or other backends rely on is a breaking change. At a
  release, the unshipped lines move to `PublicAPI.Shipped.txt`.
- A new failure gets an `AnarchonErrorCode`; a value that can't be used throws an
  `ArgumentException`.
- Comments explain non-obvious concurrency or protocol constraints only.

## Compatibility

Anarchon is extended without breaking the apps and the backends built on it. A change that breaks
one of these rules is a breaking change: it waits for a major release and goes under "Breaking" in
the changelog.

- **Public API.** Every intended change is recorded in `PublicAPI.*.txt`, and the build fails on an
  unrecorded one. Removing or renaming a type, a member or an option is breaking.
- **The backend contract** (`IBackend`, `IRunStore`) is implemented outside this repository. A new
  member has a default implementation, so existing backends keep compiling.
- **Types that backends produce and users read** (`RunRecord`, `StoredResult`, `ClaimOutcome`): a
  new member is optional. Backends build history with `RunHistory.Build()`, so new members reach
  them without changes.
- **Enums may grow:** `RunReason`, `RunStatus`, `SkipReason`, `JobType`, `AnarchonErrorCode` and
  the `ClaimOutcome` kinds may gain members in a minor release. The scheduler handles a claim
  outcome it doesn't know like `Done`.
- **Stored data.** Two versions run side by side on one store during a rolling deploy, and so do
  this package and the Node.js package. Add fields, never rename or remove them; read them with a
  default; scripts update a stored object instead of rebuilding it, so fields a newer version wrote
  survive. The Lua scripts and the stored JSON match the Node.js package: change them in both, or
  not at all. Raise `StoreFormat.Current` only for a change older versions can't read or would
  damage; that needs every instance stopped, and the changelog says so.
- **Dependencies.** Cronos decides the times of cron jobs, and every instance of a service must
  compute the same ones: a change of its major version, or of how Anarchon calls it, is a breaking
  change.

CI runs the same commands on every pull request (`.github/workflows/ci.yml`).
