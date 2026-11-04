// Copyright (C) 2026-present Aristotelis — see repository license.

// A worker service on SQL Server: the scheduler's tables and the app's tables live in one database.
// "take-order" records an order every 2 s; "rollup" totals the new orders every 10 s. Each run
// writes its work in one transaction keyed by its slot, so a retry after a takeover never counts an
// order twice.
//
//   ConnectionStrings__sqlserver="Server=db;Database=app;User Id=app;Password=...;Encrypt=true" dotnet run --project examples/SqlServer
//
// Run it in 2 or 3 terminals: every run happens in one of them, and Ctrl+C in the one running
// "rollup" hands the run over to another. For the test server of this repository, start it with
// docker compose -f test/docker-compose.test.yml up -d --wait sqlserver, and use the connection
// string in the README's Tests section.

using Anarchon;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);
var connectionString = builder.Configuration.GetConnectionString("sqlserver")
    ?? throw new InvalidOperationException("Set ConnectionStrings__sqlserver to a SqlClient connection string");

builder.Services.AddSingleton(new Orders(connectionString));
builder.Services
    .AddAnarchon()
    .UseSqlServer(options =>
    {
        options.ConnectionString = connectionString;
        options.Table = "example_sqlserver_runs";
    })
    .AddJobs<OrderJobs>();

var app = builder.Build();
await app.Services.GetRequiredService<Orders>().CreateTablesAsync();
await app.RunAsync();

/// <summary>The jobs of this service. A new object, from a new scope, runs each job.</summary>
internal sealed class OrderJobs(Orders orders, ILogger<OrderJobs> logger)
{
    [Every("2s", Name = "take-order", CatchUp = "0s")]
    public async Task TakeOrderAsync(JobContext ctx)
    {
        var amount = Random.Shared.Next(5, 100);
        await orders.AddAsync(ctx.Slot, amount, ctx.CancellationToken);
    }

    [Every("10s", Name = "rollup", MaxAttempts = 3)]
    public async Task RollUpAsync(JobContext ctx)
    {
        var (count, total) = await orders.RollUpAsync(ctx.Slot, ctx.CancellationToken);
        logger.LogInformation("rollup {Due:HH:mm:ss} on {Instance} (attempt {Attempt}): {Count} orders, {Total} in total", ctx.ScheduledAt, ctx.InstanceId, ctx.Attempt, count, total);
    }
}

/// <summary>The app's own tables, next to the scheduler's.</summary>
internal sealed class Orders(string connectionString)
{
    public async Task CreateTablesAsync()
    {
        await ExecuteAsync(
            """
            IF OBJECT_ID(N'example_orders', N'U') IS NULL
              CREATE TABLE example_orders (slot BIGINT PRIMARY KEY, amount INT NOT NULL, rollup BIGINT NULL);
            IF OBJECT_ID(N'example_rollups', N'U') IS NULL
              CREATE TABLE example_rollups (slot BIGINT PRIMARY KEY, orders INT NOT NULL, total INT NOT NULL);
            """,
            [],
            CancellationToken.None);
    }

    /// <summary>One order per run: the run's slot is its key, so the same run sent twice adds nothing.</summary>
    public Task AddAsync(long slot, int amount, CancellationToken cancellationToken) =>
        ExecuteAsync(
            "IF NOT EXISTS (SELECT 1 FROM example_orders WHERE slot = @slot) INSERT INTO example_orders (slot, amount) VALUES (@slot, @amount)",
            [new("@slot", slot), new("@amount", amount)],
            cancellationToken);

    /// <summary>
    /// Marks the orders no rollup has counted and records their total, in one transaction. A retry of
    /// a rollup that committed finds its row and returns it; one that didn't commit starts over.
    /// </summary>
    public async Task<(int Count, int Total)> RollUpAsync(long slot, CancellationToken cancellationToken)
    {
        const string sql = """
            IF NOT EXISTS (SELECT 1 FROM example_rollups WHERE slot = @slot)
            BEGIN
              UPDATE example_orders SET rollup = @slot WHERE rollup IS NULL;
              INSERT INTO example_rollups (slot, orders, total)
              SELECT @slot, COUNT(*), COALESCE(SUM(amount), 0) FROM example_orders WHERE rollup = @slot;
            END
            SELECT orders, total FROM example_rollups WHERE slot = @slot;
            """;
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken);
        await using var command = new SqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("@slot", slot);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        var result = (reader.GetInt32(0), reader.GetInt32(1));
        await reader.CloseAsync();
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    private async Task ExecuteAsync(string sql, SqlParameter[] parameters, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddRange(parameters);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
