// Copyright (C) 2026-present Aristotelis — see repository license.

using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace Anarchon.Sql;

/// <summary>Opens a connection to the backend's database, once its tables are ready.</summary>
internal delegate Task<DbConnection> OpenConnection(CancellationToken cancellationToken);

/// <summary>
/// Statements on one open connection, inside a transaction or not. Parameters are named
/// (<c>@job</c>); table names come from <see cref="SqlTables"/>, which only holds validated identifiers.
/// </summary>
internal sealed class SqlSession(DbConnection connection, DbTransaction? transaction, CancellationToken cancellationToken)
{
    /// <summary>Opens a connection, runs <paramref name="work"/> on it outside a transaction, and closes it.</summary>
    public static async Task<T> RunAsync<T>(OpenConnection open, Func<SqlSession, Task<T>> work, CancellationToken cancellationToken)
    {
        var connection = await open(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            return await work(new SqlSession(connection, null, cancellationToken)).ConfigureAwait(false);
        }
    }

    /// <summary>Opens a connection, runs <paramref name="work"/> on it outside a transaction, and closes it.</summary>
    public static async Task RunAsync(OpenConnection open, Func<SqlSession, Task> work, CancellationToken cancellationToken)
    {
        var connection = await open(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            await work(new SqlSession(connection, null, cancellationToken)).ConfigureAwait(false);
        }
    }

    /// <summary>Runs a statement and returns how many rows it changed.</summary>
    public async Task<int> ExecuteAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        var command = Command(sql, parameters);
        await using (command.ConfigureAwait(false))
        {
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Runs a statement and returns its rows.</summary>
    public async Task<List<SqlRow>> QueryAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        var command = Command(sql, parameters);
        await using (command.ConfigureAwait(false))
        {
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                return await ReadRowsAsync(reader).ConfigureAwait(false);
            }
        }
    }

    /// <summary>The first row, or null when there is none.</summary>
    public async Task<SqlRow?> QueryFirstAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        var rows = await QueryAsync(sql, parameters).ConfigureAwait(false);
        return rows.Count > 0 ? rows[0] : null;
    }

    [SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Table names are validated identifiers; every value is a parameter.")]
    private DbCommand Command(string sql, (string Name, object? Value)[] parameters)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        foreach (var (name, value) in parameters)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = $"@{name}";
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);
        }

        return command;
    }

    private async Task<List<SqlRow>> ReadRowsAsync(DbDataReader reader)
    {
        var rows = new List<SqlRow>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var values = new Dictionary<string, object?>(reader.FieldCount, StringComparer.Ordinal);
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(new SqlRow(values));
        }

        return rows;
    }
}

/// <summary>
/// One result row by column name. Drivers return integers in different types (Postgres an
/// <c>int</c> or a <c>long</c> by column, SQLite always a <c>long</c>), so every read converts.
/// </summary>
internal sealed class SqlRow(Dictionary<string, object?> values)
{
    public bool IsNull(string column) => values[column] is null;

    public long Long(string column) => Convert.ToInt64(values[column] ?? 0L, CultureInfo.InvariantCulture);

    public int Int(string column) => Convert.ToInt32(values[column] ?? 0, CultureInfo.InvariantCulture);

    public string? Text(string column) => values[column] is { } value ? Convert.ToString(value, CultureInfo.InvariantCulture) : null;
}
