// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Data;
using System.Data.Common;
using System.Diagnostics;
using AwsWrapperDataProvider.Driver.Dialects;
using AwsWrapperDataProvider.Tests.Container.Utils;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

/// <summary>
/// Checks against a real database that the caller's <see cref="CancellationToken"/> cancels opens and queries made
/// through the wrapper, and that a cancellation is never mistaken for a failed connection.
/// </summary>
public class CancellationTokenConnectivityTests : IntegrationTestBase
{
    // Long enough that only a cancellation can end the query within the time the assertions allow.
    private const int LongQuerySeconds = 10;

    private static readonly TimeSpan CancelAfter = TimeSpan.FromMilliseconds(500);

    // Comfortably above CancelAfter, and far below the 30 s connect timeout that bounds the wait when the token
    // does not reach the driver.
    private static readonly TimeSpan CancelledOpenLimit = TimeSpan.FromSeconds(5);

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task OpenAsync_PreCancelledToken_ThrowsWithoutOpening()
    {
        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, GetUrl("failover"));
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connection.OpenAsync(cts.Token));
        Assert.Equal(ConnectionState.Closed, connection.State);

        // Nothing the cancelled open did may get in the way of the next one.
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Theory]
    [InlineData("ExecuteScalarAsync")]
    [InlineData("ExecuteNonQueryAsync")]
    [InlineData("ExecuteReaderAsync")]
    [InlineData("ReadAsync")]
    [InlineData("BeginTransactionAsync")]
    [InlineData("CommitAsync")]
    [InlineData("ChangeDatabaseAsync")]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task Execute_PreCancelledToken_ThrowsAndLeavesConnectionUsable(string method)
    {
        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, GetUrl("failover"));
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        string? instanceBefore = await AuroraUtils.QueryInstanceId(connection, true);
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => InvokeAsync(connection, method, cts.Token));

        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.Equal("1", await AuroraUtils.ExecuteQuery(connection, "SELECT 1", true));
        Assert.Equal(instanceBefore, await AuroraUtils.QueryInstanceId(connection, true));
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task SetReadOnly_PreCancelledToken_DoesNotSwitchToReader()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, GetUrl("readWriteSplitting"));
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        string? writer = await AuroraUtils.QueryInstanceId(connection, true);
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        // Read/write splitting switches to a reader as soon as it sees this statement, so it must not see a call
        // that was already cancelled.
        await using (DbCommand command = connection.CreateCommand())
        {
            command.CommandText = IsMySqlFamily ? MySqlDialect.ReadOnlyQuery : PgDialect.ReadOnlyQuery;
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => command.ExecuteNonQueryAsync(cts.Token));
        }

        Assert.Equal(writer, await AuroraUtils.QueryInstanceId(connection, true));
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task RunningQuery_Cancelled_ThrowsWithoutFailingOver()
    {
        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, GetUrl("failover"));
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        string? instanceBefore = await AuroraUtils.QueryInstanceId(connection, true);
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(CancelAfter);

        OperationCanceledException exception;
        Stopwatch stopwatch = Stopwatch.StartNew();
        await using (DbCommand command = connection.CreateCommand())
        {
            command.CommandText = LongRunningQuery;
            exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => command.ExecuteScalarAsync(cts.Token));
        }

        stopwatch.Stop();

        // Only a cancel that reached the running statement carries the server's error inside the cancellation. A
        // token that fired before the statement was sent gives a bare OperationCanceledException, which would let
        // the rest of this test pass without exercising a cancel on the server at all.
        if (IsMySqlFamily)
        {
            Assert.Equal(MySqlErrorCode.QueryInterrupted, Assert.IsType<MySqlException>(exception.InnerException).ErrorCode);
        }
        else
        {
            Assert.Equal(PostgresErrorCodes.QueryCanceled, Assert.IsType<PostgresException>(exception.InnerException).SqlState);
        }

        // Ending well before the query could have finished shows that the cancellation ended it.
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(LongQuerySeconds), $"The cancelled query took {stopwatch.Elapsed}.");
        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.Equal(instanceBefore, await AuroraUtils.QueryInstanceId(connection, true));
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task RunningQuery_CancelNotAcknowledged_ThrowsCancellationInsteadOfFailingOver()
    {
        // When the server does not answer a cancel request within Cancellation Timeout, Npgsql breaks the connection
        // and throws OperationCanceledException with a TimeoutException inside. A negative timeout makes Npgsql break
        // the connection straight after sending the cancel request, which gives the same exception every time.
        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, GetUrl("failover") + "; Cancellation Timeout=-1");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(CancelAfter);

        OperationCanceledException exception;
        await using (DbCommand command = connection.CreateCommand())
        {
            command.CommandText = $"SELECT pg_sleep({LongQuerySeconds})";
            exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => command.ExecuteScalarAsync(cts.Token));
        }

        // Confirms the test exercised the unacknowledged-cancel path rather than a cancel the server answered.
        Assert.IsType<TimeoutException>(exception.InnerException);
    }

    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    [Trait("Engine", "aurora-limitless")]
    public async Task OpenAsync_CancelledWhileWaitingForPooledConnection_ThrowsOperationCanceledException()
    {
        // With a single-connection pool, a second open has to wait inside the driver for the first connection to be
        // returned. Both drivers end that wait as soon as the caller's token fires. That is not true of a physical
        // connect: the reads that wait for the server's first reply ignore the token, so a slow server can't be used
        // to hold an open in progress. No plugins, so nothing else the wrapper opens can take the one pooled
        // connection.
        string connectionString = GetUrl(string.Empty) + "; Maximum Pool Size=1";
        using AwsWrapperConnection holder = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await holder.OpenAsync(TestContext.Current.CancellationToken);

        using AwsWrapperConnection waiter = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(CancelAfter);

        Stopwatch stopwatch = Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiter.OpenAsync(cts.Token));
        stopwatch.Stop();

        // If the token never reached the driver, the open would wait out the connect timeout instead. The wrapper
        // would still report that as a cancellation, because the caller's token has fired by then, so only the
        // elapsed time tells the two apart.
        Assert.True(stopwatch.Elapsed < CancelledOpenLimit, $"The cancelled open took {stopwatch.Elapsed}.");
        Assert.Equal(ConnectionState.Closed, waiter.State);

        // The connection holding the pooled connection is unaffected.
        Assert.Equal("1", await AuroraUtils.ExecuteQuery(holder, "SELECT 1", true));
    }

    // MySQL's SLEEP() returns 1 instead of raising an error when its query is killed, so a cancelled SLEEP() looks
    // like a query that completed. A row scan this large raises ER_QUERY_INTERRUPTED when it is killed.
    private static string LongRunningQuery => IsMySqlFamily
        ? "SELECT COUNT(*) FROM information_schema.columns a CROSS JOIN information_schema.columns b CROSS JOIN information_schema.columns c"
        : $"SELECT pg_sleep({LongQuerySeconds})";

    private static string GetUrl(string plugins)
    {
        return ConnectionStringHelper.GetUrl(Engine, Endpoint, Port, Username, Password, DefaultDbName, 30, 30, plugins);
    }

    private static async Task InvokeAsync(DbConnection connection, string method, CancellationToken token)
    {
        switch (method)
        {
            case "ExecuteScalarAsync":
                await using (DbCommand command = CreateCommand(connection, "SELECT 1"))
                {
                    await command.ExecuteScalarAsync(token);
                }

                break;
            case "ExecuteNonQueryAsync":
                await using (DbCommand command = CreateCommand(connection, "SELECT 1"))
                {
                    await command.ExecuteNonQueryAsync(token);
                }

                break;
            case "ExecuteReaderAsync":
                await using (DbCommand command = CreateCommand(connection, "SELECT 1"))
                await using (await command.ExecuteReaderAsync(token))
                {
                    // Only executing the reader is under test.
                }

                break;
            case "ReadAsync":
                await using (DbCommand command = CreateCommand(connection, "SELECT 1"))
                await using (DbDataReader reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
                {
                    await reader.ReadAsync(token);
                }

                break;
            case "BeginTransactionAsync":
                await using (await connection.BeginTransactionAsync(token))
                {
                    // Only beginning the transaction is under test.
                }

                break;
            case "CommitAsync":
                await using (DbTransaction transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken))
                {
                    await transaction.CommitAsync(token);
                }

                break;
            case "ChangeDatabaseAsync":
                await connection.ChangeDatabaseAsync(DefaultDbName, token);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(method), method, null);
        }
    }

    private static DbCommand CreateCommand(DbConnection connection, string sql)
    {
        DbCommand command = connection.CreateCommand();
        command.CommandText = sql;
        return command;
    }
}
