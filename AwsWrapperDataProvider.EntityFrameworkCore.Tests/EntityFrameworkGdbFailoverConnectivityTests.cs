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
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.EntityFrameworkCore;

namespace AwsWrapperDataProvider.EntityFrameworkCore.Tests;

/// <summary>
/// Entity Framework integration tests for the <c>gdbFailover</c> plugin.
/// Mirrors the scenarios covered in <c>GdbFailoverTests</c> at the connection level
/// and the EF lifecycle patterns from <c>EntityFrameworkConnectivityTests</c>.
///
/// These tests exercise writer/reader failover modes through the EF Core pipeline
/// using a proxied Aurora topology so a writer crash can be simulated reliably.
/// </summary>
public class EntityFrameworkGdbFailoverConnectivityTests : EFIntegrationTestBase
{
    protected override bool MakeSureFirstInstanceWriter => true;

    public EntityFrameworkGdbFailoverConnectivityTests(ITestOutputHelper output) : base(output)
    {
    }

    private static string GetGdbCrashWrapperConnectionString(string connectionString, string mode)
    {
        return connectionString
            + $";Plugins={PluginCodes.GdbFailover};"
            + $"ActiveHomeFailoverMode={mode};"
            + $"InactiveHomeFailoverMode={mode};"
            + $"ClusterInstanceHostPattern=?.{ProxyDatabaseInfo!.InstanceEndpointSuffix}:{ProxyDatabaseInfo!.InstanceEndpointPort}";
    }

    /// <summary>
    /// EF writer failover using the gdbFailover plugin with strict-writer mode.
    /// The writer is crashed between EF context operations. The next EF query on
    /// an explicitly opened connection must raise <see cref="FailoverSuccessException"/>,
    /// the failed entity remains detached, and subsequent writes succeed on the
    /// newly elected writer.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EFGdbWriterFailover_StrictWriter_FailOnExecute()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var initialWriterInstanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.GetInstance(currentWriter);

        var connectionString = ConnectionStringHelper.GetUrl(
                Engine,
                initialWriterInstanceInfo.Host,
                initialWriterInstanceInfo.Port,
                Username,
                Password,
                ProxyDatabaseInfo!.DefaultDbName,
                2,
                10);
        var wrapperConnectionString = GetGdbCrashWrapperConnectionString(connectionString, "strict-writer");
        var options = this.BuildOptionsWithLogger(wrapperConnectionString, connectionString);

        using (var db = new PersonDbContext(options))
        {
            db.Database.ExecuteSqlRaw($"Truncate table persons;");
        }

        using (var db = new PersonDbContext(options))
        {
            Person jane = new() { FirstName = "Jane", LastName = "Smith" };
            db.Add(jane);
            db.SaveChanges();

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            var instanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.Logger.WriteLine($"==========================================");
            this.Logger.WriteLine($"Current node before crash is {instanceId}");
            await db.Database.CloseConnectionAsync();

            Person john = new() { FirstName = "John", LastName = "Smith" };
            await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
            {
                var connection = db.Database.GetDbConnection();
                try
                {
                    if (connection.State == System.Data.ConnectionState.Closed)
                    {
                        connection.Open();
                    }

                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var crashInstanceTask = AuroraUtils.CrashInstance(currentWriter, tcs);
                    await tcs.Task;

                    // Query to trigger failover.
                    var anyUser = await db.Persons.AnyAsync(cancellationToken: TestContext.Current.CancellationToken);

                    db.Add(john);
                    db.SaveChanges();
                    await crashInstanceTask;
                }
                finally
                {
                    connection.Close();
                }
            });

            Assert.Equal(EntityState.Detached, db.Entry(john).State);

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            var postInstanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.Logger.WriteLine($"==========================================");
            this.Logger.WriteLine($"Current node after crash is {postInstanceId}");
            Assert.NotNull(postInstanceId);
            await db.Database.CloseConnectionAsync();

            Person joe = new() { FirstName = "Joe", LastName = "Smith" };
            db.Add(joe);
            db.SaveChanges();
        }

        using (var db = new PersonDbContext(options))
        {
            Assert.True(db.Persons.Any(p => p.FirstName == "Jane"));
            Assert.True(db.Persons.Any(p => p.FirstName == "Joe"));
            Assert.False(db.Persons.Any(p => p.FirstName == "John"));
            Assert.Equal(2, db.Persons.Count());
        }
    }

    /// <summary>
    /// Async counterpart of <see cref="EFGdbWriterFailover_StrictWriter_FailOnExecute"/>.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EFGdbWriterFailover_StrictWriter_FailOnExecuteAsync()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var initialWriterInstanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.GetInstance(currentWriter);

        var connectionString = ConnectionStringHelper.GetUrl(
                Engine,
                initialWriterInstanceInfo.Host,
                initialWriterInstanceInfo.Port,
                Username,
                Password,
                ProxyDatabaseInfo!.DefaultDbName,
                2,
                10);
        var wrapperConnectionString = GetGdbCrashWrapperConnectionString(connectionString, "strict-writer");
        var options = this.BuildOptionsWithLogger(wrapperConnectionString, connectionString);

        using (var db = new PersonDbContext(options))
        {
            await db.Database.ExecuteSqlRawAsync($"Truncate table persons;", TestContext.Current.CancellationToken);
        }

        using (var db = new PersonDbContext(options))
        {
            Person jane = new() { FirstName = "Jane", LastName = "Smith" };
            await db.AddAsync(jane, TestContext.Current.CancellationToken);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            var instanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.Logger.WriteLine($"==========================================");
            this.Logger.WriteLine($"Current node before crash is {instanceId}");
            await db.Database.CloseConnectionAsync();

            Person john = new() { FirstName = "John", LastName = "Smith" };
            await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
            {
                var connection = db.Database.GetDbConnection();
                try
                {
                    if (connection.State == System.Data.ConnectionState.Closed)
                    {
                        await connection.OpenAsync(TestContext.Current.CancellationToken);
                    }

                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var crashInstanceTask = AuroraUtils.CrashInstance(currentWriter, tcs);
                    await tcs.Task;

                    // Query to trigger failover.
                    var anyUser = await db.Persons.AnyAsync(cancellationToken: TestContext.Current.CancellationToken);

                    await db.AddAsync(john, TestContext.Current.CancellationToken);
                    await db.SaveChangesAsync(TestContext.Current.CancellationToken);
                    await crashInstanceTask;
                }
                finally
                {
                    await connection.CloseAsync();
                }
            });

            Assert.Equal(EntityState.Detached, db.Entry(john).State);

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            var postInstanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.Logger.WriteLine($"==========================================");
            this.Logger.WriteLine($"Current node after crash is {postInstanceId}");
            Assert.NotNull(postInstanceId);
            await db.Database.CloseConnectionAsync();

            Person joe = new() { FirstName = "Joe", LastName = "Smith" };
            await db.AddAsync(joe, TestContext.Current.CancellationToken);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using (var db = new PersonDbContext(options))
        {
            Assert.True(await db.Persons.AnyAsync(p => p.FirstName == "Jane", TestContext.Current.CancellationToken));
            Assert.True(await db.Persons.AnyAsync(p => p.FirstName == "Joe", TestContext.Current.CancellationToken));
            Assert.False(await db.Persons.AnyAsync(p => p.FirstName == "John", TestContext.Current.CancellationToken));
            Assert.Equal(2, await db.Persons.CountAsync(TestContext.Current.CancellationToken));
        }
    }

    /// <summary>
    /// EF reader failover using the gdbFailover plugin with strict-home-reader mode.
    /// After the writer is crashed, the next EF query must raise
    /// <see cref="FailoverSuccessException"/> and the new connection must land on
    /// a reader instance.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EFGdbReaderFailover_StrictHomeReader(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var initialWriterInstanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.GetInstance(currentWriter);

        var connectionString = ConnectionStringHelper.GetUrl(
                Engine,
                initialWriterInstanceInfo.Host,
                initialWriterInstanceInfo.Port,
                Username,
                Password,
                ProxyDatabaseInfo!.DefaultDbName,
                2,
                10);
        var wrapperConnectionString = GetGdbCrashWrapperConnectionString(connectionString, "strict-home-reader");
        var options = this.BuildOptionsWithLogger(wrapperConnectionString, connectionString);

        using (var db = new PersonDbContext(options))
        {
            if (async)
            {
                await db.Database.ExecuteSqlRawAsync($"Truncate table persons;", TestContext.Current.CancellationToken);
            }
            else
            {
                db.Database.ExecuteSqlRaw($"Truncate table persons;");
            }
        }

        using var context = new PersonDbContext(options);
        var connection = context.Database.GetDbConnection();

        if (async)
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
        }
        else
        {
            connection.Open();
        }

        Assert.Equal(ConnectionState.Open, connection.State);

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var crashTask = AuroraUtils.CrashInstance(currentWriter, tcs);

        // Wait for simulation to start
        await tcs.Task;

        await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
        {
            this.Logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
            await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, async);
        });

        // Assert that we are currently connected to a reader instance.
        var currentConnectionId = await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, async);
        Assert.NotNull(currentConnectionId);

        // RDS API lags behind the writer election after a cluster failover, so we retry the check.
        Assert.True(await AuroraUtils.WaitUntilInstanceHasRoleAsync(currentConnectionId, false, TimeSpan.FromMinutes(5)));

        await crashTask;

        if (async)
        {
            await connection.CloseAsync();
        }
        else
        {
            connection.Close();
        }
    }

    /// <summary>
    /// EF reader failover using the gdbFailover plugin with home-reader-or-writer mode.
    /// Simulates a network outage on the current writer so the driver falls over to
    /// any available host (reader or writer).
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EFGdbReaderFailover_HomeReaderOrWriter(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var initialWriterInstanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.GetInstance(currentWriter);

        var connectionString = ConnectionStringHelper.GetUrl(
                Engine,
                initialWriterInstanceInfo.Host,
                initialWriterInstanceInfo.Port,
                Username,
                Password,
                ProxyDatabaseInfo!.DefaultDbName,
                2,
                10);
        var wrapperConnectionString = GetGdbCrashWrapperConnectionString(connectionString, "home-reader-or-writer");
        var options = this.BuildOptionsWithLogger(wrapperConnectionString, connectionString);

        using var context = new PersonDbContext(options);
        var connection = context.Database.GetDbConnection();

        if (async)
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
        }
        else
        {
            connection.Open();
        }

        Assert.Equal(ConnectionState.Open, connection.State);

        await ProxyHelper.DisableConnectivityAsync(currentWriter);

        try
        {
            await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
            {
                this.Logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
                await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, async);
            });
        }
        finally
        {
            await ProxyHelper.EnableConnectivityAsync(currentWriter);

            if (async)
            {
                await connection.CloseAsync();
            }
            else
            {
                connection.Close();
            }
        }
    }

    /// <summary>
    /// EF reader failover using the gdbFailover plugin with home-reader-or-writer mode,
    /// simulating a temporary failure of the current writer. The writer is expected to be
    /// re-elected once connectivity is restored.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EFGdbReaderFailover_WriterReelected(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var initialWriterInstanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.GetInstance(currentWriter);

        var connectionString = ConnectionStringHelper.GetUrl(
                Engine,
                initialWriterInstanceInfo.Host,
                initialWriterInstanceInfo.Port,
                Username,
                Password,
                ProxyDatabaseInfo!.DefaultDbName,
                2,
                10);
        var wrapperConnectionString = GetGdbCrashWrapperConnectionString(connectionString, "home-reader-or-writer");
        var options = this.BuildOptionsWithLogger(wrapperConnectionString, connectionString);

        using var context = new PersonDbContext(options);
        var connection = context.Database.GetDbConnection();

        if (async)
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
        }
        else
        {
            connection.Open();
        }

        Assert.Equal(ConnectionState.Open, connection.State);

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var simulationTask = AuroraUtils.SimulateTemporaryFailureTask(currentWriter, TimeSpan.Zero, TimeSpan.FromSeconds(12), tcs);

        // Wait for the simulation to start
        await tcs.Task;

        try
        {
            await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
            {
                this.Logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
                await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, async);
            });
        }
        finally
        {
            await simulationTask;

            if (async)
            {
                await connection.CloseAsync();
            }
            else
            {
                connection.Close();
            }
        }
    }

    /// <summary>
    /// End-to-end EF scenario with the gdbFailover plugin in strict-writer mode.
    /// Inserts a row, crashes the writer between operations, recovers through the
    /// failover exception, and confirms subsequent inserts persist on the new writer.
    /// Mirrors <c>EFCrashBeforeOpenWithFailoverPluginTest</c> but exercises gdbFailover.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EFGdbCrashBeforeOpen_StrictWriter_Persists()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var initialWriterInstanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.GetInstance(currentWriter);

        var connectionString = ConnectionStringHelper.GetUrl(
                Engine,
                initialWriterInstanceInfo.Host,
                initialWriterInstanceInfo.Port,
                Username,
                Password,
                ProxyDatabaseInfo!.DefaultDbName,
                2,
                10);
        var wrapperConnectionString = GetGdbCrashWrapperConnectionString(connectionString, "strict-writer");
        var options = this.BuildOptionsWithLogger(wrapperConnectionString, connectionString);

        using (var db = new PersonDbContext(options))
        {
            db.Database.ExecuteSqlRaw($"Truncate table persons;");
        }

        using (var db = new PersonDbContext(options))
        {
            Person jane = new() { FirstName = "Jane", LastName = "Smith" };
            db.Add(jane);
            db.SaveChanges();

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            var instanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.Logger.WriteLine($"==========================================");
            this.Logger.WriteLine($"Current node before crash is {instanceId}");
            await db.Database.CloseConnectionAsync();

            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await AuroraUtils.CrashInstance(currentWriter, tcs);

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            var postInstanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.Logger.WriteLine($"==========================================");
            this.Logger.WriteLine($"Current node after crash is {postInstanceId}");
            Assert.NotNull(postInstanceId);
            await db.Database.CloseConnectionAsync();

            Person john = new() { FirstName = "John", LastName = "Smith" };
            db.Add(john);
            db.SaveChanges();

            Person joe = new() { FirstName = "Joe", LastName = "Smith" };
            db.Add(joe);
            db.SaveChanges();
        }

        using (var db = new PersonDbContext(options))
        {
            Assert.True(db.Persons.Any(p => p.FirstName == "Jane"));
            Assert.True(db.Persons.Any(p => p.FirstName == "Joe"));
            Assert.True(db.Persons.Any(p => p.FirstName == "John"));
            Assert.Equal(3, db.Persons.Count());
        }
    }

    /// <summary>
    /// Async counterpart of <see cref="EFGdbCrashBeforeOpen_StrictWriter_Persists"/>.
    /// </summary>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task EFGdbCrashBeforeOpen_StrictWriter_PersistsAsync()
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var initialWriterInstanceInfo = TestEnvironment.Env.Info.ProxyDatabaseInfo!.GetInstance(currentWriter);

        var connectionString = ConnectionStringHelper.GetUrl(
                Engine,
                initialWriterInstanceInfo.Host,
                initialWriterInstanceInfo.Port,
                Username,
                Password,
                ProxyDatabaseInfo!.DefaultDbName,
                2,
                10);
        var wrapperConnectionString = GetGdbCrashWrapperConnectionString(connectionString, "strict-writer");
        var options = this.BuildOptionsWithLogger(wrapperConnectionString, connectionString);

        using (var db = new PersonDbContext(options))
        {
            await db.Database.ExecuteSqlRawAsync($"Truncate table persons;", TestContext.Current.CancellationToken);
        }

        using (var db = new PersonDbContext(options))
        {
            Person jane = new() { FirstName = "Jane", LastName = "Smith" };
            await db.AddAsync(jane, TestContext.Current.CancellationToken);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            var instanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.Logger.WriteLine($"==========================================");
            this.Logger.WriteLine($"Current node before crash is {instanceId}");
            await db.Database.CloseConnectionAsync();

            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await AuroraUtils.CrashInstance(currentWriter, tcs);

            await db.Database.OpenConnectionAsync(cancellationToken: TestContext.Current.CancellationToken);
            var postInstanceId = await AuroraUtils.ExecuteInstanceIdQuery(db.Database.GetDbConnection(), Engine, Deployment, true);
            this.Logger.WriteLine($"==========================================");
            this.Logger.WriteLine($"Current node after crash is {postInstanceId}");
            Assert.NotNull(postInstanceId);
            await db.Database.CloseConnectionAsync();

            Person john = new() { FirstName = "John", LastName = "Smith" };
            await db.AddAsync(john, TestContext.Current.CancellationToken);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);

            Person joe = new() { FirstName = "Joe", LastName = "Smith" };
            await db.AddAsync(joe, TestContext.Current.CancellationToken);
            await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using (var db = new PersonDbContext(options))
        {
            Assert.True(await db.Persons.AnyAsync(p => p.FirstName == "Jane", TestContext.Current.CancellationToken));
            Assert.True(await db.Persons.AnyAsync(p => p.FirstName == "Joe", TestContext.Current.CancellationToken));
            Assert.True(await db.Persons.AnyAsync(p => p.FirstName == "John", TestContext.Current.CancellationToken));
            Assert.Equal(3, await db.Persons.CountAsync(TestContext.Current.CancellationToken));
        }
    }
}
