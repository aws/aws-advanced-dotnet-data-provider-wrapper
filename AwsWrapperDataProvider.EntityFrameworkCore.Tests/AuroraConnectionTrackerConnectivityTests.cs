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
using AwsWrapperDataProvider.Driver.Plugins.Failover.Exceptions;
using AwsWrapperDataProvider.Tests.Container.Utils;
using Microsoft.EntityFrameworkCore;

namespace AwsWrapperDataProvider.EntityFrameworkCore.Tests;

/// <summary>
/// EF Core integration tests for the Aurora Connection Tracker plugin.
/// Validates that when a failover is detected by one EF context, idle connections
/// held by other EF contexts are closed by the tracker, and that those contexts
/// can recover on subsequent operations.
///
/// These tests only run on Aurora deployments because the connection tracker
/// invalidates idle connections only when the writer instance changes.
/// Currently, CrashInstance on Multi-AZ clusters simulates a temporary network
/// failure via Toxiproxy without triggering a real failover, so the writer
/// remains the same and the tracker has nothing to invalidate.
/// If Multi-AZ cluster testing adds support for real failover (writer change),
/// these tests should be extended to cover that engine as well.
/// </summary>
public class AuroraConnectionTrackerConnectivityTests : EFIntegrationTestBase
{
    private const int IdleContextCount = 3;

    protected override bool MakeSureFirstInstanceWriter => true;

    public AuroraConnectionTrackerConnectivityTests(ITestOutputHelper output) : base(output)
    {
    }

    private (string WrapperConnectionString, string ConnectionString) GetConnectionStrings(bool pooling)
    {
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine, Endpoint, Port, Username, Password, DefaultDbName, 2, 10, enablePooling: pooling);

        var wrapperConnectionString = connectionString
            + $";Plugins={PluginCodes.InitialConnection},{PluginCodes.AuroraConnectionTracker},{PluginCodes.Failover};"
            + $"EnableConnectFailover=true;"
            + $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";

        return (wrapperConnectionString, connectionString);
    }

    private DbContextOptions<PersonDbContext> BuildOptions(bool pooling)
    {
        var (wrapper, conn) = this.GetConnectionStrings(pooling);
        return this.BuildOptionsWithLogger(wrapper, conn);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    public async Task EF_IdleContextConnections_ClosedAfterFailover(bool pooling)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.DatabaseInfo.Instances.First().InstanceId;
        var options = this.BuildOptions(pooling);
        var idleContexts = new List<PersonDbContext>();

        try
        {
            using (var db = new PersonDbContext(options))
            {
                db.Database.ExecuteSqlRaw("Truncate table persons;");
            }

            // Open idle contexts, insert a Person on each, then hold the connection open.
            for (int i = 0; i < IdleContextCount; i++)
            {
                var ctx = new PersonDbContext(options);
                ctx.Add(new Person { FirstName = $"Idle{i}", LastName = "Smith" });
                ctx.SaveChanges();
                ctx.Database.OpenConnection();
                idleContexts.Add(ctx);
            }

            // Open an active context.
            using var activeCtx = new PersonDbContext(options);
            activeCtx.Add(new Person { FirstName = "Active", LastName = "Smith" });
            activeCtx.SaveChanges();
            activeCtx.Database.OpenConnection();

            // Verify all connections are open.
            foreach (var ctx in idleContexts)
            {
                Assert.Equal(ConnectionState.Open, ctx.Database.GetDbConnection().State);
            }

            Assert.Equal(ConnectionState.Open, activeCtx.Database.GetDbConnection().State);

            // Crash the writer.
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var crashTask = AuroraUtils.CrashInstance(currentWriter, tcs);
            await tcs.Task;

            // Trigger failover on the active context.
            await Assert.ThrowsAnyAsync<FailoverException>(() =>
                activeCtx.Database.ExecuteSqlRawAsync(
                    AuroraUtils.GetInstanceIdSql(Engine, Deployment),
                    TestContext.Current.CancellationToken));

            await crashTask;

            // Wait for invalidation to propagate.
            await Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            var clusterId = TestEnvironment.Env.Info.RdsDbName!;
            var newWriterId = await AuroraUtils.GetDBClusterWriterInstanceIdAsync(clusterId);

            if (currentWriter == newWriterId)
            {
                this.Logger.WriteLine($"Writer did not change, still {newWriterId}.");
                foreach (var ctx in idleContexts)
                {
                    Assert.Equal(ConnectionState.Open, ctx.Database.GetDbConnection().State);
                }
            }
            else
            {
                this.Logger.WriteLine($"Cluster failed over to instance {newWriterId}.");
                foreach (var ctx in idleContexts)
                {
                    Assert.Equal(ConnectionState.Closed, ctx.Database.GetDbConnection().State);
                }
            }

            // Verify all data committed before the crash is still present.
            using (var db = new PersonDbContext(options))
            {
                for (int i = 0; i < IdleContextCount; i++)
                {
                    Assert.True(db.Persons.Any(p => p.FirstName == $"Idle{i}"));
                }

                Assert.True(db.Persons.Any(p => p.FirstName == "Active"));
                Assert.Equal(IdleContextCount + 1, db.Persons.Count());
            }
        }
        finally
        {
            foreach (var ctx in idleContexts)
            {
                try
                {
                    ctx.Database.CloseConnection();
                }
                catch
                {
                    // Ignore
                }

                try
                {
                    ctx.Dispose();
                }
                catch
                {
                    // Ignore
                }
            }
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    public async Task EF_IdleContextConnections_ClosedAfterFailoverAsync(bool pooling)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.DatabaseInfo.Instances.First().InstanceId;
        var options = this.BuildOptions(pooling);
        var idleContexts = new List<PersonDbContext>();

        try
        {
            using (var db = new PersonDbContext(options))
            {
                await db.Database.ExecuteSqlRawAsync("Truncate table persons;", TestContext.Current.CancellationToken);
            }

            // Open idle contexts, insert a Person on each, then hold the connection open.
            for (int i = 0; i < IdleContextCount; i++)
            {
                var ctx = new PersonDbContext(options);
                await ctx.AddAsync(new Person { FirstName = $"Idle{i}", LastName = "Smith" }, TestContext.Current.CancellationToken);
                await ctx.SaveChangesAsync(TestContext.Current.CancellationToken);
                await ctx.Database.OpenConnectionAsync(TestContext.Current.CancellationToken);
                idleContexts.Add(ctx);
            }

            // Open an active context.
            using var activeCtx = new PersonDbContext(options);
            await activeCtx.AddAsync(new Person { FirstName = "Active", LastName = "Smith" }, TestContext.Current.CancellationToken);
            await activeCtx.SaveChangesAsync(TestContext.Current.CancellationToken);
            await activeCtx.Database.OpenConnectionAsync(TestContext.Current.CancellationToken);

            // Verify all connections are open.
            foreach (var ctx in idleContexts)
            {
                Assert.Equal(ConnectionState.Open, ctx.Database.GetDbConnection().State);
            }

            Assert.Equal(ConnectionState.Open, activeCtx.Database.GetDbConnection().State);

            // Crash the writer.
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var crashTask = AuroraUtils.CrashInstance(currentWriter, tcs);
            await tcs.Task;

            // Trigger failover on the active context.
            await Assert.ThrowsAnyAsync<FailoverException>(() =>
                activeCtx.Database.ExecuteSqlRawAsync(
                    AuroraUtils.GetInstanceIdSql(Engine, Deployment),
                    TestContext.Current.CancellationToken));

            await crashTask;

            // Wait for invalidation to propagate.
            await Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            var clusterId = TestEnvironment.Env.Info.RdsDbName!;
            var newWriterId = await AuroraUtils.GetDBClusterWriterInstanceIdAsync(clusterId);

            if (currentWriter == newWriterId)
            {
                this.Logger.WriteLine($"Writer did not change, still {newWriterId}.");
                foreach (var ctx in idleContexts)
                {
                    Assert.Equal(ConnectionState.Open, ctx.Database.GetDbConnection().State);
                }
            }
            else
            {
                this.Logger.WriteLine($"Cluster failed over to instance {newWriterId}.");
                foreach (var ctx in idleContexts)
                {
                    Assert.Equal(ConnectionState.Closed, ctx.Database.GetDbConnection().State);
                }
            }

            // Verify all data committed before the crash is still present.
            using (var db = new PersonDbContext(options))
            {
                for (int i = 0; i < IdleContextCount; i++)
                {
                    Assert.True(await db.Persons.AnyAsync(p => p.FirstName == $"Idle{i}", TestContext.Current.CancellationToken));
                }

                Assert.True(await db.Persons.AnyAsync(p => p.FirstName == "Active", TestContext.Current.CancellationToken));
                Assert.Equal(IdleContextCount + 1, await db.Persons.CountAsync(TestContext.Current.CancellationToken));
            }
        }
        finally
        {
            foreach (var ctx in idleContexts)
            {
                try
                {
                    await ctx.Database.CloseConnectionAsync();
                }
                catch
                {
                    // Ignore
                }

                try
                {
                    await ctx.DisposeAsync();
                }
                catch
                {
                    // Ignore
                }
            }
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    public async Task EF_IdleContextConnections_RecoverAfterInvalidation(bool pooling)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.DatabaseInfo.Instances.First().InstanceId;
        var options = this.BuildOptions(pooling);
        var idleContexts = new List<PersonDbContext>();

        try
        {
            using (var db = new PersonDbContext(options))
            {
                db.Database.ExecuteSqlRaw("Truncate table persons;");
            }

            // Open idle contexts, insert a Person on each, then hold the connection open.
            for (int i = 0; i < 2; i++)
            {
                var ctx = new PersonDbContext(options);
                ctx.Add(new Person { FirstName = $"Idle{i}", LastName = "Smith" });
                ctx.SaveChanges();
                ctx.Database.OpenConnection();
                idleContexts.Add(ctx);
            }

            // Open an active context.
            using var activeCtx = new PersonDbContext(options);
            activeCtx.Database.OpenConnection();

            // Crash the writer.
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var crashTask = AuroraUtils.CrashInstance(currentWriter, tcs);
            await tcs.Task;

            // Trigger failover on the active context.
            await Assert.ThrowsAnyAsync<FailoverException>(() =>
                activeCtx.Database.ExecuteSqlRawAsync(
                    AuroraUtils.GetInstanceIdSql(Engine, Deployment),
                    TestContext.Current.CancellationToken));

            await crashTask;
            await Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            var clusterId = TestEnvironment.Env.Info.RdsDbName!;
            var newWriterId = await AuroraUtils.GetDBClusterWriterInstanceIdAsync(clusterId);
            this.Logger.WriteLine($"Cluster failed over to instance {newWriterId}.");

            // Verify idle connections were closed.
            foreach (var ctx in idleContexts)
            {
                Assert.Equal(ConnectionState.Closed, ctx.Database.GetDbConnection().State);
            }

            // Idle context 0: read query should recover — verify pre-crash data is accessible.
            Assert.True(idleContexts[0].Persons.Any(p => p.FirstName == "Idle0"));
            Assert.True(idleContexts[0].Persons.Any(p => p.FirstName == "Idle1"));

            // Idle context 1: write operation should recover.
            idleContexts[1].Add(new Person { FirstName = "Joe", LastName = "Smith" });
            idleContexts[1].SaveChanges();

            // Verify final state: pre-crash data + post-recovery insert.
            using (var db = new PersonDbContext(options))
            {
                Assert.True(db.Persons.Any(p => p.FirstName == "Idle0"));
                Assert.True(db.Persons.Any(p => p.FirstName == "Idle1"));
                Assert.True(db.Persons.Any(p => p.FirstName == "Joe"));
                Assert.Equal(3, db.Persons.Count());
            }
        }
        finally
        {
            foreach (var ctx in idleContexts)
            {
                try
                {
                    ctx.Database.CloseConnection();
                }
                catch
                {
                    // Ignore
                }

                try
                {
                    ctx.Dispose();
                }
                catch
                {
                    // Ignore
                }
            }
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg-ef")]
    [Trait("Database", "mysql-ef")]
    [Trait("Engine", "aurora")]
    public async Task EF_IdleContextConnections_RecoverAfterInvalidationAsync(bool pooling)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        string currentWriter = TestEnvironment.Env.Info.DatabaseInfo.Instances.First().InstanceId;
        var options = this.BuildOptions(pooling);
        var idleContexts = new List<PersonDbContext>();

        try
        {
            using (var db = new PersonDbContext(options))
            {
                await db.Database.ExecuteSqlRawAsync("Truncate table persons;", TestContext.Current.CancellationToken);
            }

            // Open idle contexts, insert a Person on each, then hold the connection open.
            for (int i = 0; i < 2; i++)
            {
                var ctx = new PersonDbContext(options);
                await ctx.AddAsync(new Person { FirstName = $"Idle{i}", LastName = "Smith" }, TestContext.Current.CancellationToken);
                await ctx.SaveChangesAsync(TestContext.Current.CancellationToken);
                await ctx.Database.OpenConnectionAsync(TestContext.Current.CancellationToken);
                idleContexts.Add(ctx);
            }

            // Open an active context.
            using var activeCtx = new PersonDbContext(options);
            await activeCtx.Database.OpenConnectionAsync(TestContext.Current.CancellationToken);

            // Crash the writer.
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var crashTask = AuroraUtils.CrashInstance(currentWriter, tcs);
            await tcs.Task;

            // Trigger failover on the active context.
            await Assert.ThrowsAnyAsync<FailoverException>(() =>
                activeCtx.Database.ExecuteSqlRawAsync(
                    AuroraUtils.GetInstanceIdSql(Engine, Deployment),
                    TestContext.Current.CancellationToken));

            await crashTask;
            await Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            var clusterId = TestEnvironment.Env.Info.RdsDbName!;
            var newWriterId = await AuroraUtils.GetDBClusterWriterInstanceIdAsync(clusterId);
            Assert.SkipWhen(currentWriter == newWriterId, "Writer did not change after failover; cannot verify recovery.");

            this.Logger.WriteLine($"Cluster failed over to instance {newWriterId}.");

            // Verify idle connections were closed.
            foreach (var ctx in idleContexts)
            {
                Assert.Equal(ConnectionState.Closed, ctx.Database.GetDbConnection().State);
            }

            // Idle context 0: read query should recover — verify pre-crash data is accessible.
            Assert.True(await idleContexts[0].Persons.AnyAsync(
                p => p.FirstName == "Idle0", TestContext.Current.CancellationToken));
            Assert.True(await idleContexts[0].Persons.AnyAsync(
                p => p.FirstName == "Idle1", TestContext.Current.CancellationToken));

            // Idle context 1: write operation should recover.
            await idleContexts[1].AddAsync(
                new Person { FirstName = "Joe", LastName = "Smith" }, TestContext.Current.CancellationToken);
            await idleContexts[1].SaveChangesAsync(TestContext.Current.CancellationToken);

            // Verify final state: pre-crash data + post-recovery insert.
            using (var db = new PersonDbContext(options))
            {
                Assert.True(await db.Persons.AnyAsync(p => p.FirstName == "Idle0", TestContext.Current.CancellationToken));
                Assert.True(await db.Persons.AnyAsync(p => p.FirstName == "Idle1", TestContext.Current.CancellationToken));
                Assert.True(await db.Persons.AnyAsync(p => p.FirstName == "Joe", TestContext.Current.CancellationToken));
                Assert.Equal(3, await db.Persons.CountAsync(TestContext.Current.CancellationToken));
            }
        }
        finally
        {
            foreach (var ctx in idleContexts)
            {
                try
                {
                    await ctx.Database.CloseConnectionAsync();
                }
                catch
                {
                    // Ignore
                }

                try
                {
                    await ctx.DisposeAsync();
                }
                catch
                {
                    // Ignore
                }
            }
        }
    }
}
