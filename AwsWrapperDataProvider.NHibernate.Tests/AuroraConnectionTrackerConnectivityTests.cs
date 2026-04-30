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
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.Failover.Exceptions;
using AwsWrapperDataProvider.Tests.Container.Utils;
using NHibernate;

namespace AwsWrapperDataProvider.NHibernate.Tests;

/// <summary>
/// NHibernate integration tests for the Aurora Connection Tracker plugin.
/// Validates that when a failover is detected by one NHibernate session, idle connections
/// held by other sessions are closed by the tracker, and that those sessions
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
public class AuroraConnectionTrackerConnectivityTests : NHibernateTestBase
{
    private const int IdleSessionCount = 3;

    protected override bool MakeSureFirstInstanceWriter => true;

    private readonly ITestOutputHelper logger;

    public AuroraConnectionTrackerConnectivityTests(ITestOutputHelper output)
    {
        this.logger = output;
    }

    private static DbConnection GetConnection(ISession session)
    {
        return session.Connection
               ?? throw new InvalidOperationException("Could not get DbConnection from NHibernate session.");
    }

    private string BuildConnectionString(bool pooling)
    {
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine, Endpoint, Port, Username, Password, DefaultDbName, 2, 10, enablePooling: pooling);

        return connectionString
               + $";Plugins={PluginCodes.InitialConnection},{PluginCodes.AuroraConnectionTracker},{PluginCodes.Failover};"
               + "EnableConnectFailover=true;"
               + "FailoverMode=StrictWriter;"
               + $"ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql-nh")]
    [Trait("Database", "pg-nh")]
    [Trait("Engine", "aurora")]
    public async Task NHibernate_IdleSessions_ConnectionsClosedAfterFailover(bool pooling)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        var currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var wrapperConnectionString = this.BuildConnectionString(pooling);
        var cfg = this.GetNHibernateConfiguration(
            wrapperConnectionString,
            new Dictionary<string, string> { { "connection.release_mode", "on_close" } });
        var sessionFactory = cfg.BuildSessionFactory();
        var idleSessions = new List<ISession>();

        try
        {
            // Create and clear the table using a throwaway session.
            using (var setupSession = sessionFactory.OpenSession())
            {
                this.CreateAndClearPersonsTable(setupSession);
            }

            // Open idle sessions, insert a Person on each, and hold the connection open.
            // With on_close release mode, the connection stays open for the session's lifetime.
            for (int i = 0; i < IdleSessionCount; i++)
            {
                var session = sessionFactory.OpenSession();
                using (var tx = session.BeginTransaction())
                {
                    session.Save(new Person { FirstName = $"Idle{i}", LastName = "Smith" });
                    tx.Commit();
                }

                // Access session.Connection to ensure the connection is acquired and open.
                var conn = GetConnection(session);
                Assert.Equal(ConnectionState.Open, conn.State);
                idleSessions.Add(session);
            }

            // Open an active session and hold its connection open.
            using var activeSession = sessionFactory.OpenSession();
            using (var tx = activeSession.BeginTransaction())
            {
                activeSession.Save(new Person { FirstName = "Active", LastName = "Smith" });
                tx.Commit();
            }

            var activeConn = GetConnection(activeSession);
            Assert.Equal(ConnectionState.Open, activeConn.State);

            // Crash the writer — triggers a real Aurora failover.
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var crashTask = AuroraUtils.CrashInstance(currentWriter, tcs);
            await tcs.Task;

            // Trigger failover on the active session by executing a query against the dead writer.
            var exception = await Assert.ThrowsAnyAsync<HibernateException>(() =>
                activeSession.CreateSQLQuery(AuroraUtils.GetInstanceIdSql(Engine, Deployment))
                    .UniqueResultAsync<object>(TestContext.Current.CancellationToken));
            Assert.IsAssignableFrom<FailoverException>(exception.InnerException);

            await crashTask;

            // Wait for invalidation to propagate.
            await Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            var clusterId = TestEnvironment.Env.Info.RdsDbName!;
            var newWriterId = await AuroraUtils.GetDBClusterWriterInstanceIdAsync(clusterId);
            Assert.SkipWhen(currentWriter == newWriterId, "Writer did not change after failover; cannot verify tracker invalidation.");

            this.logger.WriteLine($"Cluster failed over from {currentWriter} to {newWriterId}.");

            // Verify idle sessions' connections were closed by the tracker.
            foreach (var session in idleSessions)
            {
                Assert.Equal(ConnectionState.Closed, GetConnection(session).State);
            }

            // Verify all data committed before the crash is still present.
            using (var verifySession = sessionFactory.OpenSession())
            {
                for (int i = 0; i < IdleSessionCount; i++)
                {
                    var persons = verifySession.CreateSQLQuery($"SELECT COUNT(*) FROM persons WHERE FirstName = 'Idle{i}'")
                        .UniqueResult<object>();
                    Assert.True(Convert.ToInt32(persons) > 0, $"Idle{i} should exist after failover.");
                }

                var activeCount = verifySession.CreateSQLQuery("SELECT COUNT(*) FROM persons WHERE FirstName = 'Active'")
                    .UniqueResult<object>();
                Assert.True(Convert.ToInt32(activeCount) > 0, "Active should exist after failover.");
            }
        }
        finally
        {
            foreach (var session in idleSessions)
            {
                try
                {
                    session.Close();
                }
                catch
                {
                    // ignore
                }

                try
                {
                    session.Dispose();
                }
                catch
                {
                    // ignore
                }
            }
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql-nh")]
    [Trait("Database", "pg-nh")]
    [Trait("Engine", "aurora")]
    public async Task NHibernate_IdleSessions_RecoverAfterInvalidation(bool pooling)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        var currentWriter = TestEnvironment.Env.Info.ProxyDatabaseInfo!.Instances.First().InstanceId;
        var wrapperConnectionString = this.BuildConnectionString(pooling);
        var cfg = this.GetNHibernateConfiguration(
            wrapperConnectionString,
            new Dictionary<string, string> { { "connection.release_mode", "on_close" } });
        var sessionFactory = cfg.BuildSessionFactory();
        var idleSessions = new List<ISession>();

        try
        {
            // Create and clear the table using a throwaway session.
            using (var setupSession = sessionFactory.OpenSession())
            {
                this.CreateAndClearPersonsTable(setupSession);
            }

            // Open 2 idle sessions, each saves a Person.
            for (int i = 0; i < 2; i++)
            {
                var session = sessionFactory.OpenSession();
                using (var tx = session.BeginTransaction())
                {
                    session.Save(new Person { FirstName = $"Idle{i}", LastName = "Smith" });
                    tx.Commit();
                }

                var conn = GetConnection(session);
                Assert.Equal(ConnectionState.Open, conn.State);
                idleSessions.Add(session);
            }

            // Open an active session.
            using var activeSession = sessionFactory.OpenSession();
            var activeConn = GetConnection(activeSession);
            Assert.Equal(ConnectionState.Open, activeConn.State);

            // Crash the writer.
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var crashTask = AuroraUtils.CrashInstance(currentWriter, tcs);
            await tcs.Task;

            // Trigger failover on the active session.
            var exception = await Assert.ThrowsAnyAsync<HibernateException>(() =>
                activeSession.CreateSQLQuery(AuroraUtils.GetInstanceIdSql(Engine, Deployment))
                    .UniqueResultAsync<object>(TestContext.Current.CancellationToken));
            Assert.IsAssignableFrom<FailoverException>(exception.InnerException);

            await crashTask;
            await Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            var clusterId = TestEnvironment.Env.Info.RdsDbName!;
            var newWriterId = await AuroraUtils.GetDBClusterWriterInstanceIdAsync(clusterId);
            Assert.SkipWhen(currentWriter == newWriterId, "Writer did not change after failover; cannot verify recovery.");

            this.logger.WriteLine($"Cluster failed over from {currentWriter} to {newWriterId}.");

            // Verify idle connections were closed by the tracker.
            foreach (var session in idleSessions)
            {
                Assert.Equal(ConnectionState.Closed, GetConnection(session).State);
            }

            // Idle session 0: first operation triggers failover plugin reconnection,
            // which throws FailoverSuccessException. The connection is now pointing
            // to the new writer, so the subsequent read succeeds.
            var reconnectException0 = await Assert.ThrowsAnyAsync<HibernateException>(() =>
                idleSessions[0].CreateSQLQuery("SELECT COUNT(*) FROM persons WHERE FirstName = 'Idle0'")
                    .UniqueResultAsync<object>(TestContext.Current.CancellationToken));
            Assert.IsAssignableFrom<FailoverException>(reconnectException0.InnerException);

            // Now the connection is re-established — read should succeed.
            var readResult = await idleSessions[0].CreateSQLQuery("SELECT COUNT(*) FROM persons WHERE FirstName = 'Idle0'")
                .UniqueResultAsync<object>(TestContext.Current.CancellationToken);
            Assert.True(Convert.ToInt32(readResult) > 0, "Idle0 should be readable after recovery.");

            // Idle session 1: first operation also triggers reconnection.
            var reconnectException1 = await Assert.ThrowsAnyAsync<HibernateException>(() =>
                idleSessions[1].CreateSQLQuery("SELECT 1")
                    .UniqueResultAsync<object>(TestContext.Current.CancellationToken));
            Assert.IsAssignableFrom<FailoverException>(reconnectException1.InnerException);

            // Now write should succeed.
            using (var tx = idleSessions[1].BeginTransaction())
            {
                await idleSessions[1].SaveAsync(new Person { FirstName = "Joe", LastName = "Smith" }, TestContext.Current.CancellationToken);
                await tx.CommitAsync(TestContext.Current.CancellationToken);
            }

            // Verify final state: pre-crash data + post-recovery insert.
            using (var verifySession = sessionFactory.OpenSession())
            {
                var idle0Count = verifySession.CreateSQLQuery("SELECT COUNT(*) FROM persons WHERE FirstName = 'Idle0'")
                    .UniqueResult<object>();
                Assert.True(Convert.ToInt32(idle0Count) > 0, "Idle0 should exist.");

                var idle1Count = verifySession.CreateSQLQuery("SELECT COUNT(*) FROM persons WHERE FirstName = 'Idle1'")
                    .UniqueResult<object>();
                Assert.True(Convert.ToInt32(idle1Count) > 0, "Idle1 should exist.");

                var joeCount = verifySession.CreateSQLQuery("SELECT COUNT(*) FROM persons WHERE FirstName = 'Joe'")
                    .UniqueResult<object>();
                Assert.True(Convert.ToInt32(joeCount) > 0, "Joe should exist after recovery write.");

                var totalCount = verifySession.CreateSQLQuery("SELECT COUNT(*) FROM persons")
                    .UniqueResult<object>();
                Assert.Equal(3, Convert.ToInt32(totalCount));
            }
        }
        finally
        {
            foreach (var session in idleSessions)
            {
                try
                {
                    session.Close();
                }
                catch
                {
                    // ignore
                }

                try
                {
                    session.Dispose();
                }
                catch
                {
                    // ignore
                }
            }
        }
    }
}
