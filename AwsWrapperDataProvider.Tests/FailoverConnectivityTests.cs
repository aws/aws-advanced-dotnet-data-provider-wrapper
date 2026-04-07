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

namespace AwsWrapperDataProvider.Tests;

public class FailoverConnectivityTests : IntegrationTestBase
{
    private readonly ITestOutputHelper logger;

    protected override bool MakeSureFirstInstanceWriter => true;

    public FailoverConnectivityTests(ITestOutputHelper output)
    {
        this.logger = output;
    }

    /// <summary>
    /// Verifies that after a writer failover, reopening a connection to the old writer's
    /// instance endpoint with pooling enabled does not silently return a stale pooled
    /// connection that is now pointing to a reader.
    ///
    /// Steps:
    /// 1. Connect to the current writer using its instance endpoint with Pooling=true.
    /// 2. Trigger a cluster failover so the writer role moves to a different instance.
    /// 3. The active connection detects the failover (FailoverSuccessException), which
    ///    causes the aurora connection tracker to invalidate (Close) tracked connections.
    /// 4. Open a brand-new wrapper connection using the same old writer instance endpoint.
    /// 5. Execute an INSERT — if the underlying driver pool handed back a stale physical
    ///    connection to the now-demoted reader, this will fail with a read-only error.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task ServerFailoverWithPooling_StaleConnectionReturnsReadOnlyError(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");

        var dbInfo = TestEnvironment.Env.Info.DatabaseInfo;
        string currentWriter = dbInfo.Instances.First().InstanceId;
        var initialWriterInstanceInfo = dbInfo.GetInstance(currentWriter);

        // Use the writer's real instance endpoint with pooling ON and the connection tracker plugin.
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            initialWriterInstanceInfo.Host,
            initialWriterInstanceInfo.Port,
            Username,
            Password,
            DefaultDbName,
            2,
            10,
            $"{PluginCodes.AuroraConnectionTracker},{PluginCodes.Failover}");
        connectionString += $"; ClusterInstanceHostPattern=?.{dbInfo.InstanceEndpointSuffix}:{dbInfo.InstanceEndpointPort}; Pooling=true";

        this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Connecting to writer instance: {initialWriterInstanceInfo.Host}");

        // Step 1: Open a connection and confirm we're on the writer.
        await using var activeConn = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(activeConn, async);
        Assert.Equal(ConnectionState.Open, activeConn.State);

        var instanceId = await AuroraUtils.ExecuteInstanceIdQuery(activeConn, Engine, Deployment, async);
        this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Connected to instance: {instanceId}");
        Assert.Equal(currentWriter, instanceId);

        // Create a test table to use for the write query later.
        const string createTableSql = "DROP TABLE IF EXISTS pooling_failover_test; CREATE TABLE pooling_failover_test (id SERIAL PRIMARY KEY, data TEXT)";
        await AuroraUtils.ExecuteNonQuery(activeConn, createTableSql, async);

        // Step 2: Trigger a cluster failover and wait for it to complete.
        var clusterId = TestEnvironment.Env.Info.RdsDbName!;
        var targetWriter = dbInfo.Instances.First(i => i.InstanceId != currentWriter).InstanceId;
        this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Triggering failover from {currentWriter} to {targetWriter}...");
        await AuroraUtils.FailoverClusterToATargetAndWaitUntilWriterChanged(clusterId, currentWriter, targetWriter);

        // Step 3: Use the active connection to trigger failover detection in the plugin.
        await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
        {
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing query to trigger failover detection...");
            await AuroraUtils.ExecuteInstanceIdQuery(activeConn, Engine, Deployment, async);
        });

        // Allow time for invalidation and pool state to settle.
        await Task.Delay(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        var newWriterId = await AuroraUtils.GetDBClusterWriterInstanceIdAsync(clusterId);
        this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Old writer: {currentWriter}, New writer: {newWriterId}");

        // Step 4: Open a new connection using the SAME old writer instance endpoint.
        // If the pool still has the stale physical connection, it will be returned here.
        this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Opening new connection to old writer endpoint (now a reader)...");
        await using var newConn = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(newConn, async);
        Assert.Equal(ConnectionState.Open, newConn.State);

        // Step 5: Execute a write query. If the pool returned a stale connection to the
        // now-demoted reader, this should fail with "cannot execute INSERT in a read-only transaction".
        const string writeSql = "INSERT INTO pooling_failover_test (data) VALUES ('post-failover-write')";
        Exception? writeException = null;
        try
        {
            await AuroraUtils.ExecuteNonQuery(newConn, writeSql, async);
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Write query succeeded — no stale pooled connection issue.");
        }
        catch (Exception ex)
        {
            writeException = ex;
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Write query failed: {ex.GetType().Name}: {ex.Message}");
        }

        // Log the outcome. We expect the write to fail if the pooling issue exists.
        if (writeException != null)
        {
            var fullMessage = writeException.InnerException?.Message ?? writeException.Message;
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} CONFIRMED: Stale pooled connection issue detected. Error: {fullMessage}");
            Assert.Contains("read-only", fullMessage, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Write succeeded — pool did not return a stale connection.");
        }

        // Cleanup
        try
        {
            await AuroraUtils.ExecuteNonQuery(newConn, "DROP TABLE IF EXISTS pooling_failover_test", async);
        }
        catch
        {
            // Best-effort cleanup
        }
    }
}
