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
using Amazon.RDS.Model;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Driver.Plugins.ReadWriteSplitting;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.CustomEndpoint.CustomEndpoint;
using AwsWrapperDataProvider.Tests.Container.Utils;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

/// <summary>
/// Integration tests for the Custom Endpoint plugin, ported from the Java driver's CustomEndpointTest.
/// Runs only on Aurora with at least 3 instances; requires first instance to be writer.
/// </summary>
public class CustomEndpointConnectivityTests : IntegrationTestBase, IClassFixture<CustomEndpointTestFixture>
{
    private readonly ITestOutputHelper _logger;
    private readonly CustomEndpointTestFixture _fixture;

    protected override bool MakeSureFirstInstanceWriter => true;

    public CustomEndpointConnectivityTests(CustomEndpointTestFixture fixture, ITestOutputHelper output)
    {
        this._fixture = fixture;
        this._logger = output;
    }

    public override async ValueTask InitializeAsync()
    {
        ConnectionPluginChainBuilder.RegisterPluginFactory<CustomEndpointPluginFactory>(PluginCodes.CustomEndpoint);
        await base.InitializeAsync();
    }

    /// <summary>
    /// Connects to a custom endpoint with failover plugin, verifies connection is to an endpoint member,
    /// triggers failover, expects FailoverSuccessException, then verifies new connection is still to an endpoint member.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task CustomEndpoint_Failover()
    {
        Assert.SkipWhen(NumberOfInstances < 3, "Skipped due to test requiring number of database instances >= 3.");
        Assert.SkipWhen(this._fixture.EndpointInfo == null, "Custom endpoint fixture not created (not Aurora or < 3 instances).");

        var dbInfo = TestEnvironment.Env.Info.DatabaseInfo;
        var port = Port;
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            this._fixture.EndpointInfo.Endpoint,
            port,
            Username,
            Password,
            DefaultDbName,
            10,
            10,
            "customEndpoint,failover",
            false);
        connectionString += "; FailoverMode=ReaderOrWriter";
        connectionString += $"; ClusterInstanceHostPattern=?.{dbInfo.InstanceEndpointSuffix}:{dbInfo.InstanceEndpointPort}";

        using AwsWrapperConnection connection = Engine switch
        {
            DatabaseEngine.MYSQL => new AwsWrapperConnection<MySqlConnection>(connectionString),
            DatabaseEngine.PG => new AwsWrapperConnection<NpgsqlConnection>(connectionString),
            _ => throw new NotSupportedException($"Unsupported engine: {Engine}"),
        };

        await connection.OpenAsync(TestContext.Current.CancellationToken);
        Assert.Equal(ConnectionState.Open, connection.State);

        var endpointMembers = this._fixture.EndpointInfo.StaticMembers ?? new List<string>();
        var instanceId = await AuroraUtils.QueryInstanceId(connection, false);
        Assert.True(endpointMembers.Contains(instanceId!), $"Instance {instanceId} should be in endpoint members: [{string.Join(", ", endpointMembers)}]");

        var currentWriter = TestEnvironment.Env.Info.DatabaseInfo.Instances[0].InstanceId;
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        if (instanceId == currentWriter)
        {
            var crashTask = AuroraUtils.CrashInstance(currentWriter, tcs);
            await tcs.Task;
            await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
            {
                this._logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
                await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, true);
            });
            await crashTask;
        }
        else
        {
            await AuroraUtils.FailoverClusterToATargetAndWaitUntilWriterChanged(
                TestEnvironment.Env.Info.RdsDbName!,
                currentWriter,
                instanceId!);
            await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
            {
                this._logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
                await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, true);
            });
        }

        var newInstanceId = await AuroraUtils.QueryInstanceId(connection, false);
        Assert.True(endpointMembers.Contains(newInstanceId!), $"New instance {newInstanceId} should be in endpoint members: [{string.Join(", ", endpointMembers)}]");
    }

    /// <summary>
    /// Ensures the custom endpoint's single instance has the specified role (writer or reader).
    /// May trigger a failover to achieve the desired role.
    /// </summary>
    private async Task SetupCustomEndpointRoleAsync(HostRole hostRole)
    {
        this._logger.WriteLine($"Setting up custom endpoint instance with role: {hostRole}");
        var endpointMembers = this._fixture.EndpointInfo!.StaticMembers ?? new List<string>();
        var clusterId = TestEnvironment.Env.Info.RdsDbName!;
        var originalWriter = await AuroraUtils.GetDBClusterWriterInstanceIdAsync(clusterId);

        var connectionStringNoPlugins = ConnectionStringHelper.GetUrl(
            Engine,
            this._fixture.EndpointInfo.Endpoint,
            Port,
            Username,
            Password,
            DefaultDbName,
            10,
            10,
            string.Empty,
            false);

        using (var conn = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionStringNoPlugins))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            var originalInstanceId = await AuroraUtils.QueryInstanceId(conn, true);
            Assert.True(endpointMembers.Contains(originalInstanceId!), $"Instance {originalInstanceId} should be in endpoint members");

            string? failoverTarget = null;
            if (hostRole == HostRole.Writer)
            {
                if (originalInstanceId == originalWriter)
                {
                    this._logger.WriteLine($"Role is already {hostRole}, no failover needed.");
                    return;
                }

                failoverTarget = originalInstanceId;
                this._logger.WriteLine("Failing over to get writer role...");
            }
            else if (hostRole == HostRole.Reader)
            {
                if (originalInstanceId != originalWriter)
                {
                    this._logger.WriteLine($"Role is already {hostRole}, no failover needed.");
                    return;
                }

                this._logger.WriteLine("Failing over to get reader role...");
            }

            await AuroraUtils.FailoverClusterToATargetAndWaitUntilWriterChanged(clusterId, originalWriter, failoverTarget!);
        }

        this._logger.WriteLine($"Verifying that new connection has role: {hostRole}");
        using (var conn = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionStringNoPlugins))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            var currentInstanceId = await AuroraUtils.QueryInstanceId(conn, true);
            Assert.True(endpointMembers.Contains(currentInstanceId!), $"Instance {currentInstanceId} should be in endpoint members");

            var newRole = await AuroraUtils.QueryHostRoleAsync(conn, Engine, true);
            Assert.Equal(hostRole, newRole);
        }

        this._logger.WriteLine($"Custom endpoint instance successfully set to role: {hostRole}");
    }

    /// <summary>
    /// Tests read-write splitting with custom endpoint changes when initial connection is to a reader:
    /// 1. Connect to reader via custom endpoint (endpoint has only reader).
    /// 2. Switch to writer fails (endpoint has no writer).
    /// 3. Add writer to endpoint; switch to writer succeeds.
    /// 4. Switch back to reader succeeds.
    /// 5. Remove writer from endpoint; switch to writer fails.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task CustomEndpoint_ReadWriteSplitting_WithCustomEndpointChanges_WithReaderAsInitConn()
    {
        Assert.SkipWhen(NumberOfInstances < 3, "Skipped due to test requiring number of database instances >= 3.");
        Assert.SkipWhen(this._fixture.EndpointInfo == null, "Custom endpoint fixture not created (not Aurora or < 3 instances).");

        await this.SetupCustomEndpointRoleAsync(HostRole.Reader);

        var pluginCodes = Engine == DatabaseEngine.PG ? "customEndpoint,efm,readWriteSplitting,failover" : "customEndpoint,readWriteSplitting,failover";
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            this._fixture.EndpointInfo!.Endpoint,
            Port,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            pluginCodes,
            false);
        connectionString += $"; ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";
        connectionString += $"; {PropertyDefinition.CustomEndpointMonitorIdleExpirationMs.Name}=30000";
        connectionString += $"; {PropertyDefinition.WaitForCustomEndpointInfoTimeoutMs.Name}=30000";

        using var connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        var endpointMembers = this._fixture.EndpointInfo.StaticMembers ?? new List<string>();
        var originalReaderId = await AuroraUtils.QueryInstanceId(connection, true);
        Assert.True(endpointMembers.Contains(originalReaderId!), $"Instance {originalReaderId} should be in endpoint members");

        this._logger.WriteLine("Initial connection is to a reader. Attempting to switch to writer...");
        await Assert.ThrowsAsync<ReadWriteSplittingDbException>(async () =>
        {
            await AuroraUtils.SetReadOnly(connection, Engine, false, true);
        });

        var writerId = await AuroraUtils.GetDBClusterWriterInstanceIdAsync(TestEnvironment.Env.Info.RdsDbName!);
        await AuroraUtils.ModifyDBClusterEndpointAsync(this._fixture.EndpointId, [originalReaderId!, writerId]);

        try
        {
            await AuroraUtils.WaitUntilEndpointHasMembersAsync(this._fixture.EndpointId, new HashSet<string> { originalReaderId!, writerId });

            await AuroraUtils.SetReadOnly(connection, Engine, false, true);
            var newInstanceId = await AuroraUtils.QueryInstanceId(connection, true);
            Assert.Equal(writerId, newInstanceId);

            await AuroraUtils.SetReadOnly(connection, Engine, true, true);
            newInstanceId = await AuroraUtils.QueryInstanceId(connection, true);
            Assert.Equal(originalReaderId, newInstanceId);
        }
        finally
        {
            await AuroraUtils.ModifyDBClusterEndpointAsync(this._fixture.EndpointId, [originalReaderId!]);
            await AuroraUtils.WaitUntilEndpointHasMembersAsync(this._fixture.EndpointId, new HashSet<string> { originalReaderId! });
        }

        this._logger.WriteLine("Writer removed from endpoint. Attempting to switch to writer should fail...");
        await Assert.ThrowsAsync<ReadWriteSplittingDbException>(async () =>
        {
            await AuroraUtils.SetReadOnly(connection, Engine, false, true);
        });
    }

    /// <summary>
    /// Tests read-write splitting with custom endpoint changes when initial connection is to the writer:
    /// 1. Connect to writer via custom endpoint (endpoint has only writer).
    /// 2. Switch to reader does not throw but stays on writer (fallback).
    /// 3. Add reader to endpoint; switch to reader succeeds.
    /// 4. Switch back to writer succeeds.
    /// 5. Remove reader from endpoint; switch to reader falls back to writer.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous unit test.</returns>
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task CustomEndpoint_ReadWriteSplitting_WithCustomEndpointChanges_WithWriterAsInitConn()
    {
        Assert.SkipWhen(NumberOfInstances < 3, "Skipped due to test requiring number of database instances >= 3.");
        Assert.SkipWhen(this._fixture.EndpointInfo == null, "Custom endpoint fixture not created (not Aurora or < 3 instances).");

        await this.SetupCustomEndpointRoleAsync(HostRole.Writer);

        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            this._fixture.EndpointInfo!.Endpoint,
            Port,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            "customEndpoint,readWriteSplitting,failover",
            false);
        connectionString += $"; ClusterInstanceHostPattern=?.{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointSuffix}:{TestEnvironment.Env.Info.DatabaseInfo.InstanceEndpointPort}";
        connectionString += $"; {PropertyDefinition.CustomEndpointMonitorIdleExpirationMs.Name}=30000";
        connectionString += $"; {PropertyDefinition.WaitForCustomEndpointInfoTimeoutMs.Name}=30000";

        using var connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        var endpointMembers = this._fixture.EndpointInfo.StaticMembers ?? new List<string>();
        var originalWriterId = await AuroraUtils.QueryInstanceId(connection, true);
        Assert.True(endpointMembers.Contains(originalWriterId!), $"Instance {originalWriterId} should be in endpoint members");

        this._logger.WriteLine("Initial connection is to the writer. Attempting to switch to reader...");
        await AuroraUtils.SetReadOnly(connection, Engine, true, true);
        var newInstanceId = await AuroraUtils.QueryInstanceId(connection, true);
        Assert.Equal(originalWriterId, newInstanceId);

        var writerId = await AuroraUtils.GetDBClusterWriterInstanceIdAsync(TestEnvironment.Env.Info.RdsDbName!);
        string readerIdToAdd = TestEnvironment.Env.Info.DatabaseInfo.Instances
            .First(i => i.InstanceId != writerId).InstanceId;

        await AuroraUtils.ModifyDBClusterEndpointAsync(this._fixture.EndpointId, [originalWriterId!, readerIdToAdd]);

        try
        {
            await AuroraUtils.WaitUntilEndpointHasMembersAsync(this._fixture.EndpointId, new HashSet<string> { originalWriterId!, readerIdToAdd });

            await AuroraUtils.SetReadOnly(connection, Engine, true, true);
            newInstanceId = await AuroraUtils.QueryInstanceId(connection, true);
            Assert.Equal(readerIdToAdd, newInstanceId);

            await AuroraUtils.SetReadOnly(connection, Engine, false, true);
        }
        finally
        {
            await AuroraUtils.ModifyDBClusterEndpointAsync(this._fixture.EndpointId, [originalWriterId!]);
            await AuroraUtils.WaitUntilEndpointHasMembersAsync(this._fixture.EndpointId, new HashSet<string> { originalWriterId! });
        }

        this._logger.WriteLine("Reader removed from endpoint. Attempting to switch to reader should fallback to writer...");
        await AuroraUtils.SetReadOnly(connection, Engine, true, true);
        newInstanceId = await AuroraUtils.QueryInstanceId(connection, true);
        Assert.Equal(originalWriterId, newInstanceId);
    }
}
