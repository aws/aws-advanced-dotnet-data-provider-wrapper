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
using AwsWrapperDataProvider.Driver.Plugins;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Plugin.CustomEndpoint.CustomEndpoint;
using AwsWrapperDataProvider.Tests.Container.Utils;
using MySqlConnector;
using Npgsql;

namespace AwsWrapperDataProvider.Tests;

/// <summary>
/// Fixture that creates a custom DB cluster endpoint before tests and deletes it after.
/// Used only for Aurora with 3+ instances (test is skipped otherwise).
/// </summary>
public class CustomEndpointTestFixture : IDisposable
{
    public CustomEndpointTestFixture()
    {
        EndpointId = "test-endpoint-1-" + Guid.NewGuid().ToString("N")[..8];
        EndpointInfo = null;

        var envInfo = TestEnvironment.Env.Info;
        if (envInfo.Request.Deployment != DatabaseEngineDeployment.AURORA || envInfo.DatabaseInfo.Instances.Count < 3)
        {
            return;
        }

        var clusterId = envInfo.RdsDbName!;
        var instances = envInfo.DatabaseInfo.Instances;
        var staticMembers = instances.Take(1).Select(i => i.InstanceId).ToList();

        var auroraUtilForCreate = AuroraTestUtils.GetUtility(envInfo);
        auroraUtilForCreate.CreateDBClusterEndpointAsync(EndpointId, clusterId, staticMembers).GetAwaiter().GetResult();
        EndpointInfo = auroraUtilForCreate.WaitUntilEndpointAvailableAsync(EndpointId).GetAwaiter().GetResult();
    }

    public string EndpointId { get; }
    public DBClusterEndpoint? EndpointInfo { get; }

    public void Dispose()
    {
        if (EndpointInfo == null)
        {
            return;
        }

        var auroraUtil = AuroraTestUtils.GetUtility();
        auroraUtil.DeleteDBClusterEndpointAsync(EndpointId).GetAwaiter().GetResult();
    }
}

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
        _fixture = fixture;
        _logger = output;
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
    [Fact]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task CustomEndpoint_Failover()
    {
        Assert.SkipWhen(NumberOfInstances < 3, "Skipped due to test requiring number of database instances >= 3.");
        Assert.SkipWhen(_fixture.EndpointInfo == null, "Custom endpoint fixture not created (not Aurora or < 3 instances).");

        var dbInfo = TestEnvironment.Env.Info.DatabaseInfo;
        var port = Port;
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            _fixture.EndpointInfo.Endpoint,
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

        var endpointMembers = _fixture.EndpointInfo.StaticMembers ?? new List<string>();
        var instanceId = AuroraUtils.QueryInstanceId(connection);
        Assert.True(endpointMembers.Contains(instanceId!), $"Instance {instanceId} should be in endpoint members: [{string.Join(", ", endpointMembers)}]");

        var currentWriter = TestEnvironment.Env.Info.DatabaseInfo.Instances[0].InstanceId;
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        if (instanceId == currentWriter)
        {
            var crashTask = AuroraUtils.CrashInstance(currentWriter, tcs);
            await tcs.Task;
            await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
            {
                _logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
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
                _logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
                await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, true);
            });
        }

        var newInstanceId = AuroraUtils.QueryInstanceId(connection);
        Assert.True(endpointMembers.Contains(newInstanceId!), $"New instance {newInstanceId} should be in endpoint members: [{string.Join(", ", endpointMembers)}]");
    }
}
