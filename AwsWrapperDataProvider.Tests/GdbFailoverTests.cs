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
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Tests.Container.Utils;

namespace AwsWrapperDataProvider.Tests;

public class GdbFailoverTests : IntegrationTestBase
{
    private readonly ITestOutputHelper logger;

    protected override bool MakeSureFirstInstanceWriter => true;

    public GdbFailoverTests(ITestOutputHelper output)
    {
        this.logger = output;
    }

    /// <summary>
    /// Writer failover using gdbFailover plugin with strict-writer mode.
    /// Current writer dies, driver failover occurs when executing a method against the connection.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task WriterFailover_FailOnConnectionInvocation(bool async)
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
            10,
            "gdbFailover");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo!.InstanceEndpointSuffix}:{ProxyDatabaseInfo!.InstanceEndpointPort}" +
            $"; ActiveHomeFailoverMode=strict-writer" +
            $"; InactiveHomeFailoverMode=strict-writer";

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var crashTask = AuroraUtils.CrashInstance(currentWriter, tcs);

        // Wait for simulation to start
        await tcs.Task;

        await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
        {
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
            await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, async);
        });

        await crashTask;

        Assert.NotNull(await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, false));
    }

    /// <summary>
    /// Reader failover using gdbFailover plugin with home-reader-or-writer mode.
    /// Current writer dies via network outage, driver fails over to any available host.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task ReaderFailover_ReaderOrWriter(bool async)
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
            10,
            "gdbFailover");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo!.InstanceEndpointSuffix}:{ProxyDatabaseInfo!.InstanceEndpointPort}" +
            $"; ActiveHomeFailoverMode=home-reader-or-writer" +
            $"; InactiveHomeFailoverMode=home-reader-or-writer";

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        await ProxyHelper.DisableConnectivityAsync(currentWriter);

        await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
        {
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
            await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, async);
        });
    }

    /// <summary>
    /// Reader failover using gdbFailover plugin with strict-home-reader mode.
    /// Current writer dies, driver fails over to a reader instance.
    /// Asserts the connected instance after failover is a reader.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task ReaderFailover_StrictReader(bool async)
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
            10,
            "gdbFailover");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo!.InstanceEndpointSuffix}:{ProxyDatabaseInfo!.InstanceEndpointPort}" +
            $"; ActiveHomeFailoverMode=strict-home-reader" +
            $"; InactiveHomeFailoverMode=strict-home-reader";

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var crashTask = AuroraUtils.CrashInstance(currentWriter, tcs);

        // Wait for simulation to start
        await tcs.Task;

        await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
        {
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
            await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, async);
        });

        // Assert that we are currently connected to a reader instance.
        var currentConnectionId = await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, async);
        Assert.NotNull(currentConnectionId);
        Assert.False(await AuroraUtils.IsDBInstanceWriterAsync(currentConnectionId));

        await crashTask;
    }

    /// <summary>
    /// Reader failover using gdbFailover plugin with home-reader-or-writer mode.
    /// Simulates a temporary failure so the writer is re-elected.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    public async Task ReaderFailover_WriterReelected(bool async)
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
            10,
            "gdbFailover");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo!.InstanceEndpointSuffix}:{ProxyDatabaseInfo!.InstanceEndpointPort}" +
            $"; ActiveHomeFailoverMode=home-reader-or-writer" +
            $"; InactiveHomeFailoverMode=home-reader-or-writer";

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var simulationTask = AuroraUtils.SimulateTemporaryFailureTask(currentWriter, TimeSpan.Zero, TimeSpan.FromSeconds(12), tcs);

        // Wait for the simulation to start
        await tcs.Task;
        await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
        {
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
            await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, async);
        });

        await simulationTask;
    }
}
