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
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Plugins.Failover;
using AwsWrapperDataProvider.Driver.Plugins.ReadWriteSplitting;
using AwsWrapperDataProvider.Tests.Container.Utils;

namespace AwsWrapperDataProvider.Tests;
public class ReadWriteSplittingTests : IntegrationTestBase
{
    private readonly ITestOutputHelper logger;
    protected override bool MakeSureFirstInstanceWriter => true;

    public ReadWriteSplittingTests(ITestOutputHelper output)
    {
        this.logger = output;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task ConnectToWriter_SwitchToReadOnly(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");
        var writer = TestEnvironment.Env.Info.DatabaseInfo!.Instances.First();
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            writer.Host,
            writer.Port,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            "readWriteSplitting");

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        var writerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);

        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        var readerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.NotEqual(writerConnectionId, readerConnectionId);

        await AuroraUtils.SetReadOnly(connection, Engine, false, async);
        var currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.Equal(writerConnectionId, currentConnectionId);

        await AuroraUtils.SetReadOnly(connection, Engine, false, async);
        currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.Equal(writerConnectionId, currentConnectionId);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task ConnectToReader_SwitchToReadWrite(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");
        var reader = TestEnvironment.Env.Info.DatabaseInfo!.Instances[1]!;
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            reader.Host,
            reader.Port,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            "readWriteSplitting");

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        var readerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);

        await AuroraUtils.SetReadOnly(connection, Engine, false, async);
        var writerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.NotEqual(writerConnectionId, readerConnectionId);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task ConnectToReaderCluster_SwitchToReadWrite(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            TestEnvironment.Env.Info.DatabaseInfo.ClusterReadOnlyEndpoint,
            TestEnvironment.Env.Info.DatabaseInfo.ClusterReadOnlyEndpointPort,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            "readWriteSplitting");

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        var readerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);

        await AuroraUtils.SetReadOnly(connection, Engine, false, async);
        var writerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.NotEqual(writerConnectionId, readerConnectionId);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task ConnectToWriter_SwitchToReadWrite_ReadOnlyTransaction(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");
        var writer = TestEnvironment.Env.Info.DatabaseInfo!.Instances.First();
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            writer.Host,
            writer.Port,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            "readWriteSplitting");

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        var writerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);

        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        var readerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.NotEqual(writerConnectionId, readerConnectionId);

        await using var tx = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await AuroraUtils.ExecuteQuery(connection, "SELECT 1", async, tx);
        await Assert.ThrowsAsync<ReadWriteSplittingDbException>(async () =>
        {
            await AuroraUtils.SetReadOnly(connection, Engine, false, async, tx);
        });

        var currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async, tx);
        Assert.Equal(readerConnectionId, currentConnectionId);

        await tx.CommitAsync(TestContext.Current.CancellationToken);

        await AuroraUtils.SetReadOnly(connection, Engine, false, async);
        currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.Equal(writerConnectionId, currentConnectionId);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task MySQL_ConnectToWriter_SwitchToReadOnlyInTransaction(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");
        var writer = TestEnvironment.Env.Info.DatabaseInfo!.Instances.First();
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            writer.Host,
            writer.Port,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            "readWriteSplitting");

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        var writerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);

        await AuroraUtils.ExecuteNonQuery(connection, "DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyTrueInTransaction", async);
        await AuroraUtils.ExecuteNonQuery(connection, "CREATE TABLE test_readWriteSplitting_readOnlyTrueInTransaction (id int not null primary key, text_field varchar(255) not null)", async);

        await using var tx = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await AuroraUtils.ExecuteNonQuery(connection, "INSERT INTO test_readWriteSplitting_readOnlyTrueInTransaction VALUES (1, 'test_field value 1')", async, tx);

        // Will not switch to read-only in a transaction
        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        var currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.Equal(writerConnectionId, currentConnectionId);

        await tx.CommitAsync(TestContext.Current.CancellationToken);

        var count = Convert.ToInt32(await AuroraUtils.ExecuteQuery(connection, "SELECT count(*) from test_readWriteSplitting_readOnlyTrueInTransaction", async));
        Assert.Equal(1, count);

        await AuroraUtils.SetReadOnly(connection, Engine, false, async);
        await AuroraUtils.ExecuteNonQuery(connection, "DROP TABLE IF EXISTS test_readWriteSplitting_readOnlyTrueInTransaction", async);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task ConnectToProxyWriter_SwitchToReadOnly_AllReadersDown(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 3, "Skipped due to test requiring number of database instances >= 3.");
        var proxyWriter = ProxyDatabaseInfo.Instances.First();
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            proxyWriter.Host,
            proxyWriter.Port,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            "readWriteSplitting");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}";

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        var writerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);

        await ProxyHelper.DisableConnectivityAsync(ProxyDatabaseInfo.ClusterReadOnlyEndpoint);
        foreach (var instance in ProxyDatabaseInfo.Instances.Skip(1))
        {
            await ProxyHelper.DisableConnectivityAsync(instance.InstanceId);
        }

        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        var currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.Equal(writerConnectionId, currentConnectionId);

        await AuroraUtils.SetReadOnly(connection, Engine, false, async);
        currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.Equal(writerConnectionId, currentConnectionId);

        await ProxyHelper.EnableAllConnectivityAsync();
        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.NotEqual(writerConnectionId, currentConnectionId);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task ConnectToProxyWriter_AllInstancesDown(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");
        var proxyWriter = ProxyDatabaseInfo.Instances.First();
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            proxyWriter.Host,
            proxyWriter.Port,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            "readWriteSplitting");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}";

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        var writerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);

        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        var readerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.NotEqual(writerConnectionId, readerConnectionId);

        await ProxyHelper.DisableAllConnectivityAsync();

        await Assert.ThrowsAsync<ReadWriteSplittingDbException>(async () =>
        {
            await AuroraUtils.SetReadOnly(connection, Engine, false, async);
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task ConnectToProxyReader_SwitchToReadOnly_AllInstancesDown(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            TestEnvironment.Env.Info.DatabaseInfo.ClusterReadOnlyEndpoint,
            TestEnvironment.Env.Info.DatabaseInfo.ClusterEndpointPort,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            "readWriteSplitting");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}";

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        await ProxyHelper.DisableAllConnectivityAsync();

        await Assert.ThrowsAsync<ReadWriteSplittingDbException>(async () =>
        {
            await AuroraUtils.SetReadOnly(connection, Engine, false, async);
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task ConnectToProxyWriter_WriterFailover(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 3, "Skipped due to test requiring number of database instances >= 3.");
        var proxyWriter = ProxyDatabaseInfo.Instances.First();
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            proxyWriter.Host,
            proxyWriter.Port,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            "failover,efm,readWriteSplitting");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}";

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        var originalWriterId = await AuroraUtils.QueryInstanceId(connection, async);

        await ProxyHelper.DisableConnectivityAsync(ProxyDatabaseInfo.ClusterReadOnlyEndpoint);
        foreach (var instance in ProxyDatabaseInfo.Instances.Skip(1))
        {
            await ProxyHelper.DisableConnectivityAsync(instance.InstanceId);
        }

        // Force internal reader connection to the writer instance
        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        var currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.Equal(originalWriterId, currentConnectionId);
        await AuroraUtils.SetReadOnly(connection, Engine, false, async);

        await ProxyHelper.EnableAllConnectivityAsync();

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var crashTask = AuroraUtils.CrashInstance(originalWriterId!, tcs);
        await tcs.Task;
        await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
        {
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
            await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, async);
        });

        var newWriterId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.True(await AuroraUtils.IsDBInstanceWriterAsync(newWriterId!));

        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.NotEqual(newWriterId, currentConnectionId);

        await AuroraUtils.SetReadOnly(connection, Engine, false, async);
        currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.Equal(newWriterId, currentConnectionId);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task ConnectToProxyWriter_FailoverToNewReader(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 3, "Skipped due to test requiring number of database instances >= 3.");
        var proxyWriter = ProxyDatabaseInfo.Instances.First();
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            proxyWriter.Host,
            proxyWriter.Port,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            "failover,efm,readWriteSplitting");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}" +
                            $"; FailoverMode=ReaderOrWriter";

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        var writerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);

        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        var readerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.NotEqual(writerConnectionId, readerConnectionId);

        string otherReaderId = string.Empty;
        foreach (var instance in TestEnvironment.Env.Info.DatabaseInfo.Instances.Skip(1))
        {
            if (instance.InstanceId != readerConnectionId)
            {
                otherReaderId = instance.InstanceId;
                break;
            }
        }

        foreach (var instance in TestEnvironment.Env.Info.DatabaseInfo.Instances)
        {
            if (instance.InstanceId != otherReaderId)
            {
                await ProxyHelper.DisableConnectivityAsync(instance.InstanceId);
            }
        }

        await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
        {
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
            await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, async);
        });

        Assert.Equal(ConnectionState.Open, connection.State);
        var currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.Equal(otherReaderId, currentConnectionId);
        Assert.NotEqual(readerConnectionId, currentConnectionId);

        await ProxyHelper.EnableAllConnectivityAsync();

        await AuroraUtils.SetReadOnly(connection, Engine, false, async);
        currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.Equal(writerConnectionId, currentConnectionId);

        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.Equal(otherReaderId, currentConnectionId);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    [Trait("Engine", "multi-az-cluster")]
    [Trait("Engine", "multi-az-instance")]
    public async Task ConnectToProxyWriter_FailoverReaderToWriter(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 3, "Skipped due to test requiring number of database instances >= 3.");
        var proxyWriter = ProxyDatabaseInfo.Instances.First();
        var connectionString = ConnectionStringHelper.GetUrl(
            Engine,
            proxyWriter.Host,
            proxyWriter.Port,
            Username,
            Password,
            DefaultDbName,
            3,
            10,
            "failover,efm,readWriteSplitting");
        connectionString += $"; ClusterInstanceHostPattern=?.{ProxyDatabaseInfo.InstanceEndpointSuffix}:{ProxyDatabaseInfo.InstanceEndpointPort}";

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);
        Assert.Equal(ConnectionState.Open, connection.State);

        var writerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        this.logger.WriteLine($"writerConnectionId={writerConnectionId}");

        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        var readerConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        this.logger.WriteLine($"readerConnectionId={readerConnectionId}");
        Assert.NotEqual(writerConnectionId, readerConnectionId);

        foreach (var instance in TestEnvironment.Env.Info.DatabaseInfo.Instances)
        {
            if (instance.InstanceId != writerConnectionId)
            {
                await ProxyHelper.DisableConnectivityAsync(instance.InstanceId);
            }
        }

        await ProxyHelper.DisableConnectivityAsync(ProxyDatabaseInfo.ClusterReadOnlyEndpoint);

        await Assert.ThrowsAsync<FailoverSuccessException>(async () =>
        {
            this.logger.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} Executing instance ID query to trigger failover...");
            await AuroraUtils.ExecuteInstanceIdQuery(connection, Engine, Deployment, async);
        });
        Assert.Equal(ConnectionState.Open, connection.State);
        var currentConnectionId = await AuroraUtils.QueryInstanceId(connection, async);
        this.logger.WriteLine($"currentConnectionId={currentConnectionId}");
        Assert.Equal(writerConnectionId, currentConnectionId);

        await ProxyHelper.EnableAllConnectivityAsync();
    }
}
