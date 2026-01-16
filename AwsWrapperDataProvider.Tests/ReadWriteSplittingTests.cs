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
}
