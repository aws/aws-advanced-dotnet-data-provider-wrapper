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

/// <summary>
/// Integration tests for the <c>gdbReadWriteSplitting</c> plugin. The plugin extends the regular
/// <c>readWriteSplitting</c> plugin with home-region restrictions, but on a single-region Aurora
/// cluster the home region auto-detects from the endpoint URL and all instances live in that region,
/// so the restrict-writer/restrict-reader checks (default true) are trivially satisfied. As a result,
/// the basic happy-path scenarios verified by <see cref="ReadWriteSplittingTests"/> apply equally
/// to the GDB variant. This class mirrors those scenarios with <c>gdbReadWriteSplitting</c> as the
/// plugin code so we get CI coverage of the plugin chain wiring, factory registration, and
/// <c>InitializeWriterConnection</c> / <c>GetReaderHostCandidates</c> overrides on real clusters.
///
/// True cross-region behavior (writer outside home region, Global Write Forwarding, secondary
/// region connection) requires an actual Global Aurora cluster and is covered by
/// <see cref="GdbReadWriteSplittingLocalTests"/>.
/// </summary>
public class GdbReadWriteSplittingTests : IntegrationTestBase
{
    private const string PluginCode = "gdbReadWriteSplitting";

    private readonly ITestOutputHelper logger;
    protected override bool MakeSureFirstInstanceWriter => true;

    public GdbReadWriteSplittingTests(ITestOutputHelper output)
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
            PluginCode);

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

        // Idempotent: a second read-write set should keep us on the writer.
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
            PluginCode);

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
            PluginCode);

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
            PluginCode);

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

    /// <summary>
    /// Verifies that the GDB read/write splitting plugin auto-detects the home region from the
    /// endpoint URL and that switching to read-only successfully selects a reader. With default
    /// <c>gdbRwRestrictReaderToHomeRegion=true</c>, every reader on a single-region cluster is in
    /// the home region by definition, so the candidate filter is a no-op and the behavior matches
    /// the regular read/write splitting plugin.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task GdbHomeRegionDefaults_SwitchToReadOnly_SelectsLocalReader(bool async)
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
            PluginCode);

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);

        var writerId = await AuroraUtils.QueryInstanceId(connection, async);

        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        var readerId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.NotEqual(writerId, readerId);

        // Verify the picked reader is one of the cluster's readers (i.e. the home-region filter
        // didn't silently drop a valid candidate).
        var clusterReaderIds = TestEnvironment.Env.Info.DatabaseInfo!.Instances
            .Skip(1)
            .Select(i => i.InstanceId)
            .ToHashSet();
        Assert.Contains(readerId, clusterReaderIds);
    }

    /// <summary>
    /// Verifies that an explicit <c>GdbRwHomeRegion</c> matching the cluster's region is accepted
    /// and the plugin behaves identically to the auto-detected case.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task GdbExplicitHomeRegion_SwitchToReadOnly(bool async)
    {
        Assert.SkipWhen(NumberOfInstances < 2, "Skipped due to test requiring number of database instances >= 2.");
        var region = TestEnvironment.Env.Info.Region;
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
            PluginCode);
        connectionString += $"; GdbRwHomeRegion={region}";

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);

        var writerId = await AuroraUtils.QueryInstanceId(connection, async);

        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        var readerId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.NotEqual(writerId, readerId);
    }

    /// <summary>
    /// Verifies that turning the restrict flags off does not break the happy path on a
    /// single-region cluster.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("Category", "Integration")]
    [Trait("Database", "mysql")]
    [Trait("Database", "pg")]
    [Trait("Engine", "aurora")]
    public async Task GdbRestrictFlagsOff_SwitchToReadOnly(bool async)
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
            PluginCode);
        connectionString += "; GdbRwRestrictWriterToHomeRegion=false; GdbRwRestrictReaderToHomeRegion=false";

        using AwsWrapperConnection connection = AuroraUtils.CreateAwsWrapperConnection(Engine, connectionString);
        await AuroraUtils.OpenDbConnection(connection, async);

        var writerId = await AuroraUtils.QueryInstanceId(connection, async);

        await AuroraUtils.SetReadOnly(connection, Engine, true, async);
        var readerId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.NotEqual(writerId, readerId);

        await AuroraUtils.SetReadOnly(connection, Engine, false, async);
        var afterId = await AuroraUtils.QueryInstanceId(connection, async);
        Assert.Equal(writerId, afterId);
    }
}
