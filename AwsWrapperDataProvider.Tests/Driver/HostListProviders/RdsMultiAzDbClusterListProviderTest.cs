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
using Apps72.Dev.Data.DbMocker;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.HostListProviders;

public class RdsMultiAzDbClusterListProviderTest : IDisposable
{
    private const string TestHost = "test-cluster.cluster-xyz.us-east-2.rds.amazonaws.com";
    private const string WriterNodeId = "writer-node-id";
    private const string ReaderNodeId1 = "reader-node-id-1";
    private const string ReaderNodeId2 = "reader-node-id-2";
    private const string WriterEndpoint = "writer-instance.xyz.us-east-2.rds.amazonaws.com";
    private const string ReaderEndpoint1 = "reader-instance-1.xyz.us-east-2.rds.amazonaws.com";
    private const string ReaderEndpoint2 = "reader-instance-2.xyz.us-east-2.rds.amazonaws.com";
    private const string TopologyQuery = "SELECT endpoint, id, port FROM topology_table";
    private const string NodeIdQuery = "SELECT node_id FROM current_node";
    private const string IsReaderQuery = "SELECT is_reader FROM node_info";
    private const string FetchWriterNodeQuery = "SELECT writer_node_id FROM cluster_info";
    private const string FetchWriterNodeQueryHeader = "writer_node_id";

    private readonly Dictionary<string, string> properties;
    private readonly Mock<IHostListProviderService> mockHostListProviderService;
    private readonly MockDbConnection mockConnection;

    private readonly TimeSpan topologyRefreshRate = TimeSpan.FromSeconds(5);

    public RdsMultiAzDbClusterListProviderTest()
    {
        this.properties = new Dictionary<string, string>
        {
            { PropertyDefinition.Host.Name, TestHost },
            { PropertyDefinition.ClusterInstanceHostPattern.Name, "?.cluster-xyz.us-east-2.rds.amazonaws.com" },
        };
        this.mockHostListProviderService = new Mock<IHostListProviderService>();
        this.mockHostListProviderService.Setup(s => s.HostSpecBuilder).Returns(new HostSpecBuilder());

        this.mockConnection = new MockDbConnection();
        this.mockConnection.Open();
    }

    public void Dispose()
    {
        RdsHostListProvider.ClearAll();
    }

    private Mock<RdsMultiAzDbClusterListProvider> GetMultiAzRdsHostListProviderSpy()
    {
        return this.GetMultiAzRdsHostListProviderSpy(null);
    }

    private Mock<RdsMultiAzDbClusterListProvider> GetMultiAzRdsHostListProviderSpy(string? host)
    {
        if (host != null)
        {
            this.properties[PropertyDefinition.Host.Name] = host;
        }

        var multiAzRdsHostListProviderSpy = new Mock<RdsMultiAzDbClusterListProvider>(
            this.properties,
            this.mockHostListProviderService.Object,
            TopologyQuery,
            NodeIdQuery,
            IsReaderQuery,
            FetchWriterNodeQuery,
            FetchWriterNodeQueryHeader)
        {
            CallBase = true,
        };
        multiAzRdsHostListProviderSpy.Object.EnsureInitialized();
        return multiAzRdsHostListProviderSpy;
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task QueryForTopology_WithWriterNodeFromFetchQuery_ReturnsCorrectTopology()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();

        this.mockConnection.Mocks.When(cmd => cmd.CommandText == FetchWriterNodeQuery)
            .ReturnsTable(MockTable.WithColumns(FetchWriterNodeQueryHeader)
            .AddRow(WriterNodeId));
        this.mockConnection.Mocks.When(cmd => cmd.CommandText == TopologyQuery)
            .ReturnsTable(MockTable.WithColumns("endpoint", "id", "port")
            .AddRow(WriterEndpoint, WriterNodeId, 5432)
            .AddRow(ReaderEndpoint1, ReaderNodeId1, 5432)
            .AddRow(ReaderEndpoint2, ReaderNodeId2, 5432));

        var result = await multiAzProviderSpy.Object.QueryForTopologyAsync(this.mockConnection);

        Assert.NotNull(result);
        Assert.Equal(3, result.Count);

        var writer = result.FirstOrDefault(h => h.Role == HostRole.Writer);
        var readers = result.Where(h => h.Role == HostRole.Reader).ToList();

        Assert.NotNull(writer);
        Assert.Equal(2, readers.Count);
        Assert.Equal("writer-instance.cluster-xyz.us-east-2.rds.amazonaws.com", writer.Host);
        Assert.Contains(readers, r => r.Host == "reader-instance-1.cluster-xyz.us-east-2.rds.amazonaws.com");
        Assert.Contains(readers, r => r.Host == "reader-instance-2.cluster-xyz.us-east-2.rds.amazonaws.com");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task QueryForTopology_WithNoWriterNodeFound_ReturnsEmptyList()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();

        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.Empty());

        var result = await multiAzProviderSpy.Object.QueryForTopologyAsync(this.mockConnection);

        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task QueryForTopology_WithNullWriterNodeId_ReturnsEmptyList()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();

        this.mockConnection.Mocks.When(cmd => cmd.CommandText == FetchWriterNodeQuery)
            .ReturnsTable(MockTable.WithColumns(FetchWriterNodeQueryHeader, "unrelated_column")
            .AddRow(null, 1));
        this.mockConnection.Mocks.WhenAny().ReturnsTable(MockTable.Empty());

        var result = await multiAzProviderSpy.Object.QueryForTopologyAsync(this.mockConnection);

        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task QueryForTopology_WithMissingEndpoint_ThrowsDataException()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();

        this.mockConnection.Mocks.When(cmd => cmd.CommandText == FetchWriterNodeQuery)
            .ReturnsTable(MockTable.WithColumns(FetchWriterNodeQueryHeader)
            .AddRow(WriterNodeId));
        this.mockConnection.Mocks.When(cmd => cmd.CommandText == TopologyQuery)
            .ReturnsTable(MockTable.WithColumns("endpoint", "id", "port")
            .AddRow(null, WriterNodeId, 5432));

        await Assert.ThrowsAsync<NullReferenceException>(async () =>
            await multiAzProviderSpy.Object.QueryForTopologyAsync(this.mockConnection));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task QueryForTopology_WithNoWriterInstance_ReturnsEmptyList()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();

        this.mockConnection.Mocks.When(cmd => cmd.CommandText == FetchWriterNodeQuery)
            .ReturnsTable(MockTable.WithColumns(FetchWriterNodeQueryHeader)
            .AddRow("non-existent-writer"));
        this.mockConnection.Mocks.When(cmd => cmd.CommandText == TopologyQuery)
            .ReturnsTable(MockTable.WithColumns("endpoint", "id", "port")
            .AddRow(ReaderEndpoint1, ReaderNodeId1, 5432)
            .AddRow(ReaderEndpoint2, ReaderNodeId2, 5432));

        var result = await multiAzProviderSpy.Object.QueryForTopologyAsync(this.mockConnection);

        Assert.NotNull(result);
        Assert.Empty(result); // Should return empty list when no writer found
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTopology_ReturnsCachedTopology()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();
        var cachedHosts = new List<HostSpec>
        {
            new HostSpecBuilder().WithHost("cached-writer").WithRole(HostRole.Writer).Build(),
            new HostSpecBuilder().WithHost("cached-reader").WithRole(HostRole.Reader).Build(),
        };

        RdsHostListProvider.TopologyCache.Set(multiAzProviderSpy.Object.ClusterId, cachedHosts, this.topologyRefreshRate);

        var result = await multiAzProviderSpy.Object.GetTopologyAsync(this.mockConnection, false);

        Assert.Equal(cachedHosts, result.Hosts);
        Assert.True(result.IsCachedData);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTopology_WithForceUpdate_ReturnsUpdatedTopology()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();
        var cachedHosts = new List<HostSpec>
        {
            new HostSpecBuilder().WithHost("cached-writer").WithRole(HostRole.Writer).Build(),
        };
        var expectedHost = "fresh-writer.cluster-xyz.us-east-2.rds.amazonaws.com";

        RdsHostListProvider.TopologyCache.Set(multiAzProviderSpy.Object.ClusterId, cachedHosts, this.topologyRefreshRate);

        this.mockConnection.Mocks.When(cmd => cmd.CommandText == FetchWriterNodeQuery)
            .ReturnsTable(MockTable.WithColumns(FetchWriterNodeQueryHeader)
            .AddRow(WriterNodeId));
        this.mockConnection.Mocks.When(cmd => cmd.CommandText == TopologyQuery)
            .ReturnsTable(MockTable.WithColumns("endpoint", "id", "port")
            .AddRow(expectedHost, WriterNodeId, 5432));

        var result = await multiAzProviderSpy.Object.GetTopologyAsync(this.mockConnection, true);

        Assert.NotEqual(cachedHosts, result.Hosts);
        Assert.False(result.IsCachedData);
        Assert.Single(result.Hosts);
        Assert.Equal(expectedHost, result.Hosts[0].Host);
    }
}
