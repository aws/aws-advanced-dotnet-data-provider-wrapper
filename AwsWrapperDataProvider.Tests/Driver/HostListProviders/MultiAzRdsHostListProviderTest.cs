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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Tests.Driver.Dialects;
using Microsoft.Extensions.Caching.Memory;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.HostListProviders;

public class MultiAzRdsHostListProviderTest : IDisposable
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
    private readonly Mock<IDbConnection> mockConnection;
    private readonly Mock<IDbCommand> mockCommand;
    private readonly Mock<IDataReader> mockReader;

    private readonly TimeSpan topologyRefreshRate = TimeSpan.FromSeconds(5);

    public MultiAzRdsHostListProviderTest()
    {
        this.properties = new Dictionary<string, string>
        {
            { PropertyDefinition.Host.Name, TestHost },
            { PropertyDefinition.ClusterInstanceHostPattern.Name, "?.cluster-xyz.us-east-2.rds.amazonaws.com" },
        };
        this.mockHostListProviderService = new Mock<IHostListProviderService>();
        this.mockHostListProviderService.Setup(s => s.HostSpecBuilder).Returns(new HostSpecBuilder());

        this.mockConnection = new Mock<IDbConnection>();
        this.mockCommand = new Mock<IDbCommand>();
        this.mockReader = new Mock<IDataReader>();

        this.mockConnection.Setup(c => c.State).Returns(ConnectionState.Open);
        this.mockConnection.Setup(c => c.CreateCommand()).Returns(this.mockCommand.Object);
        this.mockCommand.Setup(c => c.ExecuteReader()).Returns(this.mockReader.Object);
    }

    public void Dispose()
    {
        RdsHostListProvider.ClearAll();
    }

    private Mock<MultiAzRdsHostListProvider> GetMultiAzRdsHostListProviderSpy()
    {
        return this.GetMultiAzRdsHostListProviderSpy(null);
    }

    private Mock<MultiAzRdsHostListProvider> GetMultiAzRdsHostListProviderSpy(string? host)
    {
        if (host != null)
        {
            this.properties[PropertyDefinition.Host.Name] = host;
        }

        var multiAzRdsHostListProviderSpy = new Mock<MultiAzRdsHostListProvider>(
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
    public void QueryForTopology_WithWriterNodeFromFetchQuery_ReturnsCorrectTopology()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();

        // Mock the sequence of commands that will be created
        var mockWriterNodeCommand = new Mock<IDbCommand>();
        var mockTopologyCommand = new Mock<IDbCommand>();
        var mockWriterNodeReader = new Mock<IDataReader>();
        var mockTopologyReader = new Mock<IDataReader>();

        this.mockConnection.SetupSequence(c => c.CreateCommand())
            .Returns(mockWriterNodeCommand.Object)
            .Returns(mockTopologyCommand.Object);

        mockWriterNodeCommand.Setup(c => c.ExecuteReader()).Returns(mockWriterNodeReader.Object);
        mockTopologyCommand.Setup(c => c.ExecuteReader()).Returns(mockTopologyReader.Object);

        // Setup writer node query to return writer node ID
        mockWriterNodeReader.Setup(r => r.Read()).Returns(true);
        mockWriterNodeReader.Setup(r => r.GetOrdinal(FetchWriterNodeQueryHeader)).Returns(0);
        mockWriterNodeReader.Setup(r => r.IsDBNull(0)).Returns(false);
        mockWriterNodeReader.Setup(r => r.GetString(0)).Returns(WriterNodeId);

        // Setup topology query results
        mockTopologyReader.SetupSequence(r => r.Read())
            .Returns(true) // Writer
            .Returns(true) // Reader 1
            .Returns(true) // Reader 2
            .Returns(false); // End

        mockTopologyReader.Setup(r => r.GetOrdinal("endpoint")).Returns(0);
        mockTopologyReader.Setup(r => r.GetOrdinal("id")).Returns(1);
        mockTopologyReader.Setup(r => r.GetOrdinal("port")).Returns(2);

        mockTopologyReader.Setup(r => r.IsDBNull(0)).Returns(false);
        mockTopologyReader.Setup(r => r.IsDBNull(1)).Returns(false);
        mockTopologyReader.Setup(r => r.IsDBNull(2)).Returns(false);

        mockTopologyReader.SetupSequence(r => r.GetString(0))
            .Returns(WriterEndpoint)
            .Returns(ReaderEndpoint1)
            .Returns(ReaderEndpoint2);

        mockTopologyReader.SetupSequence(r => r.GetString(1))
            .Returns(WriterNodeId)
            .Returns(ReaderNodeId1)
            .Returns(ReaderNodeId2);

        mockTopologyReader.Setup(r => r.GetInt32(2)).Returns(5432);

        var result = multiAzProviderSpy.Object.QueryForTopology(this.mockConnection.Object);

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
    public void QueryForTopology_WithNoWriterNodeFound_ThrowsInvalidOperationException()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();

        var mockWriterNodeCommand = new Mock<IDbCommand>();
        var mockWriterNodeReader = new Mock<IDataReader>();

        this.mockConnection.Setup(c => c.CreateCommand()).Returns(mockWriterNodeCommand.Object);
        mockWriterNodeCommand.Setup(c => c.ExecuteReader()).Returns(mockWriterNodeReader.Object);

        // Setup writer node query to return no results
        mockWriterNodeReader.Setup(r => r.Read()).Returns(false);

        Assert.Throws<InvalidOperationException>(() =>
            multiAzProviderSpy.Object.QueryForTopology(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void QueryForTopology_WithNullWriterNodeId_ThrowsInvalidOperationException()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();

        var mockWriterNodeCommand = new Mock<IDbCommand>();
        var mockWriterNodeReader = new Mock<IDataReader>();

        this.mockConnection.Setup(c => c.CreateCommand()).Returns(mockWriterNodeCommand.Object);
        mockWriterNodeCommand.Setup(c => c.ExecuteReader()).Returns(mockWriterNodeReader.Object);

        // Setup writer node query to return null value
        mockWriterNodeReader.Setup(r => r.Read()).Returns(true);
        mockWriterNodeReader.Setup(r => r.GetOrdinal(FetchWriterNodeQueryHeader)).Returns(0);
        mockWriterNodeReader.Setup(r => r.IsDBNull(0)).Returns(true);

        Assert.Throws<InvalidOperationException>(() =>
            multiAzProviderSpy.Object.QueryForTopology(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void QueryForTopology_WithMissingEndpoint_ThrowsDataException()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();

        var mockWriterNodeCommand = new Mock<IDbCommand>();
        var mockTopologyCommand = new Mock<IDbCommand>();
        var mockWriterNodeReader = new Mock<IDataReader>();
        var mockTopologyReader = new Mock<IDataReader>();

        this.mockConnection.SetupSequence(c => c.CreateCommand())
            .Returns(mockWriterNodeCommand.Object)
            .Returns(mockTopologyCommand.Object);

        mockWriterNodeCommand.Setup(c => c.ExecuteReader()).Returns(mockWriterNodeReader.Object);
        mockTopologyCommand.Setup(c => c.ExecuteReader()).Returns(mockTopologyReader.Object);

        // Setup writer node query
        mockWriterNodeReader.Setup(r => r.Read()).Returns(true);
        mockWriterNodeReader.Setup(r => r.GetOrdinal(FetchWriterNodeQueryHeader)).Returns(0);
        mockWriterNodeReader.Setup(r => r.IsDBNull(0)).Returns(false);
        mockWriterNodeReader.Setup(r => r.GetString(0)).Returns(WriterNodeId);

        // Setup topology query with missing endpoint
        mockTopologyReader.Setup(r => r.Read()).Returns(true);
        mockTopologyReader.Setup(r => r.GetOrdinal("endpoint")).Returns(0);
        mockTopologyReader.Setup(r => r.GetOrdinal("id")).Returns(1);
        mockTopologyReader.Setup(r => r.GetOrdinal("port")).Returns(2);
        mockTopologyReader.Setup(r => r.IsDBNull(0)).Returns(true); // Missing endpoint

        Assert.Throws<DataException>(() =>
            multiAzProviderSpy.Object.QueryForTopology(this.mockConnection.Object));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void QueryForTopology_WithNoWriterInstance_ReturnsEmptyList()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();

        var mockWriterNodeCommand = new Mock<IDbCommand>();
        var mockTopologyCommand = new Mock<IDbCommand>();
        var mockWriterNodeReader = new Mock<IDataReader>();
        var mockTopologyReader = new Mock<IDataReader>();

        this.mockConnection.SetupSequence(c => c.CreateCommand())
            .Returns(mockWriterNodeCommand.Object)
            .Returns(mockTopologyCommand.Object);

        mockWriterNodeCommand.Setup(c => c.ExecuteReader()).Returns(mockWriterNodeReader.Object);
        mockTopologyCommand.Setup(c => c.ExecuteReader()).Returns(mockTopologyReader.Object);

        // Setup writer node query
        mockWriterNodeReader.Setup(r => r.Read()).Returns(true);
        mockWriterNodeReader.Setup(r => r.GetOrdinal(FetchWriterNodeQueryHeader)).Returns(0);
        mockWriterNodeReader.Setup(r => r.IsDBNull(0)).Returns(false);
        mockWriterNodeReader.Setup(r => r.GetString(0)).Returns("non-existent-writer");

        // Setup topology query with only readers (no matching writer)
        mockTopologyReader.SetupSequence(r => r.Read())
            .Returns(true) // Reader 1
            .Returns(true) // Reader 2
            .Returns(false); // End

        mockTopologyReader.Setup(r => r.GetOrdinal("endpoint")).Returns(0);
        mockTopologyReader.Setup(r => r.GetOrdinal("id")).Returns(1);
        mockTopologyReader.Setup(r => r.GetOrdinal("port")).Returns(2);

        mockTopologyReader.Setup(r => r.IsDBNull(0)).Returns(false);
        mockTopologyReader.Setup(r => r.IsDBNull(1)).Returns(false);
        mockTopologyReader.Setup(r => r.IsDBNull(2)).Returns(false);

        mockTopologyReader.SetupSequence(r => r.GetString(0))
            .Returns(ReaderEndpoint1)
            .Returns(ReaderEndpoint2);

        mockTopologyReader.SetupSequence(r => r.GetString(1))
            .Returns(ReaderNodeId1)
            .Returns(ReaderNodeId2);

        mockTopologyReader.Setup(r => r.GetInt32(2)).Returns(5432);

        var result = multiAzProviderSpy.Object.QueryForTopology(this.mockConnection.Object);

        Assert.NotNull(result);
        Assert.Empty(result); // Should return empty list when no writer found
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetTopology_ReturnsCachedTopology()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();
        var cachedHosts = new List<HostSpec>
        {
            new HostSpecBuilder().WithHost("cached-writer").WithRole(HostRole.Writer).Build(),
            new HostSpecBuilder().WithHost("cached-reader").WithRole(HostRole.Reader).Build(),
        };

        RdsHostListProvider.TopologyCache.Set(multiAzProviderSpy.Object.ClusterId, cachedHosts, this.topologyRefreshRate);

        var result = multiAzProviderSpy.Object.GetTopology(this.mockConnection.Object, false);

        Assert.Equal(cachedHosts, result.Hosts);
        Assert.True(result.IsCachedData);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetTopology_WithForceUpdate_ReturnsUpdatedTopology()
    {
        var multiAzProviderSpy = this.GetMultiAzRdsHostListProviderSpy();
        var cachedHosts = new List<HostSpec>
        {
            new HostSpecBuilder().WithHost("cached-writer").WithRole(HostRole.Writer).Build(),
        };

        RdsHostListProvider.TopologyCache.Set(multiAzProviderSpy.Object.ClusterId, cachedHosts, this.topologyRefreshRate);

        // Setup mock for QueryForTopology to return different results
        var mockWriterNodeCommand = new Mock<IDbCommand>();
        var mockTopologyCommand = new Mock<IDbCommand>();
        var mockWriterNodeReader = new Mock<IDataReader>();
        var mockTopologyReader = new Mock<IDataReader>();

        this.mockConnection.SetupSequence(c => c.CreateCommand())
            .Returns(mockWriterNodeCommand.Object)
            .Returns(mockTopologyCommand.Object);

        mockWriterNodeCommand.Setup(c => c.ExecuteReader()).Returns(mockWriterNodeReader.Object);
        mockTopologyCommand.Setup(c => c.ExecuteReader()).Returns(mockTopologyReader.Object);

        // Setup for fresh query
        mockWriterNodeReader.Setup(r => r.Read()).Returns(true);
        mockWriterNodeReader.Setup(r => r.GetOrdinal(FetchWriterNodeQueryHeader)).Returns(0);
        mockWriterNodeReader.Setup(r => r.IsDBNull(0)).Returns(false);
        mockWriterNodeReader.Setup(r => r.GetString(0)).Returns(WriterNodeId);

        mockTopologyReader.SetupSequence(r => r.Read()).Returns(true).Returns(false);
        mockTopologyReader.Setup(r => r.GetOrdinal("endpoint")).Returns(0);
        mockTopologyReader.Setup(r => r.GetOrdinal("id")).Returns(1);
        mockTopologyReader.Setup(r => r.GetOrdinal("port")).Returns(2);
        mockTopologyReader.Setup(r => r.IsDBNull(0)).Returns(false);
        mockTopologyReader.Setup(r => r.IsDBNull(1)).Returns(false);
        mockTopologyReader.Setup(r => r.IsDBNull(2)).Returns(false);
        mockTopologyReader.Setup(r => r.GetString(0)).Returns("fresh-writer.xyz.us-east-2.rds.amazonaws.com");
        mockTopologyReader.Setup(r => r.GetString(1)).Returns(WriterNodeId);
        mockTopologyReader.Setup(r => r.GetInt32(2)).Returns(5432);

        var result = multiAzProviderSpy.Object.GetTopology(this.mockConnection.Object, true);

        Assert.NotEqual(cachedHosts, result.Hosts);
        Assert.False(result.IsCachedData);
        Assert.Single(result.Hosts);
        Assert.Equal("fresh-writer.cluster-xyz.us-east-2.rds.amazonaws.com", result.Hosts[0].Host);
    }
}
