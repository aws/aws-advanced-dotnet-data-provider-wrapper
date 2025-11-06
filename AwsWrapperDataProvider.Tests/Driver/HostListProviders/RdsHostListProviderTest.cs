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

using System.Data.Common;
using Apps72.Dev.Data.DbMocker;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.HostListProviders;

public class RdsHostListProviderTest : IDisposable
{
    private const string ClusterA = "cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com";
    private const string Instance1 = "instance-a-1.xyz.us-east-2.rds.amazonaws.com";
    private const string Instance2 = "instance-a-2.xyz.us-east-2.rds.amazonaws.com";
    private const string Instance3 = "instance-a-3.xyz.us-east-2.rds.amazonaws.com";

    private readonly Dictionary<string, string> properties;
    private readonly Mock<IHostListProviderService> mockHostListProviderService;
    private readonly MockDbConnection mockConnection;
    private readonly List<HostSpec> clusterAHosts = [
        new HostSpecBuilder().WithHost(Instance1).WithRole(HostRole.Writer).Build(),
        new HostSpecBuilder().WithHost(Instance2).WithRole(HostRole.Reader).Build(),
        new HostSpecBuilder().WithHost(Instance3).WithRole(HostRole.Reader).Build(),
    ];

    private readonly TimeSpan topologyRefreshRate = TimeSpan.FromSeconds(5);

    public RdsHostListProviderTest()
    {
        this.properties = new Dictionary<string, string>
        {
            { PropertyDefinition.Host.Name, "test-host.example.com" },
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

    private Mock<RdsHostListProvider> GetRdsHostListProviderSpy()
    {
        return this.GetRdsHostListProviderSpy(null);
    }

    private Mock<RdsHostListProvider> GetRdsHostListProviderSpy(string? host)
    {
        if (host != null)
        {
            this.properties[PropertyDefinition.Host.Name] = host;
        }

        var rdsHostListProviderSpy = new Mock<RdsHostListProvider>(this.properties, this.mockHostListProviderService.Object, "foo", "bar", "baz")
        {
            CallBase = true,
        };
        rdsHostListProviderSpy.Object.EnsureInitialized();
        return rdsHostListProviderSpy;
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTopology_ReturnCachedTopology()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();

        RdsHostListProvider.TopologyCache.Set(rdsHostListProviderSpy.Object.ClusterId, this.clusterAHosts, this.topologyRefreshRate);

        var result = await rdsHostListProviderSpy.Object.GetTopologyAsync(this.mockConnection, false);
        Assert.Equal(this.clusterAHosts, result.Hosts);
        Assert.True(result.IsCachedData);
        rdsHostListProviderSpy.Verify(r => r.QueryForTopologyAsync(It.IsAny<DbConnection>()), Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTopology_WithForceUpdate_ReturnUpdatedTopology()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();

        RdsHostListProvider.TopologyCache.Set(rdsHostListProviderSpy.Object.ClusterId, this.clusterAHosts, this.topologyRefreshRate);
        List<HostSpec> newHosts = [new HostSpecBuilder().WithHost("newHost").Build()];
        rdsHostListProviderSpy.Setup(r => r.QueryForTopologyAsync(It.IsAny<DbConnection>())).ReturnsAsync(newHosts);

        var result = await rdsHostListProviderSpy.Object.GetTopologyAsync(this.mockConnection, true);
        Assert.Equal(newHosts, result.Hosts);
        Assert.False(result.IsCachedData);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTopology_WithoutForceUpdate_ReturnEmptyHostList()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();
        rdsHostListProviderSpy.Object.ClusterId = "cluster-id";

        RdsHostListProvider.TopologyCache.Set(rdsHostListProviderSpy.Object.ClusterId, this.clusterAHosts, this.topologyRefreshRate);
        rdsHostListProviderSpy.Setup(r => r.QueryForTopologyAsync(It.IsAny<DbConnection>())).ReturnsAsync([]);

        var result = await rdsHostListProviderSpy.Object.GetTopologyAsync(this.mockConnection, false);
        Assert.Equal(this.clusterAHosts, result.Hosts);
        Assert.True(result.IsCachedData);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTopology_WithForceUpdate_ReturnInitialHostList()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();

        rdsHostListProviderSpy.Setup(r => r.QueryForTopologyAsync(It.IsAny<DbConnection>())).ReturnsAsync([]);

        var result = await rdsHostListProviderSpy.Object.GetTopologyAsync(this.mockConnection, true);
        Assert.Equal([new HostSpecBuilder().WithHost(this.properties[PropertyDefinition.Host.Name]).Build()], result.Hosts);
        Assert.False(result.IsCachedData);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Refresh_SuggestedClusterIdForRds()
    {
        RdsHostListProvider.ClearAll();
        var rdsHostListProviderSpy1 = this.GetRdsHostListProviderSpy(ClusterA);

        rdsHostListProviderSpy1.Setup(r => r.QueryForTopologyAsync(It.IsAny<DbConnection>())).ReturnsAsync(this.clusterAHosts);
        Assert.Equal(0, RdsHostListProvider.TopologyCache.Count);

        var provider1Hosts = await rdsHostListProviderSpy1.Object.RefreshAsync(this.mockConnection);
        Assert.Equal(this.clusterAHosts, provider1Hosts);

        var rdsHostListProviderSpy2 = this.GetRdsHostListProviderSpy(ClusterA);

        Assert.Equal(rdsHostListProviderSpy1.Object.ClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.True(rdsHostListProviderSpy1.Object.IsPrimaryClusterId);
        Assert.True(rdsHostListProviderSpy2.Object.IsPrimaryClusterId);

        var provider2Hosts = await rdsHostListProviderSpy2.Object.RefreshAsync(this.mockConnection);
        Assert.Equal(this.clusterAHosts, provider2Hosts);

        Assert.Equal(1, RdsHostListProvider.TopologyCache.Count);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Refresh_SuggestedClusterIdForInstance()
    {
        RdsHostListProvider.ClearAll();
        var rdsHostListProviderSpy1 = this.GetRdsHostListProviderSpy(ClusterA);

        rdsHostListProviderSpy1.Setup(r => r.QueryForTopologyAsync(It.IsAny<DbConnection>())).ReturnsAsync(this.clusterAHosts);
        Assert.Equal(0, RdsHostListProvider.TopologyCache.Count);

        var provider1Hosts = await rdsHostListProviderSpy1.Object.RefreshAsync(this.mockConnection);
        Assert.Equal(this.clusterAHosts, provider1Hosts);

        var rdsHostListProviderSpy2 = this.GetRdsHostListProviderSpy(Instance3);

        Assert.Equal(rdsHostListProviderSpy1.Object.ClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.True(rdsHostListProviderSpy1.Object.IsPrimaryClusterId);
        Assert.True(rdsHostListProviderSpy2.Object.IsPrimaryClusterId);

        var provider2Hosts = await rdsHostListProviderSpy2.Object.RefreshAsync(this.mockConnection);
        Assert.Equal(this.clusterAHosts, provider2Hosts);

        Assert.Equal(1, RdsHostListProvider.TopologyCache.Count);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Refresh_AcceptSuggestion()
    {
        RdsHostListProvider.ClearAll();
        var rdsHostListProviderSpy1 = this.GetRdsHostListProviderSpy(Instance2);

        rdsHostListProviderSpy1.Setup(r => r.QueryForTopologyAsync(It.IsAny<DbConnection>())).ReturnsAsync(this.clusterAHosts);
        Assert.Equal(0, RdsHostListProvider.TopologyCache.Count);

        var provider1Hosts = await rdsHostListProviderSpy1.Object.RefreshAsync(this.mockConnection);
        Assert.Equal(this.clusterAHosts, provider1Hosts);

        var rdsHostListProviderSpy2 = this.GetRdsHostListProviderSpy(ClusterA);
        rdsHostListProviderSpy2.Setup(r => r.QueryForTopologyAsync(It.IsAny<DbConnection>())).ReturnsAsync(this.clusterAHosts);

        var provider2Hosts = await rdsHostListProviderSpy2.Object.RefreshAsync(this.mockConnection);
        Assert.Equal(this.clusterAHosts, provider2Hosts);

        Assert.NotEqual(rdsHostListProviderSpy1.Object.ClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.False(rdsHostListProviderSpy1.Object.IsPrimaryClusterId);
        Assert.True(rdsHostListProviderSpy2.Object.IsPrimaryClusterId);
        Assert.Equal(2, RdsHostListProvider.TopologyCache.Count);
        Assert.Equal(ClusterA, RdsHostListProvider.SuggestedPrimaryClusterIdCache.Get<string>(rdsHostListProviderSpy1.Object.ClusterId));

        provider1Hosts = await rdsHostListProviderSpy1.Object.ForceRefreshAsync(this.mockConnection);
        Assert.Equal(rdsHostListProviderSpy1.Object.ClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.True(rdsHostListProviderSpy1.Object.IsPrimaryClusterId);
        Assert.True(rdsHostListProviderSpy2.Object.IsPrimaryClusterId);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTopology_StaleRecord()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();
        string hostName1 = "hostName1";
        string hostName2 = "hostName2";
        double cpuUtilization = 11.1;
        double nodeLag = 0.123;
        DateTime firstTimestamp = DateTime.UtcNow;
        DateTime secondTimestamp = firstTimestamp.AddMinutes(1);
        long weight = (long)((Math.Round(nodeLag) * 100L) + Math.Round(cpuUtilization));
        HostSpec expcetedWriter = new HostSpecBuilder()
            .WithHost(hostName2)
            .WithPort(HostSpec.NoPort)
            .WithRole(HostRole.Writer)
            .WithAvailability(HostAvailability.Available)
            .WithWeight(weight)
            .WithLastUpdateTime(secondTimestamp)
            .Build();

        this.mockConnection.Mocks.WhenAny()
            .ReturnsTable(MockTable.WithColumns("SERVER_ID", "CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END", "CPU", "REPLICA_LAG_IN_MILLISECONDS", "LAST_UPDATE_TIMESTAMP")
            .AddRow(hostName1, true, cpuUtilization, nodeLag, firstTimestamp)
            .AddRow(hostName2, true, cpuUtilization, nodeLag, secondTimestamp));

        var result = await rdsHostListProviderSpy.Object.GetTopologyAsync(this.mockConnection, true);
        Assert.Single(result.Hosts);
        Assert.Equal(expcetedWriter, result.Hosts[0]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTopology_InvalidLastUpdatedTimestamp()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();
        string hostName = "hostName";
        double cpuUtilization = 11.1;
        double nodeLag = 0.123;
        string expectedLastUpdatedTimestampRounded = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm");

        this.mockConnection.Mocks.WhenAny()
            .ReturnsTable(MockTable.WithColumns("SERVER_ID", "CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END", "CPU", "REPLICA_LAG_IN_MILLISECONDS")
            .AddRow(hostName, true, cpuUtilization, nodeLag));

        var result = await rdsHostListProviderSpy.Object.GetTopologyAsync(this.mockConnection, true);
        Assert.Single(result.Hosts);
        Assert.Equal(expectedLastUpdatedTimestampRounded, result.Hosts.First().LastUpdateTime.ToString("yyyy-MM-dd HH:mm"));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTopology_ReturnLatestWriter()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();
        HostSpec expectedWriterHost = new HostSpecBuilder()
            .WithHost("expectedWriterHost")
            .WithRole(HostRole.Writer)
            .WithLastUpdateTime(DateTime.Parse("3000-01-01 00:00:00"))
            .Build();
        HostSpec unexpectedWriterHost0 = new HostSpecBuilder()
            .WithHost("unexpectedWriterHost0")
            .WithRole(HostRole.Writer)
            .WithLastUpdateTime(DateTime.Parse("1000-01-01 00:00:00"))
            .Build();
        HostSpec unexpectedWriterHost1 = new HostSpecBuilder()
            .WithHost("unexpectedWriterHost1")
            .WithRole(HostRole.Writer)
            .WithLastUpdateTime(DateTime.Parse("2000-01-01 00:00:00"))
            .Build();
        HostSpec unexpectedWriterHostWithNullLastUpdateTime = new HostSpecBuilder()
            .WithHost("unexpectedWriterHostWithNullLastUpdateTime")
            .WithRole(HostRole.Writer)
            .Build();

        this.mockConnection.Mocks.WhenAny()
            .ReturnsTable(MockTable.WithColumns("SERVER_ID", "CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END", "CPU", "REPLICA_LAG_IN_MILLISECONDS", "LAST_UPDATE_TIMESTAMP")
            .AddRow(unexpectedWriterHostWithNullLastUpdateTime.Host, true, 0D, 0D, unexpectedWriterHostWithNullLastUpdateTime.LastUpdateTime)
            .AddRow(unexpectedWriterHost0.Host, true, 0D, 0D, unexpectedWriterHost0.LastUpdateTime)
            .AddRow(expectedWriterHost.Host, true, 0D, 0D, expectedWriterHost.LastUpdateTime)
            .AddRow(unexpectedWriterHost1.Host, true, 0D, 0D, unexpectedWriterHost1.LastUpdateTime));

        var result = await rdsHostListProviderSpy.Object.GetTopologyAsync(this.mockConnection, true);
        Assert.Equal(expectedWriterHost.Host, result.Hosts.First().Host);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ClusterUrlUsedAsDefaultClusterId()
    {
        string readerClusterUrl = "mycluster.cluster-ro-XYZ.us-east-1.rds.amazonaws.com";
        string expectedClusterId = "mycluster.cluster-XYZ.us-east-1.rds.amazonaws.com";
        var rdsHostListProviderSpy1 = this.GetRdsHostListProviderSpy(readerClusterUrl);

        Assert.Equal(expectedClusterId, rdsHostListProviderSpy1.Object.ClusterId);

        List<HostSpec> mockTopology = [
            new HostSpecBuilder().WithHost("host").Build(),
        ];
        rdsHostListProviderSpy1.Setup(r => r.QueryForTopologyAsync(It.IsAny<DbConnection>())).ReturnsAsync(mockTopology);

        await rdsHostListProviderSpy1.Object.RefreshAsync(this.mockConnection);

        Assert.Equal(mockTopology, RdsHostListProvider.TopologyCache.Get<List<HostSpec>>(rdsHostListProviderSpy1.Object.ClusterId));
        rdsHostListProviderSpy1.Verify(r => r.QueryForTopologyAsync(It.IsAny<DbConnection>()), Times.Once);

        var rdsHostListProviderSpy2 = this.GetRdsHostListProviderSpy(readerClusterUrl);
        Assert.Equal(expectedClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.Equal(mockTopology, RdsHostListProvider.TopologyCache.Get<List<HostSpec>>(rdsHostListProviderSpy2.Object.ClusterId));
        rdsHostListProviderSpy2.Verify(r => r.QueryForTopologyAsync(It.IsAny<DbConnection>()), Times.Never);
    }
}
