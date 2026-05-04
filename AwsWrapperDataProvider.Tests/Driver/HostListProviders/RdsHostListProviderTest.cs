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

using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.Dialects;
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
    private readonly Mock<IPluginService> mockPluginService;
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
        this.mockPluginService = new Mock<IPluginService>();
        this.mockPluginService.Setup(s => s.IsDialectConfirmed).Returns(true);
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

        var rdsHostListProviderSpy = new Mock<RdsHostListProvider>(this.properties, this.mockHostListProviderService.Object, "bar", this.mockPluginService.Object, new AuroraTopologyUtils(new HostSpecBuilder(), Mock.Of<ITopologyDialect>(d => d.TopologyQuery == "foo" && d.WriterIdQuery == "qux")))
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

        var result = await rdsHostListProviderSpy.Object.GetTopologyAsync();
        Assert.Equal(this.clusterAHosts, result.Hosts);
        Assert.True(result.IsCachedData);
        rdsHostListProviderSpy.Verify(r => r.ForceRefreshMonitorAsync(It.IsAny<bool>(), It.IsAny<long>()), Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTopology_NoCachedTopology_MonitorReturnsHosts()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();
        List<HostSpec> newHosts = [new HostSpecBuilder().WithHost("newHost").Build()];

        rdsHostListProviderSpy.Setup(r => r.ForceRefreshMonitorAsync(It.IsAny<bool>(), It.IsAny<long>()))
            .ReturnsAsync((IList<HostSpec>)newHosts);

        var result = await rdsHostListProviderSpy.Object.GetTopologyAsync();
        Assert.Equal(newHosts, result.Hosts);
        Assert.False(result.IsCachedData);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTopology_NoCachedTopology_MonitorReturnsEmpty_ReturnInitialHostList()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();

        rdsHostListProviderSpy.Setup(r => r.ForceRefreshMonitorAsync(It.IsAny<bool>(), It.IsAny<long>()))
            .ReturnsAsync((IList<HostSpec>?)null);

        var result = await rdsHostListProviderSpy.Object.GetTopologyAsync();
        Assert.Equal([new HostSpecBuilder().WithHost(this.properties[PropertyDefinition.Host.Name]).Build()], result.Hosts);
        Assert.False(result.IsCachedData);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Refresh_SuggestedClusterIdForRds()
    {
        RdsHostListProvider.ClearAll();
        var rdsHostListProviderSpy1 = this.GetRdsHostListProviderSpy(ClusterA);

        // Pre-populate the topology cache to simulate the monitor having fetched topology
        RdsHostListProvider.TopologyCache.Set(rdsHostListProviderSpy1.Object.ClusterId, this.clusterAHosts, this.topologyRefreshRate);

        var provider1Hosts = await rdsHostListProviderSpy1.Object.RefreshAsync();
        Assert.Equal(this.clusterAHosts, provider1Hosts);

        var rdsHostListProviderSpy2 = this.GetRdsHostListProviderSpy(ClusterA);

        Assert.Equal(rdsHostListProviderSpy1.Object.ClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.True(rdsHostListProviderSpy1.Object.IsPrimaryClusterId);
        Assert.True(rdsHostListProviderSpy2.Object.IsPrimaryClusterId);

        var provider2Hosts = await rdsHostListProviderSpy2.Object.RefreshAsync();
        Assert.Equal(this.clusterAHosts, provider2Hosts);

        Assert.Equal(1, RdsHostListProvider.TopologyCache.Count);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Refresh_SuggestedClusterIdForInstance()
    {
        RdsHostListProvider.ClearAll();
        var rdsHostListProviderSpy1 = this.GetRdsHostListProviderSpy(ClusterA);

        // Pre-populate the topology cache to simulate the monitor having fetched topology
        RdsHostListProvider.TopologyCache.Set(rdsHostListProviderSpy1.Object.ClusterId, this.clusterAHosts, this.topologyRefreshRate);

        var provider1Hosts = await rdsHostListProviderSpy1.Object.RefreshAsync();
        Assert.Equal(this.clusterAHosts, provider1Hosts);

        var rdsHostListProviderSpy2 = this.GetRdsHostListProviderSpy(Instance3);

        Assert.Equal(rdsHostListProviderSpy1.Object.ClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.True(rdsHostListProviderSpy1.Object.IsPrimaryClusterId);
        Assert.True(rdsHostListProviderSpy2.Object.IsPrimaryClusterId);

        var provider2Hosts = await rdsHostListProviderSpy2.Object.RefreshAsync();
        Assert.Equal(this.clusterAHosts, provider2Hosts);

        Assert.Equal(1, RdsHostListProvider.TopologyCache.Count);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Refresh_AcceptSuggestion()
    {
        RdsHostListProvider.ClearAll();
        var rdsHostListProviderSpy1 = this.GetRdsHostListProviderSpy(Instance2);

        // Mock ForceRefreshMonitorAsync to return clusterAHosts and populate the cache
        rdsHostListProviderSpy1.Setup(r => r.ForceRefreshMonitorAsync(It.IsAny<bool>(), It.IsAny<long>()))
            .ReturnsAsync((IList<HostSpec>)this.clusterAHosts)
            .Callback(() =>
            {
                RdsHostListProvider.TopologyCache.Set(rdsHostListProviderSpy1.Object.ClusterId, this.clusterAHosts, this.topologyRefreshRate);
            });

        Assert.Equal(0, RdsHostListProvider.TopologyCache.Count);

        var provider1Hosts = await rdsHostListProviderSpy1.Object.RefreshAsync();
        Assert.Equal(this.clusterAHosts, provider1Hosts);

        var rdsHostListProviderSpy2 = this.GetRdsHostListProviderSpy(ClusterA);
        // Pre-populate cache for provider2
        RdsHostListProvider.TopologyCache.Set(rdsHostListProviderSpy2.Object.ClusterId, this.clusterAHosts, this.topologyRefreshRate);

        var provider2Hosts = await rdsHostListProviderSpy2.Object.RefreshAsync();
        Assert.Equal(this.clusterAHosts, provider2Hosts);

        Assert.NotEqual(rdsHostListProviderSpy1.Object.ClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.False(rdsHostListProviderSpy1.Object.IsPrimaryClusterId);
        Assert.True(rdsHostListProviderSpy2.Object.IsPrimaryClusterId);

        // Simulate the suggestion that would normally be set by the monitor/topology flow
        RdsHostListProvider.SuggestedPrimaryClusterIdCache.Set(
            rdsHostListProviderSpy1.Object.ClusterId,
            ClusterA,
            TimeSpan.FromMinutes(10));
        Assert.Equal(ClusterA, RdsHostListProvider.SuggestedPrimaryClusterIdCache.Get<string>(rdsHostListProviderSpy1.Object.ClusterId));

        // RefreshAsync calls GetTopologyAsync which picks up the suggested primary cluster ID
        provider1Hosts = await rdsHostListProviderSpy1.Object.RefreshAsync();
        Assert.Equal(rdsHostListProviderSpy1.Object.ClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.True(rdsHostListProviderSpy1.Object.IsPrimaryClusterId);
        Assert.True(rdsHostListProviderSpy2.Object.IsPrimaryClusterId);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTopology_StaleRecord()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();
        string hostName2 = "hostName2";
        double cpuUtilization = 11.1;
        double nodeLag = 0.123;
        DateTime secondTimestamp = DateTime.UtcNow.AddMinutes(1);
        long weight = (long)((Math.Round(nodeLag) * 100L) + Math.Round(cpuUtilization));
        HostSpec expectedWriter = new HostSpecBuilder()
            .WithHost(hostName2)
            .WithHostId(hostName2)
            .WithPort(HostSpec.NoPort)
            .WithRole(HostRole.Writer)
            .WithAvailability(HostAvailability.Available)
            .WithWeight(weight)
            .WithLastUpdateTime(secondTimestamp)
            .Build();

        // Simulate the monitor returning a topology with the latest writer selected
        rdsHostListProviderSpy.Setup(r => r.ForceRefreshMonitorAsync(It.IsAny<bool>(), It.IsAny<long>()))
            .ReturnsAsync((IList<HostSpec>)new List<HostSpec> { expectedWriter });

        var result = await rdsHostListProviderSpy.Object.GetTopologyAsync();
        Assert.Single(result.Hosts);
        Assert.Equal(expectedWriter, result.Hosts[0]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetTopology_InvalidLastUpdatedTimestamp()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();
        string hostName = "hostName";
        string expectedLastUpdatedTimestampRounded = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm");

        HostSpec writerHost = new HostSpecBuilder()
            .WithHost(hostName)
            .WithHostId(hostName)
            .WithPort(HostSpec.NoPort)
            .WithRole(HostRole.Writer)
            .WithAvailability(HostAvailability.Available)
            .WithWeight(0)
            .WithLastUpdateTime(DateTime.UtcNow)
            .Build();

        rdsHostListProviderSpy.Setup(r => r.ForceRefreshMonitorAsync(It.IsAny<bool>(), It.IsAny<long>()))
            .ReturnsAsync((IList<HostSpec>)new List<HostSpec> { writerHost });

        var result = await rdsHostListProviderSpy.Object.GetTopologyAsync();
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

        // The monitor is responsible for selecting the latest writer from multiple writer candidates.
        // We simulate the monitor returning only the expected writer.
        rdsHostListProviderSpy.Setup(r => r.ForceRefreshMonitorAsync(It.IsAny<bool>(), It.IsAny<long>()))
            .ReturnsAsync((IList<HostSpec>)new List<HostSpec> { expectedWriterHost });

        var result = await rdsHostListProviderSpy.Object.GetTopologyAsync();
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

        // Pre-populate the topology cache to simulate the monitor having fetched topology
        RdsHostListProvider.TopologyCache.Set(rdsHostListProviderSpy1.Object.ClusterId, mockTopology, this.topologyRefreshRate);

        await rdsHostListProviderSpy1.Object.RefreshAsync();

        Assert.Equal(mockTopology, RdsHostListProvider.TopologyCache.Get<List<HostSpec>>(rdsHostListProviderSpy1.Object.ClusterId));

        var rdsHostListProviderSpy2 = this.GetRdsHostListProviderSpy(readerClusterUrl);
        Assert.Equal(expectedClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.Equal(mockTopology, RdsHostListProvider.TopologyCache.Get<List<HostSpec>>(rdsHostListProviderSpy2.Object.ClusterId));
    }
}
