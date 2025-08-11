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

public class RdsHostListProviderTest : IDisposable
{
    private const string ClusterA = "cluster-a.cluster-xyz.us-east-2.rds.amazonaws.com";
    private const string Instance1 = "instance-a-1.xyz.us-east-2.rds.amazonaws.com";
    private const string Instance2 = "instance-a-2.xyz.us-east-2.rds.amazonaws.com";
    private const string Instance3 = "instance-a-3.xyz.us-east-2.rds.amazonaws.com";

    private readonly Dictionary<string, string> properties;
    private readonly Mock<IHostListProviderService> mockHostListProviderService;
    private readonly Mock<IDbConnection> mockConnection;
    private readonly Mock<IDbCommand> mockCommand;
    private readonly Mock<IDataReader> mockReader;
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
        this.mockConnection = new Mock<IDbConnection>();
        this.mockCommand = new Mock<IDbCommand>();
        this.mockReader = new Mock<IDataReader>();
        this.mockConnection.Setup(c => c.CreateCommand()).Returns(this.mockCommand.Object);
        this.mockConnection.Setup(c => c.State).Returns(ConnectionState.Open);
        this.mockCommand.Setup(c => c.ExecuteReader()).Returns(this.mockReader.Object);
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
    public void GetTopology_ReturnCachedTopology()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();

        RdsHostListProvider.TopologyCache.Set(rdsHostListProviderSpy.Object.ClusterId, this.clusterAHosts, this.topologyRefreshRate);

        var result = rdsHostListProviderSpy.Object.GetTopology(this.mockConnection.Object, false);
        Assert.Equal(this.clusterAHosts, result.Hosts);
        Assert.True(result.IsCachedData);
        rdsHostListProviderSpy.Verify(r => r.QueryForTopology(It.IsAny<IDbConnection>()), Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetTopology_WithForceUpdate_ReturnUpdatedTopology()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();

        RdsHostListProvider.TopologyCache.Set(rdsHostListProviderSpy.Object.ClusterId, this.clusterAHosts, this.topologyRefreshRate);
        List<HostSpec> newHosts = [new HostSpecBuilder().WithHost("newHost").Build()];
        rdsHostListProviderSpy.Setup(r => r.QueryForTopology(It.IsAny<IDbConnection>())).Returns(newHosts);

        var result = rdsHostListProviderSpy.Object.GetTopology(this.mockConnection.Object, true);
        Assert.Equal(newHosts, result.Hosts);
        Assert.False(result.IsCachedData);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetTopology_WithoutForceUpdate_ReturnEmptyHostList()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();
        rdsHostListProviderSpy.Object.ClusterId = "cluster-id";

        RdsHostListProvider.TopologyCache.Set(rdsHostListProviderSpy.Object.ClusterId, this.clusterAHosts, this.topologyRefreshRate);
        rdsHostListProviderSpy.Setup(r => r.QueryForTopology(It.IsAny<DbConnection>())).Returns([]);

        var result = rdsHostListProviderSpy.Object.GetTopology(this.mockConnection.Object, false);
        Assert.Equal(this.clusterAHosts, result.Hosts);
        Assert.True(result.IsCachedData);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetTopology_WithForceUpdate_ReturnInitialHostList()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();

        rdsHostListProviderSpy.Setup(r => r.QueryForTopology(It.IsAny<DbConnection>())).Returns([]);

        var result = rdsHostListProviderSpy.Object.GetTopology(this.mockConnection.Object, true);
        Assert.Equal([new HostSpecBuilder().WithHost(this.properties[PropertyDefinition.Host.Name]).Build()], result.Hosts);
        Assert.False(result.IsCachedData);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Refresh_SuggestedClusterIdForRds()
    {
        RdsHostListProvider.ClearAll();
        var rdsHostListProviderSpy1 = this.GetRdsHostListProviderSpy(ClusterA);

        rdsHostListProviderSpy1.Setup(r => r.QueryForTopology(It.IsAny<IDbConnection>())).Returns(this.clusterAHosts);
        Assert.Equal(0, RdsHostListProvider.TopologyCache.Count);

        var provider1Hosts = rdsHostListProviderSpy1.Object.Refresh(this.mockConnection.Object);
        Assert.Equal(this.clusterAHosts, provider1Hosts);

        var rdsHostListProviderSpy2 = this.GetRdsHostListProviderSpy(ClusterA);

        Assert.Equal(rdsHostListProviderSpy1.Object.ClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.True(rdsHostListProviderSpy1.Object.IsPrimaryClusterId);
        Assert.True(rdsHostListProviderSpy2.Object.IsPrimaryClusterId);

        var provider2Hosts = rdsHostListProviderSpy2.Object.Refresh(this.mockConnection.Object);
        Assert.Equal(this.clusterAHosts, provider2Hosts);

        Assert.Equal(1, RdsHostListProvider.TopologyCache.Count);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Refresh_SuggestedClusterIdForInstance()
    {
        RdsHostListProvider.ClearAll();
        var rdsHostListProviderSpy1 = this.GetRdsHostListProviderSpy(ClusterA);

        rdsHostListProviderSpy1.Setup(r => r.QueryForTopology(It.IsAny<IDbConnection>())).Returns(this.clusterAHosts);
        Assert.Equal(0, RdsHostListProvider.TopologyCache.Count);

        var provider1Hosts = rdsHostListProviderSpy1.Object.Refresh(this.mockConnection.Object);
        Assert.Equal(this.clusterAHosts, provider1Hosts);

        var rdsHostListProviderSpy2 = this.GetRdsHostListProviderSpy(Instance3);

        Assert.Equal(rdsHostListProviderSpy1.Object.ClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.True(rdsHostListProviderSpy1.Object.IsPrimaryClusterId);
        Assert.True(rdsHostListProviderSpy2.Object.IsPrimaryClusterId);

        var provider2Hosts = rdsHostListProviderSpy2.Object.Refresh(this.mockConnection.Object);
        Assert.Equal(this.clusterAHosts, provider2Hosts);

        Assert.Equal(1, RdsHostListProvider.TopologyCache.Count);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Refresh_AcceptSuggestion()
    {
        RdsHostListProvider.ClearAll();
        var rdsHostListProviderSpy1 = this.GetRdsHostListProviderSpy(Instance2);

        rdsHostListProviderSpy1.Setup(r => r.QueryForTopology(It.IsAny<IDbConnection>())).Returns(this.clusterAHosts);
        Assert.Equal(0, RdsHostListProvider.TopologyCache.Count);

        var provider1Hosts = rdsHostListProviderSpy1.Object.Refresh(this.mockConnection.Object);
        Assert.Equal(this.clusterAHosts, provider1Hosts);

        var rdsHostListProviderSpy2 = this.GetRdsHostListProviderSpy(ClusterA);
        rdsHostListProviderSpy2.Setup(r => r.QueryForTopology(It.IsAny<IDbConnection>())).Returns(this.clusterAHosts);

        var provider2Hosts = rdsHostListProviderSpy2.Object.Refresh(this.mockConnection.Object);
        Assert.Equal(this.clusterAHosts, provider2Hosts);

        Assert.NotEqual(rdsHostListProviderSpy1.Object.ClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.False(rdsHostListProviderSpy1.Object.IsPrimaryClusterId);
        Assert.True(rdsHostListProviderSpy2.Object.IsPrimaryClusterId);
        Assert.Equal(2, RdsHostListProvider.TopologyCache.Count);
        Assert.Equal(ClusterA, RdsHostListProvider.SuggestedPrimaryClusterIdCache.Get<string>(rdsHostListProviderSpy1.Object.ClusterId));

        provider1Hosts = rdsHostListProviderSpy1.Object.ForceRefresh(this.mockConnection.Object);
        Assert.Equal(rdsHostListProviderSpy1.Object.ClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.True(rdsHostListProviderSpy1.Object.IsPrimaryClusterId);
        Assert.True(rdsHostListProviderSpy2.Object.IsPrimaryClusterId);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetTopology_StaleRecord()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();
        string hostName1 = "hostName1";
        string hostName2 = "hostName2";
        double cpuUtilization = 11.1;
        double nodeLag = 0.123;
        DateTime firstTimestamp = DateTime.UtcNow;
        DateTime secondTimestamp = firstTimestamp.AddMinutes(1);
        this.mockReader.SetupSequence(r => r.Read()).Returns(true).Returns(true).Returns(false);
        this.mockReader.SetupSequence(r => r.GetString(0)).Returns(hostName1).Returns(hostName2);
        this.mockReader.SetupSequence(r => r.GetBoolean(1)).Returns(true).Returns(true);
        this.mockReader.SetupSequence(r => r.GetDouble(2)).Returns(cpuUtilization).Returns(cpuUtilization);
        this.mockReader.SetupSequence(r => r.GetDouble(3)).Returns(nodeLag).Returns(nodeLag);
        this.mockReader.SetupSequence(r => r.GetDateTime(4)).Returns(firstTimestamp).Returns(secondTimestamp);
        long weight = (long)((Math.Round(nodeLag) * 100L) + Math.Round(cpuUtilization));
        HostSpec expcetedWriter = new HostSpecBuilder()
            .WithHost(hostName2)
            .WithPort(HostSpec.NoPort)
            .WithRole(HostRole.Writer)
            .WithAvailability(HostAvailability.Available)
            .WithWeight(weight)
            .WithLastUpdateTime(secondTimestamp)
            .Build();

        var result = rdsHostListProviderSpy.Object.GetTopology(this.mockConnection.Object, true);
        Assert.Single(result.Hosts);
        Assert.Equal(expcetedWriter, result.Hosts[0]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetTopology_InvalidLastUpdatedTimestamp()
    {
        var rdsHostListProviderSpy = this.GetRdsHostListProviderSpy();
        string hostName = "hostName";
        double cpuUtilization = 11.1;
        double nodeLag = 0.123;
        this.mockReader.SetupSequence(r => r.Read()).Returns(true).Returns(false);
        this.mockReader.SetupSequence(r => r.GetString(0)).Returns(hostName);
        this.mockReader.SetupSequence(r => r.GetBoolean(1)).Returns(true);
        this.mockReader.SetupSequence(r => r.GetDouble(2)).Returns(cpuUtilization);
        this.mockReader.SetupSequence(r => r.GetDouble(3)).Returns(nodeLag);
        this.mockReader.SetupSequence(r => r.GetDateTime(4)).Throws(new MockDbException());
        string expectedLastUpdatedTimestampRounded = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm");
        var result = rdsHostListProviderSpy.Object.GetTopology(this.mockConnection.Object, true);
        Assert.Single(result.Hosts);
        Assert.Equal(expectedLastUpdatedTimestampRounded, result.Hosts.First().LastUpdateTime.ToString("yyyy-MM-dd HH:mm"));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetTopology_ReturnLatestWriter()
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

        this.mockReader.SetupSequence(r => r.Read()).Returns(true).Returns(true).Returns(true).Returns(true).Returns(false);
        this.mockReader.SetupSequence(r => r.GetString(0))
            .Returns(unexpectedWriterHostWithNullLastUpdateTime.Host)
            .Returns(unexpectedWriterHost0.Host)
            .Returns(expectedWriterHost.Host)
            .Returns(unexpectedWriterHost1.Host);
        this.mockReader.Setup(r => r.GetBoolean(1)).Returns(true);
        this.mockReader.Setup(r => r.GetDouble(2)).Returns(0);
        this.mockReader.Setup(r => r.GetDouble(3)).Returns(0);
        this.mockReader.SetupSequence(r => r.GetDateTime(4))
            .Returns(unexpectedWriterHostWithNullLastUpdateTime.LastUpdateTime)
            .Returns(unexpectedWriterHost0.LastUpdateTime)
            .Returns(expectedWriterHost.LastUpdateTime)
            .Returns(unexpectedWriterHost1.LastUpdateTime);

        var result = rdsHostListProviderSpy.Object.GetTopology(this.mockConnection.Object, true);
        Assert.Equal(expectedWriterHost.Host, result.Hosts.First().Host);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ClusterUrlUsedAsDefaultClusterId()
    {
        string readerClusterUrl = "mycluster.cluster-ro-XYZ.us-east-1.rds.amazonaws.com";
        string expectedClusterId = "mycluster.cluster-XYZ.us-east-1.rds.amazonaws.com";
        var rdsHostListProviderSpy1 = this.GetRdsHostListProviderSpy(readerClusterUrl);

        Assert.Equal(expectedClusterId, rdsHostListProviderSpy1.Object.ClusterId);

        List<HostSpec> mockTopology = [
            new HostSpecBuilder().WithHost("host").Build(),
        ];
        rdsHostListProviderSpy1.Setup(r => r.QueryForTopology(It.IsAny<IDbConnection>())).Returns(mockTopology);

        rdsHostListProviderSpy1.Object.Refresh(this.mockConnection.Object);

        Assert.Equal(mockTopology, RdsHostListProvider.TopologyCache.Get<List<HostSpec>>(rdsHostListProviderSpy1.Object.ClusterId));
        rdsHostListProviderSpy1.Verify(r => r.QueryForTopology(It.IsAny<IDbConnection>()), Times.Once);

        var rdsHostListProviderSpy2 = this.GetRdsHostListProviderSpy(readerClusterUrl);
        Assert.Equal(expectedClusterId, rdsHostListProviderSpy2.Object.ClusterId);
        Assert.Equal(mockTopology, RdsHostListProvider.TopologyCache.Get<List<HostSpec>>(rdsHostListProviderSpy2.Object.ClusterId));
        rdsHostListProviderSpy2.Verify(r => r.QueryForTopology(It.IsAny<IDbConnection>()), Times.Never);
    }
}
