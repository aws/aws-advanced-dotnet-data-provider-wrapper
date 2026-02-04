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

using Amazon;
using Amazon.RDS;
using Amazon.RDS.Model;
using AwsWrapperDataProvider.Driver;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Plugin.CustomEndpoint.CustomEndpoint;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.CustomEndpoint;

public class CustomEndpointMonitorTests : IDisposable
{
    private const string CustomEndpointUrl1 = "custom1.cluster-custom-XYZ.us-east-1.rds.amazonaws.com";
    private const string CustomEndpointUrl2 = "custom2.cluster-custom-XYZ.us-east-1.rds.amazonaws.com";
    private const string EndpointId = "custom1";
    private const string ClusterId = "cluster1";
    private const string EndpointRoleType = "ANY";

    private static readonly List<string> StaticMembersList = ["member1", "member2"];
    private static readonly HashSet<string> StaticMembersSet = [.. StaticMembersList];

    private static readonly CustomEndpointInfo ExpectedInfo = new(
        EndpointId,
        ClusterId,
        CustomEndpointUrl1,
        CustomEndpointRoleType.Any,
        StaticMembersSet,
        MemberTypeList.StaticList);

    private readonly Mock<IPluginService> mockPluginService;
    private readonly HostSpec host;
    private readonly List<HostSpec> allHosts;

    public CustomEndpointMonitorTests()
    {
        this.mockPluginService = new Mock<IPluginService>();
        this.host = new HostSpec(CustomEndpointUrl1, 5432, EndpointId, HostRole.Writer, HostAvailability.Available, HostSpec.DefaultWeight, DateTime.UtcNow);
        this.allHosts =
        [
            new HostSpec("member1.host", 5432, "member1", HostRole.Reader, HostAvailability.Available, HostSpec.DefaultWeight, DateTime.UtcNow),
            new HostSpec("member2.host", 5432, "member2", HostRole.Reader, HostAvailability.Available, HostSpec.DefaultWeight, DateTime.UtcNow),
        ];
        this.mockPluginService.Setup(s => s.AllHosts).Returns(this.allHosts);
    }

    public void Dispose()
    {
        CustomEndpointPlugin.CloseMonitors();
        CustomEndpointMonitor.ClearCache();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Run_CachesEndpointInfoAndUpdatesHostAvailability()
    {
        var twoEndpointList = new List<DBClusterEndpoint>
        {
            CreateMockEndpoint(CustomEndpointUrl2, "custom2", null, null),
            CreateMockEndpoint(CustomEndpointUrl1, EndpointId, StaticMembersList, null),
        };
        var oneEndpointList = new List<DBClusterEndpoint>
        {
            CreateMockEndpoint(CustomEndpointUrl1, EndpointId, StaticMembersList, null),
        };

        var mockRds = new Mock<AmazonRDSClient>(RegionEndpoint.USEast1);
        mockRds
            .SetupSequence(x => x.DescribeDBClusterEndpointsAsync(It.IsAny<DescribeDBClusterEndpointsRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeDBClusterEndpointsResponse { DBClusterEndpoints = twoEndpointList })
            .ReturnsAsync(new DescribeDBClusterEndpointsResponse { DBClusterEndpoints = oneEndpointList });

        Func<RegionEndpoint, AmazonRDSClient> rdsClientFunc = _ => mockRds.Object;

        var monitor = new CustomEndpointMonitor(
            this.mockPluginService.Object,
            this.host,
            EndpointId,
            RegionEndpoint.USEast1,
            TimeSpan.FromMilliseconds(50),
            rdsClientFunc);

        // Wait for 2 run cycles. The first returns unexpected number of endpoints, the second returns one.
        await Task.Delay(150, TestContext.Current.CancellationToken);

        Assert.True(CustomEndpointMonitor.CustomEndpointInfoCache.TryGetValue(this.host.Host, out var cachedInfo));
        Assert.NotNull(cachedInfo);
        Assert.Equal(ExpectedInfo.EndpointIdentifier, cachedInfo.EndpointIdentifier);
        Assert.Equal(ExpectedInfo.ClusterIdentifier, cachedInfo.ClusterIdentifier);
        Assert.Equal(ExpectedInfo.Url, cachedInfo.Url);
        Assert.Equal(ExpectedInfo.RoleType, cachedInfo.RoleType);
        Assert.Equal(ExpectedInfo.MemberListType, cachedInfo.MemberListType);
        Assert.True(StaticMembersSet.SetEquals(cachedInfo.GetStaticMembers() ?? []));

        monitor.Dispose();

        // Static list: member1 and member2 should be Available, others Unavailable
        Assert.Equal(HostAvailability.Available, this.allHosts[0].Availability);
        Assert.Equal(HostAvailability.Available, this.allHosts[1].Availability);

        mockRds.Verify(x => x.DescribeDBClusterEndpointsAsync(It.IsAny<DescribeDBClusterEndpointsRequest>(), It.IsAny<CancellationToken>()), Times.AtLeast(2));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ClearCache_RemovesAllEntries()
    {
        CustomEndpointMonitor.CustomEndpointInfoCache[CustomEndpointUrl1] = ExpectedInfo;
        Assert.True(CustomEndpointMonitor.CustomEndpointInfoCache.ContainsKey(CustomEndpointUrl1));

        CustomEndpointMonitor.ClearCache();
        Assert.False(CustomEndpointMonitor.CustomEndpointInfoCache.ContainsKey(CustomEndpointUrl1));
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CustomEndpointInfo_FromDBClusterEndpoint_StaticMembers()
    {
        var dbEndpoint = CreateMockEndpoint(CustomEndpointUrl1, EndpointId, StaticMembersList, null);
        var info = CustomEndpointInfo.FromDBClusterEndpoint(dbEndpoint);

        Assert.Equal(EndpointId, info.EndpointIdentifier);
        Assert.Equal(ClusterId, info.ClusterIdentifier);
        Assert.Equal(CustomEndpointUrl1, info.Url);
        Assert.Equal(CustomEndpointRoleType.Any, info.RoleType);
        Assert.Equal(MemberTypeList.StaticList, info.MemberListType);
        Assert.NotNull(info.GetStaticMembers());
        Assert.True(StaticMembersSet.SetEquals(info.GetStaticMembers()!));
        Assert.Null(info.GetExcludedMembers());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void CustomEndpointInfo_FromDBClusterEndpoint_ExcludedMembers()
    {
        var excludedList = new List<string> { "excluded1" };
        var dbEndpoint = CreateMockEndpoint(CustomEndpointUrl1, EndpointId, null, excludedList);
        var info = CustomEndpointInfo.FromDBClusterEndpoint(dbEndpoint);

        Assert.Equal(MemberTypeList.ExclusionList, info.MemberListType);
        Assert.Null(info.GetStaticMembers());
        Assert.NotNull(info.GetExcludedMembers());
        Assert.Single(info.GetExcludedMembers()!);
        Assert.Contains("excluded1", info.GetExcludedMembers()!);
    }

    private static DBClusterEndpoint CreateMockEndpoint(string url, string endpointId, List<string>? staticMembers, List<string>? excludedMembers)
    {
        return new DBClusterEndpoint
        {
            Endpoint = url,
            DBClusterEndpointIdentifier = endpointId,
            DBClusterIdentifier = ClusterId,
            CustomEndpointType = EndpointRoleType,
            StaticMembers = staticMembers ?? [],
            ExcludedMembers = excludedMembers ?? [],
        };
    }
}
