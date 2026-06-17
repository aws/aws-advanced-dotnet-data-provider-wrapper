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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Plugin.Iam.Utils;
using Moq;

namespace AwsWrapperDataProvider.Tests.Driver.Plugins.Iam;

public class GdbRegionUtilsTests
{
    private const string GlobalHost = "atlas-pg.global-cluster1.global.rds.amazonaws.com";
    private const string GlobalClusterIdentifier = "atlas-pg";

    private static HostSpec CreateHostSpec(string host)
    {
        return new HostSpec(host, 5432, HostRole.Writer, HostAvailability.Available);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetRegionAsync_WithWriterMember_ReturnsRegion()
    {
        var mockRds = new Mock<AmazonRDSClient>(RegionEndpoint.USEast1);
        mockRds
            .Setup(x => x.DescribeGlobalClustersAsync(It.IsAny<DescribeGlobalClustersRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeGlobalClustersResponse
            {
                GlobalClusters =
                [
                    new GlobalCluster
                    {
                        GlobalClusterMembers =
                        [
                            new GlobalClusterMember
                            {
                                IsWriter = false,
                                DBClusterArn = "arn:aws:rds:us-west-2:123456789012:cluster:secondary",
                            },
                            new GlobalClusterMember
                            {
                                IsWriter = true,
                                DBClusterArn = "arn:aws:rds:us-east-2:123456789012:cluster:primary",
                            },
                        ],
                    },
                ],
            });

        var gdbRegionUtils = new GdbRegionUtils(mockRds.Object);
        string? region = await gdbRegionUtils.GetRegionAsync(CreateHostSpec(GlobalHost), [], PropertyDefinition.IamRegion);

        Assert.Equal("us-east-2", region);
        mockRds.Verify(
            x => x.DescribeGlobalClustersAsync(
                It.Is<DescribeGlobalClustersRequest>(r => r.GlobalClusterIdentifier == GlobalClusterIdentifier),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetRegionAsync_NoWriterMember_ReturnsNull()
    {
        var mockRds = new Mock<AmazonRDSClient>(RegionEndpoint.USEast1);
        mockRds
            .Setup(x => x.DescribeGlobalClustersAsync(It.IsAny<DescribeGlobalClustersRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeGlobalClustersResponse
            {
                GlobalClusters =
                [
                    new GlobalCluster
                    {
                        GlobalClusterMembers =
                        [
                            new GlobalClusterMember
                            {
                                IsWriter = false,
                                DBClusterArn = "arn:aws:rds:us-west-2:123456789012:cluster:secondary",
                            },
                        ],
                    },
                ],
            });

        var gdbRegionUtils = new GdbRegionUtils(mockRds.Object);
        string? region = await gdbRegionUtils.GetRegionAsync(CreateHostSpec(GlobalHost), [], PropertyDefinition.IamRegion);

        Assert.Null(region);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetRegionAsync_EmptyResponse_ReturnsNull()
    {
        var mockRds = new Mock<AmazonRDSClient>(RegionEndpoint.USEast1);
        mockRds
            .Setup(x => x.DescribeGlobalClustersAsync(It.IsAny<DescribeGlobalClustersRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeGlobalClustersResponse { GlobalClusters = [] });

        var gdbRegionUtils = new GdbRegionUtils(mockRds.Object);
        string? region = await gdbRegionUtils.GetRegionAsync(CreateHostSpec(GlobalHost), [], PropertyDefinition.IamRegion);

        Assert.Null(region);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetRegionAsync_NonRdsHost_ReturnsNull()
    {
        var mockRds = new Mock<AmazonRDSClient>(RegionEndpoint.USEast1);

        var gdbRegionUtils = new GdbRegionUtils(mockRds.Object);
        string? region = await gdbRegionUtils.GetRegionAsync(
            CreateHostSpec("not-a-cluster-host.example.com"),
            [],
            PropertyDefinition.IamRegion);

        Assert.Null(region);
        mockRds.Verify(
            x => x.DescribeGlobalClustersAsync(
                It.IsAny<DescribeGlobalClustersRequest>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetRegionAsync_InvalidRegionInArn_ReturnsNull()
    {
        var mockRds = new Mock<AmazonRDSClient>(RegionEndpoint.USEast1);
        mockRds
            .Setup(x => x.DescribeGlobalClustersAsync(It.IsAny<DescribeGlobalClustersRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeGlobalClustersResponse
            {
                GlobalClusters =
                [
                    new GlobalCluster
                    {
                        GlobalClusterMembers =
                        [
                            new GlobalClusterMember
                            {
                                IsWriter = true,
                                DBClusterArn = "arn:aws:rds:not-a-real-region:123456789012:cluster:primary",
                            },
                        ],
                    },
                ],
            });

        var gdbRegionUtils = new GdbRegionUtils(mockRds.Object);
        string? region = await gdbRegionUtils.GetRegionAsync(CreateHostSpec(GlobalHost), [], PropertyDefinition.IamRegion);

        Assert.Null(region);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetRegionAsync_RegionInProps_SkipsRdsCall()
    {
        var mockRds = new Mock<AmazonRDSClient>(RegionEndpoint.USEast1);
        var props = new Dictionary<string, string>
        {
            [PropertyDefinition.IamRegion.Name] = "eu-west-1",
        };

        var gdbRegionUtils = new GdbRegionUtils(mockRds.Object);
        string? region = await gdbRegionUtils.GetRegionAsync(CreateHostSpec(GlobalHost), props, PropertyDefinition.IamRegion);

        Assert.Equal("eu-west-1", region);
        mockRds.Verify(
            x => x.DescribeGlobalClustersAsync(
                It.IsAny<DescribeGlobalClustersRequest>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }
}
