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

using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Tests.Driver.Utils;

/// <summary>
/// Unit tests for RdsUrlType and RdsUtils global database extensions.
/// Requirements: 1.1, 1.2, 1.5, 1.6
/// </summary>
public class RdsUrlTypeGlobalTests
{
    // --- Requirement 1.1: RdsGlobalWriterCluster properties ---

    [Fact]
    [Trait("Category", "Unit")]
    public void RdsGlobalWriterCluster_IsRds_ShouldBeTrue()
    {
        Assert.True(RdsUrlType.RdsGlobalWriterCluster.IsRds);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RdsGlobalWriterCluster_IsRdsCluster_ShouldBeTrue()
    {
        Assert.True(RdsUrlType.RdsGlobalWriterCluster.IsRdsCluster);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void RdsGlobalWriterCluster_HasRegion_ShouldBeFalse()
    {
        Assert.False(RdsUrlType.RdsGlobalWriterCluster.HasRegion);
    }

    // --- Requirement 1.2: HasRegion is correct for all existing RdsUrlType values ---

    [Theory]
    [Trait("Category", "Unit")]
    [MemberData(nameof(HasRegionTestData))]
    public void HasRegion_ShouldBeCorrectForAllTypes(RdsUrlType type, bool expectedHasRegion, string typeName)
    {
        Assert.Equal(expectedHasRegion, type.HasRegion);
    }

    public static IEnumerable<object[]> HasRegionTestData()
    {
        // HasRegion = true
        yield return new object[] { RdsUrlType.RdsWriterCluster, true, nameof(RdsUrlType.RdsWriterCluster) };
        yield return new object[] { RdsUrlType.RdsReaderCluster, true, nameof(RdsUrlType.RdsReaderCluster) };
        yield return new object[] { RdsUrlType.RdsCustomCluster, true, nameof(RdsUrlType.RdsCustomCluster) };
        yield return new object[] { RdsUrlType.RdsProxy, true, nameof(RdsUrlType.RdsProxy) };
        yield return new object[] { RdsUrlType.RdsInstance, true, nameof(RdsUrlType.RdsInstance) };
        yield return new object[] { RdsUrlType.RdsAuroraLimitlessDbShardGroup, true, nameof(RdsUrlType.RdsAuroraLimitlessDbShardGroup) };

        // HasRegion = false
        yield return new object[] { RdsUrlType.IpAddress, false, nameof(RdsUrlType.IpAddress) };
        yield return new object[] { RdsUrlType.RdsGlobalWriterCluster, false, nameof(RdsUrlType.RdsGlobalWriterCluster) };
        yield return new object[] { RdsUrlType.Other, false, nameof(RdsUrlType.Other) };
    }

    // --- Requirements 1.5, 1.6: IdentifyRdsType returns RdsGlobalWriterCluster for global endpoints ---

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("test-global-db.global-123456789012.global.rds.amazonaws.com")]
    [InlineData("mydb.global-abcdef012345.global.rds.amazonaws.com")]
    [InlineData("prod-cluster.global-a1b2c3.global.rds.amazonaws.com")]
    public void IdentifyRdsType_GlobalEndpoints_ShouldReturnRdsGlobalWriterCluster(string host)
    {
        var result = RdsUtils.IdentifyRdsType(host);
        Assert.Equal(RdsUrlType.RdsGlobalWriterCluster, result);
    }

    // --- Requirements 1.5, 1.6: IdentifyRdsType does NOT return RdsGlobalWriterCluster for regional endpoints ---

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com")]
    [InlineData("database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com")]
    [InlineData("instance-test-name.XYZ.us-east-2.rds.amazonaws.com")]
    [InlineData("proxy-test-name.proxy-XYZ.us-east-2.rds.amazonaws.com")]
    [InlineData("custom-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com")]
    [InlineData("database-test-name.shardgrp-XYZ.us-east-2.rds.amazonaws.com")]
    public void IdentifyRdsType_RegionalEndpoints_ShouldNotReturnRdsGlobalWriterCluster(string host)
    {
        var result = RdsUtils.IdentifyRdsType(host);
        Assert.NotEqual(RdsUrlType.RdsGlobalWriterCluster, result);
    }
}
