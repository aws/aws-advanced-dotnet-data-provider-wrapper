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

public class RdsUtilsTests
{
    private const string UsEastRegionCluster =
        "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com";
    private const string UsEastRegionClusterReadOnly =
        "database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
    private const string UsEastRegionInstance =
        "instance-test-name.XYZ.us-east-2.rds.amazonaws.com";
    private const string UsEastRegionProxy =
        "proxy-test-name.proxy-XYZ.us-east-2.rds.amazonaws.com";
    private const string UsEastRegionCustomDomain =
        "custom-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com";
    private const string UsEastRegionLimitlessDbShardGroup =
        "database-test-name.shardgrp-XYZ.us-east-2.rds.amazonaws.com";

    private const string ChinaRegionCluster =
        "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
    private const string ChinaRegionClusterReadOnly =
        "database-test-name.cluster-ro-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
    private const string ChinaRegionInstance =
        "instance-test-name.XYZ.rds.cn-northwest-1.amazonaws.com.cn";
    private const string ChinaRegionProxy =
        "proxy-test-name.proxy-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
    private const string ChinaRegionCustomDomain =
        "custom-test-name.cluster-custom-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
    private const string ChinaRegionLimitlessDbShardGroup =
        "database-test-name.shardgrp-XYZ.rds.cn-northwest-1.amazonaws.com.cn";

    private const string OldChinaRegionCluster =
        "database-test-name.cluster-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
    private const string OldChinaRegionClusterReadOnly =
        "database-test-name.cluster-ro-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
    private const string OldChinaRegionInstance =
        "instance-test-name.XYZ.cn-northwest-1.rds.amazonaws.com.cn";
    private const string OldChinaRegionProxy =
        "proxy-test-name.proxy-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
    private const string OldChinaRegionCustomDomain =
        "custom-test-name.cluster-custom-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
    private const string OldChinaRegionLimitlessDbShardGroup =
        "database-test-name.shardgrp-XYZ.cn-northwest-1.rds.amazonaws.com.cn";

    private const string UsEastRegionElbUrl =
        "elb-name.elb.us-east-2.amazonaws.com";

    private const string UsIsobEastRegionCluster =
        "database-test-name.cluster-XYZ.rds.us-isob-east-1.sc2s.sgov.gov";
    private const string UsIsobEastRegionClusterReadOnly =
        "database-test-name.cluster-ro-XYZ.rds.us-isob-east-1.sc2s.sgov.gov";
    private const string UsIsobEastRegionInstance =
        "instance-test-name.XYZ.rds.us-isob-east-1.sc2s.sgov.gov";
    private const string UsIsobEastRegionProxy =
        "proxy-test-name.proxy-XYZ.rds.us-isob-east-1.sc2s.sgov.gov";
    private const string UsIsobEastRegionCustomDomain =
        "custom-test-name.cluster-custom-XYZ.rds.us-isob-east-1.sc2s.sgov.gov";
    private const string UsIsobEastRegionLimitlessDbShardGroup =
        "database-test-name.shardgrp-XYZ.rds.us-isob-east-1.sc2s.sgov.gov";

    private const string UsGovEastRegionCluster =
        "database-test-name.cluster-XYZ.rds.us-gov-east-1.amazonaws.com";
    private const string UsIsoEastRegionCluster =
        "database-test-name.cluster-XYZ.rds.us-iso-east-1.c2s.ic.gov";
    private const string UsIsoEastRegionClusterReadOnly =
        "database-test-name.cluster-ro-XYZ.rds.us-iso-east-1.c2s.ic.gov";
    private const string UsIsoEastRegionInstance =
        "instance-test-name.XYZ.rds.us-iso-east-1.c2s.ic.gov";
    private const string UsIsoEastRegionProxy =
        "proxy-test-name.proxy-XYZ.rds.us-iso-east-1.c2s.ic.gov";
    private const string UsIsoEastRegionCustomDomain =
        "custom-test-name.cluster-custom-XYZ.rds.us-iso-east-1.c2s.ic.gov";
    private const string UsIsoEastRegionLimitlessDbShardGroup =
        "database-test-name.shardgrp-XYZ.rds.us-iso-east-1.c2s.ic.gov";

    public static IEnumerable<object?[]> IdentifyRdsTypeTestData()
    {
        yield return new object?[] { UsEastRegionCluster, RdsUrlType.RdsWriterCluster };
        yield return new object?[] { UsEastRegionClusterReadOnly, RdsUrlType.RdsReaderCluster };
        yield return new object?[] { UsEastRegionCustomDomain, RdsUrlType.RdsCustomCluster };
        yield return new object?[] { UsEastRegionProxy, RdsUrlType.RdsProxy };
        yield return new object?[] { UsEastRegionInstance, RdsUrlType.RdsInstance };
        yield return new object?[] { UsEastRegionLimitlessDbShardGroup, RdsUrlType.RdsAuroraLimitlessDbShardGroup };
        yield return new object?[] { "192.168.1.1", RdsUrlType.IpAddress };
        yield return new object?[] { "2001:0db8:85a3:0000:0000:8a2e:0370:7334", RdsUrlType.IpAddress };
        yield return new object?[] { "2001:db8::8a2e:370:7334", RdsUrlType.IpAddress };
        yield return new object?[] { "example.com", RdsUrlType.Other };
        yield return new object?[] { string.Empty, RdsUrlType.Other };
        yield return new object?[] { null, RdsUrlType.Other };

        // Old China Dns Pattern
        yield return new object?[] { OldChinaRegionCluster, RdsUrlType.RdsWriterCluster };
        yield return new object?[] { OldChinaRegionClusterReadOnly, RdsUrlType.RdsReaderCluster };
        yield return new object?[] { OldChinaRegionCustomDomain, RdsUrlType.RdsCustomCluster };
        yield return new object?[] { OldChinaRegionProxy, RdsUrlType.RdsProxy };
        yield return new object?[] { OldChinaRegionInstance, RdsUrlType.RdsInstance };
        yield return new object?[] { OldChinaRegionLimitlessDbShardGroup, RdsUrlType.RdsAuroraLimitlessDbShardGroup };

        // China Dns Pattern
        yield return new object?[] { ChinaRegionCluster, RdsUrlType.RdsWriterCluster };
        yield return new object?[] { ChinaRegionClusterReadOnly, RdsUrlType.RdsReaderCluster };
        yield return new object?[] { ChinaRegionCustomDomain, RdsUrlType.RdsCustomCluster };
        yield return new object?[] { ChinaRegionProxy, RdsUrlType.RdsProxy };
        yield return new object?[] { ChinaRegionInstance, RdsUrlType.RdsInstance };
        yield return new object?[] { ChinaRegionLimitlessDbShardGroup, RdsUrlType.RdsAuroraLimitlessDbShardGroup };

        // Gov Dns Pattern
        yield return new object?[] { UsGovEastRegionCluster, RdsUrlType.RdsWriterCluster };
        yield return new object?[] { UsIsoEastRegionCluster, RdsUrlType.RdsWriterCluster };
        yield return new object?[] { UsIsoEastRegionClusterReadOnly, RdsUrlType.RdsReaderCluster };
        yield return new object?[] { UsIsoEastRegionCustomDomain, RdsUrlType.RdsCustomCluster };
        yield return new object?[] { UsIsoEastRegionProxy, RdsUrlType.RdsProxy };
        yield return new object?[] { UsIsoEastRegionInstance, RdsUrlType.RdsInstance };
        yield return new object?[] { UsIsoEastRegionLimitlessDbShardGroup, RdsUrlType.RdsAuroraLimitlessDbShardGroup };
        yield return new object?[] { UsIsobEastRegionCluster, RdsUrlType.RdsWriterCluster };
        yield return new object?[] { UsIsobEastRegionClusterReadOnly, RdsUrlType.RdsReaderCluster };
        yield return new object?[] { UsIsobEastRegionCustomDomain, RdsUrlType.RdsCustomCluster };
        yield return new object?[] { UsIsobEastRegionProxy, RdsUrlType.RdsProxy };
        yield return new object?[] { UsIsobEastRegionInstance, RdsUrlType.RdsInstance };
        yield return new object?[] { UsIsobEastRegionLimitlessDbShardGroup, RdsUrlType.RdsAuroraLimitlessDbShardGroup };

        // Should be case insensitive
        yield return new object?[] { "mydb.cluster-123456789012.us-east-1.RDS.AMAZONAWS.COM", RdsUrlType.RdsWriterCluster };
        yield return new object?[] { "MYDB.CLUSTER-123456789012.US-EAST-1.RDS.AMAZONAWS.COM", RdsUrlType.RdsWriterCluster };

        // Other
        yield return new object?[] { UsEastRegionElbUrl, RdsUrlType.Other };
    }

    [Theory]
    [Trait("Category", "Unit")]
    [MemberData(nameof(IdentifyRdsTypeTestData))]
    public void IdentifyRdsType_ShouldReturnCorrectType(string? host, RdsUrlType expectedType)
    {
        var result = RdsUtils.IdentifyRdsType(host);
        Assert.Equal(expectedType, result);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(UsEastRegionCluster, "database-test-name")]
    [InlineData(UsEastRegionClusterReadOnly, "database-test-name")]
    [InlineData(UsEastRegionInstance, "instance-test-name")]
    [InlineData(UsEastRegionProxy, "proxy-test-name")]
    [InlineData(UsEastRegionCustomDomain, "custom-test-name")]
    [InlineData(UsEastRegionLimitlessDbShardGroup, "database-test-name")]
    [InlineData("192.168.1.1", null)]
    [InlineData("example.com", null)]
    [InlineData("", null)]
    [InlineData(null, null)]
    public void GetRdsInstanceId_ShouldReturnCorrectInstanceId(string? host, string? expectedInstanceId)
    {
        var result = RdsUtils.GetRdsInstanceId(host);
        Assert.Equal(expectedInstanceId, result);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(OldChinaRegionCluster, "database-test-name")]
    [InlineData(ChinaRegionInstance, "instance-test-name")]
    [InlineData(UsGovEastRegionCluster, "database-test-name")]
    [InlineData(UsIsoEastRegionInstance, "instance-test-name")]
    [InlineData(UsIsobEastRegionCluster, "database-test-name")]
    public void GetRdsInstanceId_WithSpecialDomains_ShouldReturnCorrectInstanceId(string host, string expectedInstanceId)
    {
        var result = RdsUtils.GetRdsInstanceId(host);
        Assert.Equal(expectedInstanceId, result);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void ClearCache_ShouldClearCachedPatterns()
    {
        // First call to populate the cache
        RdsUtils.IdentifyRdsType(UsEastRegionCluster);
        RdsUtils.ClearCache();

        // Verify the method doesn't throw an error, and next calls still work
        var result = RdsUtils.IdentifyRdsType(UsEastRegionCluster);
        Assert.Equal(RdsUrlType.RdsWriterCluster, result);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void IdentifyRdsType_WithCachedValue_ShouldReturnSameResult()
    {
        string host = UsEastRegionCluster;

        var firstResult = RdsUtils.IdentifyRdsType(host);
        var secondResult = RdsUtils.IdentifyRdsType(host);

        Assert.Equal(RdsUrlType.RdsWriterCluster, firstResult);
        Assert.Equal(firstResult, secondResult);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetRdsInstanceId_WithElbUrl_ShouldReturnNull()
    {
        var result = RdsUtils.GetRdsInstanceId(UsEastRegionElbUrl);
        Assert.Null(result);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("?.XYZ.us-east-2.rds.amazonaws.com", UsEastRegionCluster)]
    [InlineData("?.XYZ.us-east-2.rds.amazonaws.com", UsEastRegionClusterReadOnly)]
    [InlineData("?.XYZ.us-east-2.rds.amazonaws.com", UsEastRegionCustomDomain)]
    [InlineData("?.XYZ.us-east-2.rds.amazonaws.com", UsEastRegionInstance)]
    [InlineData("?.XYZ.us-east-2.rds.amazonaws.com", UsEastRegionLimitlessDbShardGroup)]
    [InlineData("?.XYZ.us-east-2.rds.amazonaws.com", UsEastRegionProxy)]

    [InlineData("?.XYZ.rds.us-gov-east-1.amazonaws.com", UsGovEastRegionCluster)]

    [InlineData("?.XYZ.rds.us-isob-east-1.sc2s.sgov.gov", UsIsobEastRegionCluster)]
    [InlineData("?.XYZ.rds.us-isob-east-1.sc2s.sgov.gov", UsIsobEastRegionClusterReadOnly)]
    [InlineData("?.XYZ.rds.us-isob-east-1.sc2s.sgov.gov", UsIsobEastRegionCustomDomain)]
    [InlineData("?.XYZ.rds.us-isob-east-1.sc2s.sgov.gov", UsIsobEastRegionInstance)]
    [InlineData("?.XYZ.rds.us-isob-east-1.sc2s.sgov.gov", UsIsobEastRegionLimitlessDbShardGroup)]
    [InlineData("?.XYZ.rds.us-isob-east-1.sc2s.sgov.gov", UsIsobEastRegionProxy)]

    [InlineData("?.XYZ.rds.us-iso-east-1.c2s.ic.gov", UsIsoEastRegionCluster)]
    [InlineData("?.XYZ.rds.us-iso-east-1.c2s.ic.gov", UsIsoEastRegionClusterReadOnly)]
    [InlineData("?.XYZ.rds.us-iso-east-1.c2s.ic.gov", UsIsoEastRegionCustomDomain)]
    [InlineData("?.XYZ.rds.us-iso-east-1.c2s.ic.gov", UsIsoEastRegionInstance)]
    [InlineData("?.XYZ.rds.us-iso-east-1.c2s.ic.gov", UsIsoEastRegionLimitlessDbShardGroup)]
    [InlineData("?.XYZ.rds.us-iso-east-1.c2s.ic.gov", UsIsoEastRegionProxy)]

    [InlineData("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", ChinaRegionCluster)]
    [InlineData("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", ChinaRegionClusterReadOnly)]
    [InlineData("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", ChinaRegionCustomDomain)]
    [InlineData("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", ChinaRegionInstance)]
    [InlineData("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", ChinaRegionLimitlessDbShardGroup)]
    [InlineData("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", ChinaRegionProxy)]

    [InlineData("?.XYZ.cn-northwest-1.rds.amazonaws.com.cn", OldChinaRegionCluster)]
    [InlineData("?.XYZ.cn-northwest-1.rds.amazonaws.com.cn", OldChinaRegionClusterReadOnly)]
    [InlineData("?.XYZ.cn-northwest-1.rds.amazonaws.com.cn", OldChinaRegionCustomDomain)]
    [InlineData("?.XYZ.cn-northwest-1.rds.amazonaws.com.cn", OldChinaRegionInstance)]
    [InlineData("?.XYZ.cn-northwest-1.rds.amazonaws.com.cn", OldChinaRegionLimitlessDbShardGroup)]
    [InlineData("?.XYZ.cn-northwest-1.rds.amazonaws.com.cn", OldChinaRegionProxy)]
    public void GetRdsInstanceHostPatternTest(string expectedHostPattern, string host)
    {
        Assert.Equal(expectedHostPattern, RdsUtils.GetRdsInstanceHostPattern(host));
    }
}
