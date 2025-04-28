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

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(UsEastRegionCluster, RdsUrlType.RdsWriterCluster)]
    [InlineData(UsEastRegionClusterReadOnly, RdsUrlType.RdsReaderCluster)]
    [InlineData(UsEastRegionCustomDomain, RdsUrlType.RdsCustomCluster)]
    [InlineData(UsEastRegionProxy, RdsUrlType.RdsProxy)]
    [InlineData(UsEastRegionInstance, RdsUrlType.RdsInstance)]
    [InlineData(UsEastRegionLimitlessDbShardGroup, RdsUrlType.RdsAuroraLimitlessDbShardGroup)]
    [InlineData("192.168.1.1", RdsUrlType.IpAddress)]
    [InlineData("2001:0db8:85a3:0000:0000:8a2e:0370:7334", RdsUrlType.IpAddress)]
    [InlineData("2001:db8::8a2e:370:7334", RdsUrlType.IpAddress)]
    [InlineData("example.com", RdsUrlType.Other)]
    [InlineData("", RdsUrlType.Other)]
    [InlineData(null, RdsUrlType.Other)]
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
    [InlineData(OldChinaRegionCluster, RdsUrlType.RdsWriterCluster)]
    [InlineData(OldChinaRegionClusterReadOnly, RdsUrlType.RdsReaderCluster)]
    [InlineData(OldChinaRegionCustomDomain, RdsUrlType.RdsCustomCluster)]
    [InlineData(OldChinaRegionProxy, RdsUrlType.RdsProxy)]
    [InlineData(OldChinaRegionInstance, RdsUrlType.RdsInstance)]
    [InlineData(OldChinaRegionLimitlessDbShardGroup, RdsUrlType.RdsAuroraLimitlessDbShardGroup)]
    public void IdentifyRdsType_WithAuroraOldChinaDnsPattern_ShouldReturnCorrectType(string host, RdsUrlType expectedType)
    {
        var result = RdsUtils.IdentifyRdsType(host);
        Assert.Equal(expectedType, result);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(ChinaRegionCluster, RdsUrlType.RdsWriterCluster)]
    [InlineData(ChinaRegionClusterReadOnly, RdsUrlType.RdsReaderCluster)]
    [InlineData(ChinaRegionCustomDomain, RdsUrlType.RdsCustomCluster)]
    [InlineData(ChinaRegionProxy, RdsUrlType.RdsProxy)]
    [InlineData(ChinaRegionInstance, RdsUrlType.RdsInstance)]
    [InlineData(ChinaRegionLimitlessDbShardGroup, RdsUrlType.RdsAuroraLimitlessDbShardGroup)]
    public void IdentifyRdsType_WithAuroraChinaDnsPattern_ShouldReturnCorrectType(string host, RdsUrlType expectedType)
    {
        var result = RdsUtils.IdentifyRdsType(host);
        Assert.Equal(expectedType, result);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(UsGovEastRegionCluster, RdsUrlType.RdsWriterCluster)]
    [InlineData(UsIsoEastRegionCluster, RdsUrlType.RdsWriterCluster)]
    [InlineData(UsIsoEastRegionClusterReadOnly, RdsUrlType.RdsReaderCluster)]
    [InlineData(UsIsoEastRegionCustomDomain, RdsUrlType.RdsCustomCluster)]
    [InlineData(UsIsoEastRegionProxy, RdsUrlType.RdsProxy)]
    [InlineData(UsIsoEastRegionInstance, RdsUrlType.RdsInstance)]
    [InlineData(UsIsoEastRegionLimitlessDbShardGroup, RdsUrlType.RdsAuroraLimitlessDbShardGroup)]
    [InlineData(UsIsobEastRegionCluster, RdsUrlType.RdsWriterCluster)]
    [InlineData(UsIsobEastRegionClusterReadOnly, RdsUrlType.RdsReaderCluster)]
    [InlineData(UsIsobEastRegionCustomDomain, RdsUrlType.RdsCustomCluster)]
    [InlineData(UsIsobEastRegionProxy, RdsUrlType.RdsProxy)]
    [InlineData(UsIsobEastRegionInstance, RdsUrlType.RdsInstance)]
    [InlineData(UsIsobEastRegionLimitlessDbShardGroup, RdsUrlType.RdsAuroraLimitlessDbShardGroup)]
    public void IdentifyRdsType_WithAuroraGovDnsPattern_ShouldReturnCorrectType(string host, RdsUrlType expectedType)
    {
        var result = RdsUtils.IdentifyRdsType(host);
        Assert.Equal(expectedType, result);
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

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("mydb.cluster-123456789012.us-east-1.RDS.AMAZONAWS.COM", RdsUrlType.RdsWriterCluster)]
    [InlineData("MYDB.CLUSTER-123456789012.US-EAST-1.RDS.AMAZONAWS.COM", RdsUrlType.RdsWriterCluster)]
    public void IdentifyRdsType_ShouldBeCaseInsensitive(string host, RdsUrlType expectedType)
    {
        var result = RdsUtils.IdentifyRdsType(host);
        Assert.Equal(expectedType, result);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(UsEastRegionElbUrl, RdsUrlType.Other)]
    public void IdentifyRdsType_WithElbUrl_ShouldReturnOtherType(string host, RdsUrlType expectedType)
    {
        var result = RdsUtils.IdentifyRdsType(host);
        Assert.Equal(expectedType, result);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GetRdsInstanceId_WithElbUrl_ShouldReturnNull()
    {
        var result = RdsUtils.GetRdsInstanceId(UsEastRegionElbUrl);
        Assert.Null(result);
    }
}
