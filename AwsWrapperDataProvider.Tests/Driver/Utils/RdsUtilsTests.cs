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
    [Theory]
    [InlineData("mydb.cluster-123456789012.us-east-1.rds.amazonaws.com", RdsUrlType.RdsWriterCluster)]
    [InlineData("mydb.cluster-ro-123456789012.us-east-1.rds.amazonaws.com", RdsUrlType.RdsReaderCluster)]
    [InlineData("mydb.cluster-custom-123456789012.us-east-1.rds.amazonaws.com", RdsUrlType.RdsCustomCluster)]
    [InlineData("mydb.proxy-123456789012.us-east-1.rds.amazonaws.com", RdsUrlType.RdsProxy)]
    [InlineData("mydb.123456789012.us-east-1.rds.amazonaws.com", RdsUrlType.RdsInstance)]
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
    [InlineData("mydb.cluster-123456789012.us-east-1.rds.amazonaws.com", "mydb")]
    [InlineData("mydb.cluster-ro-123456789012.us-east-1.rds.amazonaws.com", "mydb")]
    [InlineData("mydb.cluster-custom-123456789012.us-east-1.rds.amazonaws.com", "mydb")]
    [InlineData("mydb.proxy-123456789012.us-east-1.rds.amazonaws.com", "mydb")]
    [InlineData("mydb.123456789012.us-east-1.rds.amazonaws.com", "mydb")]
    [InlineData("mydb-instance.123456789012.us-east-1.rds.amazonaws.com", "mydb-instance")]
    [InlineData("mydb.123456789012.cn-north-1.rds.amazonaws.com.cn", "mydb")]
    [InlineData("mydb.123456789012.rds.cn-north-1.amazonaws.com.cn", "mydb")]
    [InlineData("mydb.123456789012.rds.us-gov-west-1.amazonaws.com", "mydb")]
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
    [InlineData("mydb.cluster-123456789012.cn-north-1.rds.amazonaws.com.cn", RdsUrlType.RdsWriterCluster)]
    [InlineData("mydb.cluster-ro-123456789012.cn-north-1.rds.amazonaws.com.cn", RdsUrlType.RdsReaderCluster)]
    [InlineData("mydb.cluster-custom-123456789012.cn-north-1.rds.amazonaws.com.cn", RdsUrlType.RdsCustomCluster)]
    [InlineData("mydb.proxy-123456789012.cn-north-1.rds.amazonaws.com.cn", RdsUrlType.RdsProxy)]
    [InlineData("mydb.123456789012.cn-north-1.rds.amazonaws.com.cn", RdsUrlType.RdsInstance)]
    public void IdentifyRdsType_WithAuroraOldChinaDnsPattern_ShouldReturnCorrectType(string host, RdsUrlType expectedType)
    {
        var result = RdsUtils.IdentifyRdsType(host);
        Assert.Equal(expectedType, result);
    }

    [Theory]
    [InlineData("mydb.cluster-123456789012.rds.cn-north-1.amazonaws.com.cn", RdsUrlType.RdsWriterCluster)]
    [InlineData("mydb.cluster-ro-123456789012.rds.cn-north-1.amazonaws.com.cn", RdsUrlType.RdsReaderCluster)]
    [InlineData("mydb.cluster-custom-123456789012.rds.cn-north-1.amazonaws.com.cn", RdsUrlType.RdsCustomCluster)]
    [InlineData("mydb.proxy-123456789012.rds.cn-north-1.amazonaws.com.cn", RdsUrlType.RdsProxy)]
    [InlineData("mydb.123456789012.rds.cn-north-1.amazonaws.com.cn", RdsUrlType.RdsInstance)]
    public void IdentifyRdsType_WithAuroraChinaDnsPattern_ShouldReturnCorrectType(string host, RdsUrlType expectedType)
    {
        var result = RdsUtils.IdentifyRdsType(host);
        Assert.Equal(expectedType, result);
    }

    [Theory]
    [InlineData("mydb.cluster-123456789012.rds.us-gov-west-1.amazonaws.com", RdsUrlType.RdsWriterCluster)]
    [InlineData("mydb.cluster-ro-123456789012.rds.us-gov-west-1.amazonaws.com", RdsUrlType.RdsReaderCluster)]
    [InlineData("mydb.cluster-custom-123456789012.rds.us-gov-west-1.amazonaws.com", RdsUrlType.RdsCustomCluster)]
    [InlineData("mydb.proxy-123456789012.rds.us-gov-west-1.amazonaws.com", RdsUrlType.RdsProxy)]
    [InlineData("mydb.123456789012.rds.us-gov-west-1.amazonaws.com", RdsUrlType.RdsInstance)]
    [InlineData("mydb.cluster-123456789012.rds.us-gov-west-1.c2s.ic.gov", RdsUrlType.RdsWriterCluster)]
    [InlineData("mydb.cluster-123456789012.rds.us-gov-west-1.sc2s.sgov.gov", RdsUrlType.RdsWriterCluster)]
    public void IdentifyRdsType_WithAuroraGovDnsPattern_ShouldReturnCorrectType(string host, RdsUrlType expectedType)
    {
        var result = RdsUtils.IdentifyRdsType(host);
        Assert.Equal(expectedType, result);
    }

    [Theory]
    [InlineData("mydb.cluster-123456789012.cn-north-1.rds.amazonaws.com.cn", "mydb")]
    [InlineData("mydb.123456789012.rds.cn-north-1.amazonaws.com.cn", "mydb")]
    [InlineData("mydb.123456789012.rds.us-gov-west-1.amazonaws.com", "mydb")]
    [InlineData("mydb.123456789012.rds.us-gov-west-1.c2s.ic.gov", "mydb")]
    [InlineData("mydb.123456789012.rds.us-gov-west-1.sc2s.sgov.gov", "mydb")]
    public void GetRdsInstanceId_WithSpecialDomains_ShouldReturnCorrectInstanceId(string host, string expectedInstanceId)
    {
        var result = RdsUtils.GetRdsInstanceId(host);
        Assert.Equal(expectedInstanceId, result);
    }

    [Fact]
    public void ClearCache_ShouldClearCachedPatterns()
    {
        // First call to populate cache
        RdsUtils.IdentifyRdsType("mydb.cluster-123456789012.us-east-1.rds.amazonaws.com");
        RdsUtils.ClearCache();

        // Verify the method doesn't throw and subsequent calls still work
        var result = RdsUtils.IdentifyRdsType("mydb.cluster-123456789012.us-east-1.rds.amazonaws.com");
        Assert.Equal(RdsUrlType.RdsWriterCluster, result);
    }

    [Fact]
    public void IdentifyRdsType_WithCachedValue_ShouldReturnSameResult()
    {
        string host = "mydb.cluster-123456789012.us-east-1.rds.amazonaws.com";

        var firstResult = RdsUtils.IdentifyRdsType(host);
        var secondResult = RdsUtils.IdentifyRdsType(host);

        Assert.Equal(RdsUrlType.RdsWriterCluster, firstResult);
        Assert.Equal(firstResult, secondResult);
    }

    [Theory]
    [InlineData("mydb.cluster-123456789012.us-east-1.RDS.AMAZONAWS.COM", RdsUrlType.RdsWriterCluster)]
    [InlineData("MYDB.CLUSTER-123456789012.US-EAST-1.RDS.AMAZONAWS.COM", RdsUrlType.RdsWriterCluster)]
    public void IdentifyRdsType_ShouldBeCaseInsensitive(string host, RdsUrlType expectedType)
    {
        var result = RdsUtils.IdentifyRdsType(host);
        Assert.Equal(expectedType, result);
    }
}
