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
using AwsWrapperDataProvider.Plugin.FederatedAuth.Utils;

namespace AwsWrapperDataProvider.Tests.Driver.Utils;

public class RegionUtilsTests
{
    private const string UsEastRegionCluster =
        "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com";

    private const string InvalidRegionCluster =
        "database-test-name.cluster-XYZ.invalid-region.rds.amazonaws.com";

    private const string RegionIndeterminateCluster =
        "my-cluster-endpoint.com";

    private const string UsEastRegionSecretArn =
        "arn:aws:secretsmanager:us-east-2:123:secret:secret-name";

    private const string InvalidRegionSecretArn =
        "arn:aws:secretsmanager:invalid-region:123:secret:secret-name";

    private const string SecretName =
        "secret-name";

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData("us-east-2", true)]
    [InlineData("cn-northwest-1", true)]
    [InlineData("us-isob-east-1", true)]
    [InlineData("us-gov-east-1", true)]
    [InlineData("invalid-region", false)]
    public void IsValidRegion_ShouldReturnCorrectResult(string region, bool expectedResult)
    {
        bool result = RegionUtils.IsValidRegion(region);
        Assert.Equal(expectedResult, result);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(null, null)]
    [InlineData("us-east-2", "us-east-2")]
    [InlineData("invalid-region", null)]
    public void GetRegionFromProps_ShouldReturnValidRegion(string? propRegionValue, string? expectedRegion)
    {
        Dictionary<string, string> props = propRegionValue == null ?
            []
            : new Dictionary<string, string> { { PropertyDefinition.IamRegion.Name, propRegionValue } };

        string? result = RegionUtils.GetRegionFromProps(props, PropertyDefinition.IamRegion);
        Assert.Equal(expectedRegion, result);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(UsEastRegionCluster, "us-east-2")]
    [InlineData(InvalidRegionCluster, null)]
    [InlineData(RegionIndeterminateCluster, null)]
    public void GetRegionFromHost_ShouldReturnValidRegion(string host, string? expectedRegion)
    {
        string? result = RegionUtils.GetRegionFromHost(host);
        Assert.Equal(expectedRegion, result);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(UsEastRegionSecretArn, "us-east-2")]
    [InlineData(InvalidRegionSecretArn, null)]
    [InlineData(SecretName, null)]
    public void GetRegionFromSecretId_ShouldReturnValidRegion(string secretId, string? expectedRegion)
    {
        string? result = RegionUtils.GetRegionFromSecretId(secretId);
        Assert.Equal(expectedRegion, result);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [InlineData(UsEastRegionCluster, null, "us-east-2")]
    [InlineData(RegionIndeterminateCluster, "us-east-2", "us-east-2")]
    [InlineData(UsEastRegionCluster, "us-west-2", "us-west-2")]
    [InlineData(InvalidRegionCluster, null, null)]
    [InlineData(RegionIndeterminateCluster, null, null)]
    [InlineData(UsEastRegionCluster, "invalid-region", "us-east-2")]
    public void GetRegion_ShouldReturnPropertyRegionOverHostRegion(string host, string? propRegionValue, string? expectedRegion)
    {
        Dictionary<string, string> props = propRegionValue == null ?
            []
            : new Dictionary<string, string> { { PropertyDefinition.IamRegion.Name, propRegionValue } };

        string? result = RegionUtils.GetRegion(host, props, PropertyDefinition.IamRegion);
        Assert.Equal(expectedRegion, result);
    }
}
