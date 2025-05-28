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

using System.Reflection.Metadata;

namespace AwsWrapperDataProvider.Driver.Utils;

public static class RegionDefinition
{
    public static readonly string ApSouth2 = "ap-south-2";
    public static readonly string ApSouth1 = "ap-south-1";
    public static readonly string EuSouth1 = "eu-south-1";
    public static readonly string EuSouth2 = "eu-south-2";
    public static readonly string UsGovEast1 = "us-gov-east-1";
    public static readonly string MeCentral1 = "me-central-1";
    public static readonly string IlCentral1 = "il-central-1";
    public static readonly string UsIsofSouth1 = "us-isof-south-1";
    public static readonly string CaCentral1 = "ca-central-1";
    public static readonly string MxCentral1 = "mx-central-1";
    public static readonly string EuCentral1 = "eu-central-1";
    public static readonly string UsIsoWest1 = "us-iso-west-1";
    public static readonly string EuCentral2 = "eu-central-2";
    public static readonly string EuIsoeWest1 = "eu-isoe-west-1";
    public static readonly string UsWest1 = "us-west-1";
    public static readonly string UsWest2 = "us-west-2";
    public static readonly string AfSouth1 = "af-south-1";
    public static readonly string EuNorth1 = "eu-north-1";
    public static readonly string EuWest3 = "eu-west-3";
    public static readonly string EuWest2 = "eu-west-2";
    public static readonly string EuWest1 = "eu-west-1";
    public static readonly string ApNortheast3 = "ap-northeast-3";
    public static readonly string ApNortheast2 = "ap-northeast-2";
    public static readonly string ApNortheast1 = "ap-northeast-1";
    public static readonly string MeSouth1 = "me-south-1";
    public static readonly string SaEast1 = "sa-east-1";
    public static readonly string ApEast1 = "ap-east-1";
    public static readonly string CnNorth1 = "cn-north-1";
    public static readonly string CaWest1 = "ca-west-1";
    public static readonly string UsGovWest1 = "us-gov-west-1";
    public static readonly string ApSoutheast1 = "ap-southeast-1";
    public static readonly string ApSoutheast2 = "ap-southeast-2";
    public static readonly string UsIsoEast1 = "us-iso-east-1";
    public static readonly string ApSoutheast3 = "ap-southeast-3";
    public static readonly string ApSoutheast4 = "ap-southeast-4";
    public static readonly string ApSoutheast5 = "ap-southeast-5";
    public static readonly string UsEast1 = "us-east-1";
    public static readonly string UsEast2 = "us-east-2";
    public static readonly string ApSoutheast7 = "ap-southeast-7";
    public static readonly string CnNorthwest1 = "cn-northwest-1";
    public static readonly string UsIsobEast1 = "us-isob-east-1";
    public static readonly string UsIsofEast1 = "us-isof-east-1";

    private static readonly HashSet<string> AllRegions = [
        ApSouth2,
        ApSouth1,
        EuSouth1,
        EuSouth2,
        UsGovEast1,
        MeCentral1,
        IlCentral1,
        UsIsofSouth1,
        CaCentral1,
        MxCentral1,
        EuCentral1,
        UsIsoWest1,
        EuCentral2,
        EuIsoeWest1,
        UsWest1,
        UsWest2,
        AfSouth1,
        EuNorth1,
        EuWest3,
        EuWest2,
        EuWest1,
        ApNortheast3,
        ApNortheast2,
        ApNortheast1,
        MeSouth1,
        SaEast1,
        ApEast1,
        CnNorth1,
        CaWest1,
        UsGovWest1,
        ApSoutheast1,
        ApSoutheast2,
        UsIsoEast1,
        ApSoutheast3,
        ApSoutheast4,
        ApSoutheast5,
        UsEast1,
        UsEast2,
        ApSoutheast7,
        CnNorthwest1,
        UsIsobEast1,
        UsIsofEast1,
    ];

    public static bool IsValidRegion(string region)
    {
        return AllRegions.Contains(region);
    }
}
