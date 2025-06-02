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

using System.Text.RegularExpressions;
using Amazon;

namespace AwsWrapperDataProvider.Driver.Utils;

public static class RegionUtils
{
    private static readonly string RegionGroup = "region";

    private static readonly Regex SecretIdPattern = new(
        @$"^arn:aws:secretsmanager:(?<{RegionGroup}>[a-z\-0-9]+):.*");

    public static bool IsValidRegion(string region)
    {
        try
        {
            RegionEndpoint.GetBySystemName(region);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public static string? GetRegionFromProps(Dictionary<string, string> props, AwsWrapperProperty prop)
    {
        string? region = prop.GetString(props);

        if (region == null)
        {
            return null;
        }

        return IsValidRegion(region) ? region : null;
    }

    public static string? GetRegionFromHost(string host)
    {
        string? region = RdsUtils.GetRdsRegion(host);

        if (region == null)
        {
            return null;
        }

        return IsValidRegion(region) ? region : null;
    }

    public static string? GetRegionFromSecretId(string secretId)
    {
        var match = SecretIdPattern.Match(secretId);
        string? region = match.Groups[RegionGroup].Value;

        return region == null ? null : IsValidRegion(region) ? region : null;
    }

    public static string? GetRegion(string host, Dictionary<string, string> props, AwsWrapperProperty prop)
    {
        string? region = GetRegionFromProps(props, prop);
        return region ?? GetRegionFromHost(host);
    }
}
