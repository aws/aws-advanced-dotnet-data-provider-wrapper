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

namespace AwsWrapperDataProvider.Plugin.SecretsManager.Utils;

/// <summary>
/// Methods to get the AWS region from a variety of sources.
/// </summary>
public static partial class RegionUtils
{
    private static readonly string RegionGroup = "region";

    [GeneratedRegex(@$"^arn:aws:secretsmanager:(?<region>[a-z\-0-9]+):.*", RegexOptions.IgnoreCase, "en-CA")]
    private static partial Regex SecretIdPattern();

    public static bool IsValidRegion(string region)
    {
        return RegionEndpoint.EnumerableAllRegions.Any(r => r.SystemName.Equals(region, StringComparison.OrdinalIgnoreCase));
    }

    public static string? GetRegionFromSecretId(string secretId)
    {
        var match = SecretIdPattern().Match(secretId);
        string? region = match.Groups[RegionGroup].Value;

        return region == null ? null : IsValidRegion(region) ? region : null;
    }
}
