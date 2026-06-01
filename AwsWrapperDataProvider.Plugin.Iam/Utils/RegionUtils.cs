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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Plugin.Iam.Utils;

/// <summary>
/// Methods to get the AWS region from a variety of sources.
/// <para>
/// The class exposes stateless static helpers (<see cref="IsValidRegion"/>,
/// <see cref="GetRegionFromProps"/>, <see cref="GetRegionFromHost"/>,
/// <see cref="GetRegion(string, Dictionary{string, string}, AwsWrapperProperty)"/>) plus a
/// virtual instance method <see cref="GetRegionAsync"/> that subclasses (e.g.
/// <see cref="GdbRegionUtils"/>) can override to provide alternative resolution strategies.
/// </para>
/// </summary>
public partial class RegionUtils
{
    public static bool IsValidRegion(string region)
    {
        return RegionEndpoint.EnumerableAllRegions.Any(r => r.SystemName.Equals(region, StringComparison.OrdinalIgnoreCase));
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

    public static string? GetRegion(string host, Dictionary<string, string> props, AwsWrapperProperty prop)
    {
        string? region = GetRegionFromProps(props, prop);
        return region ?? GetRegionFromHost(host);
    }

    /// <summary>
    /// Determines the AWS region for the given host. The default implementation checks the
    /// supplied props first, falling back to parsing the region from the hostname.
    /// </summary>
    /// <remarks>
    /// Subclasses can override this method to provide alternative resolution (e.g.
    /// <see cref="GdbRegionUtils"/> resolves Global Aurora Database endpoints via the
    /// <c>DescribeGlobalClusters</c> RDS API).
    /// </remarks>
    /// <param name="hostSpec">The host spec from which to extract the region if not in props.</param>
    /// <param name="props">The connection properties.</param>
    /// <param name="prop">The region property to check first.</param>
    /// <returns>The AWS region or null if it could not be determined.</returns>
    public virtual Task<string?> GetRegionAsync(HostSpec hostSpec, Dictionary<string, string> props, AwsWrapperProperty prop)
    {
        return Task.FromResult(GetRegion(hostSpec.Host, props, prop));
    }
}
