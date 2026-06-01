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
using Amazon.RDS;
using Amazon.RDS.Model;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Plugin.Iam.Utils;

/// <summary>
/// Resolves the AWS region for an Aurora Global Database (GDB) endpoint by querying the
/// <c>DescribeGlobalClusters</c> RDS API to find the writer cluster's regional ARN.
/// <para>
/// Required because the region cannot be parsed from a global endpoint hostname
/// (<c>&lt;cluster&gt;.global-&lt;id&gt;.global.rds.amazonaws.com</c>). The IAM user or role
/// invoking this must have the <c>rds:DescribeGlobalClusters</c> permission.
/// </para>
/// </summary>
public partial class GdbRegionUtils : RegionUtils
{
    private const string RegionGroup = "region";

    [GeneratedRegex(@"^arn:aws:rds:(?<region>[^:\n]*):[^:\n]*:([^:/\n]*[:/])?(.*)$", RegexOptions.IgnoreCase, "en-CA")]
    private static partial Regex GdbClusterArnPattern();

    private readonly IAmazonRDS? rdsClient;

    public GdbRegionUtils()
        : this(null)
    {
    }

    /// <summary>
    /// Creates a new <see cref="GdbRegionUtils"/>.
    /// </summary>
    /// <param name="rdsClient">Optional RDS client; if null, a new <see cref="AmazonRDSClient"/> is created using the SDK's default credentials chain on each call.</param>
    public GdbRegionUtils(IAmazonRDS? rdsClient)
    {
        this.rdsClient = rdsClient;
    }

    /// <inheritdoc/>
    public override async Task<string?> GetRegionAsync(HostSpec hostSpec, Dictionary<string, string> props, AwsWrapperProperty prop)
    {
        string? region = GetRegionFromProps(props, prop);
        if (region != null)
        {
            return region;
        }

        string? clusterId = RdsUtils.GetRdsClusterId(hostSpec.Host);
        if (string.IsNullOrEmpty(clusterId))
        {
            return null;
        }

        bool ownsClient = this.rdsClient == null;
        IAmazonRDS client = this.rdsClient ?? new AmazonRDSClient();
        try
        {
            string? writerClusterArn = await FindWriterClusterArnAsync(client, clusterId);
            if (string.IsNullOrEmpty(writerClusterArn))
            {
                return null;
            }

            return GetRegionFromClusterArn(writerClusterArn);
        }
        finally
        {
            if (ownsClient)
            {
                client.Dispose();
            }
        }
    }

    private static async Task<string?> FindWriterClusterArnAsync(IAmazonRDS rdsClient, string globalClusterIdentifier)
    {
        DescribeGlobalClustersResponse response = await rdsClient.DescribeGlobalClustersAsync(
            new DescribeGlobalClustersRequest
            {
                GlobalClusterIdentifier = globalClusterIdentifier,
            });

        if (response.GlobalClusters == null)
        {
            return null;
        }

        foreach (GlobalCluster cluster in response.GlobalClusters)
        {
            if (cluster.GlobalClusterMembers == null)
            {
                continue;
            }

            foreach (GlobalClusterMember member in cluster.GlobalClusterMembers)
            {
                if (member.IsWriter == true)
                {
                    return member.DBClusterArn;
                }
            }
        }

        return null;
    }

    private static string? GetRegionFromClusterArn(string clusterArn)
    {
        Match match = GdbClusterArnPattern().Match(clusterArn);
        if (!match.Success)
        {
            return null;
        }

        string region = match.Groups[RegionGroup].Value;
        return string.IsNullOrEmpty(region) || !IsValidRegion(region) ? null : region;
    }
}
