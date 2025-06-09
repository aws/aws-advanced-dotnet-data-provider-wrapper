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

using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace AwsWrapperDataProvider.Driver.Utils;

public static partial class RdsUtils
{
    // Group names for regex matches
    private const string InstanceGroup = "instance";
    private const string DnsGroup = "dns";
    private const string DomainGroup = "domain";
    private const string RegionGroup = "region";

    // Regular expression patterns for different AWS RDS endpoint types
    [GeneratedRegex(@"^(?<instance>.+)\.(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?(?<domain>[a-zA-Z0-9]+\.(?<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com)$", RegexOptions.IgnoreCase, "en-CA")]
    private static partial Regex AuroraDnsPattern();

    [GeneratedRegex(@"^(?<instance>.+)\.(?<dns>cluster-|cluster-ro-)(?<domain>[a-zA-Z0-9]+\.(?<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com)$", RegexOptions.IgnoreCase, "en-CA")]
    private static partial Regex AuroraClusterPattern();

    [GeneratedRegex(@"^(?<instance>.+)\.(?<dns>shardgrp-)(?<domain>[a-zA-Z0-9]+\.(?<region>[a-zA-Z0-9\-]+)\.rds\.(amazonaws\.com|amazonaws\.com\.cn|c2s\.ic\.gov|sc2s\.sgov\.gov))$", RegexOptions.IgnoreCase, "en-CA")]
    private static partial Regex AuroraLimitlessClusterPattern();

    [GeneratedRegex(@"^(?<instance>.+)\.(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?(?<domain>[a-zA-Z0-9]+\.rds\.(?<region>[a-zA-Z0-9\-]+)\.amazonaws\.com\.cn)$", RegexOptions.IgnoreCase, "en-CA")]
    private static partial Regex AuroraChinaDnsPattern();

    [GeneratedRegex(@"^(?<instance>.+)\.(?<dns>cluster-|cluster-ro-)(?<domain>[a-zA-Z0-9]+\.rds\.(?<region>[a-zA-Z0-9\-]+)\.amazonaws\.com\.cn)$", RegexOptions.IgnoreCase, "en-CA")]
    private static partial Regex AuroraChinaClusterPattern();

    [GeneratedRegex(@"^(?<instance>.+)\.(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?(?<domain>[a-zA-Z0-9]+\.(?<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com\.cn)$", RegexOptions.IgnoreCase, "en-CA")]
    private static partial Regex AuroraOldChinaDnsPattern();

    [GeneratedRegex(@"^(?<instance>.+)\.(?<dns>cluster-|cluster-ro-)(?<domain>[a-zA-Z0-9]+\.(?<region>[a-zA-Z0-9\-]+)\.rds\.amazonaws\.com\.cn)$", RegexOptions.IgnoreCase, "en-CA")]
    private static partial Regex AuroraOldChinaClusterPattern();

    [GeneratedRegex(@"^(?<instance>.+)\.(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?(?<domain>[a-zA-Z0-9]+\.rds\.(?<region>[a-zA-Z0-9\-]+)\.(amazonaws\.com|c2s\.ic\.gov|sc2s\.sgov\.gov))$", RegexOptions.IgnoreCase, "en-CA")]
    private static partial Regex AuroraGovDnsPattern();

    [GeneratedRegex(@"^(?<instance>.+)\.(?<dns>cluster-|cluster-ro-)(?<domain>[a-zA-Z0-9]+\.rds\.(?<region>[a-zA-Z0-9\-]+)\.(amazonaws\.com|c2s\.ic\.gov|sc2s\.sgov\.gov))$", RegexOptions.IgnoreCase, "en-CA")]
    private static partial Regex AuroraGovClusterPattern();

    private static readonly Regex[] AuroraDnsPatterns = [AuroraDnsPattern(), AuroraChinaDnsPattern(), AuroraOldChinaDnsPattern(), AuroraGovDnsPattern()];

    [GeneratedRegex(@"^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){1}(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){2}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$")]
    private static partial Regex IpV4Pattern();

    [GeneratedRegex(@"^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){7}$")]
    private static partial Regex IpV6Pattern();

    [GeneratedRegex("^(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)::(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)$")]
    private static partial Regex IpV6CompressedPattern();

    // Cache for DNS patterns to improve performance
    private static readonly ConcurrentDictionary<string, Match> CachedPatterns = new();

    public static RdsUrlType IdentifyRdsType(string? host)
    {
        if (string.IsNullOrEmpty(host))
        {
            return RdsUrlType.Other;
        }

        if (IpV4Pattern().IsMatch(host) || IpV6Pattern().IsMatch(host) || IpV6CompressedPattern().IsMatch(host))
        {
            return RdsUrlType.IpAddress;
        }

        string? dnsGroup = GetDnsGroup(host);

        // ELB URLs will also be classified as other
        if (dnsGroup is null)
        {
            return RdsUrlType.Other;
        }

        // Is RDS writer cluster DNS.
        if (dnsGroup.Equals("cluster-", StringComparison.OrdinalIgnoreCase))
        {
            return RdsUrlType.RdsWriterCluster;
        }

        // Is RDS reader cluster DNS.
        if (dnsGroup.Equals("cluster-ro-", StringComparison.OrdinalIgnoreCase))
        {
            return RdsUrlType.RdsReaderCluster;
        }

        // Is RDS custom cluster DNS.
        if (dnsGroup.StartsWith("cluster-custom-"))
        {
            return RdsUrlType.RdsCustomCluster;
        }

        // Is RDS proxy DNS.
        if (dnsGroup.StartsWith("proxy-"))
        {
            return RdsUrlType.RdsProxy;
        }

        // Is RDS shard group DNS.
        if (dnsGroup.StartsWith("shardgrp-"))
        {
            return RdsUrlType.RdsAuroraLimitlessDbShardGroup;
        }

        // Is generic RDS DNS.
        return RdsUrlType.RdsInstance;
    }

    public static string? GetRdsInstanceId(string? host)
    {
        return string.IsNullOrEmpty(host) ? null : CacheMatcher(host, AuroraDnsPatterns)?.Groups[InstanceGroup].Value;
    }

    public static string GetRdsInstanceHostPattern(string host)
    {
        if (string.IsNullOrEmpty(host))
        {
            return "?";
        }

        string? group = CacheMatcher(host, AuroraDnsPatterns)?.Groups[DomainGroup].Value;
        return string.IsNullOrEmpty(group) ? "?" : $"?.{group}";
    }

    public static bool IsDnsPatternValid(string pattern)
    {
        return pattern.Contains('?');
    }

    public static string? GetRdsClusterHostUrl(string host)
    {
        if (AuroraClusterPattern().IsMatch(host))
        {
            return AuroraClusterPattern().Replace(host, "${instance}.cluster-${domain}");
        }

        if (AuroraChinaClusterPattern().IsMatch(host))
        {
            return AuroraChinaClusterPattern().Replace(host, "${instance}.cluster-${domain}");
        }

        if (AuroraOldChinaClusterPattern().IsMatch(host))
        {
            return AuroraOldChinaClusterPattern().Replace(host, "${instance}.cluster-${domain}");
        }

        if (AuroraGovClusterPattern().IsMatch(host))
        {
            return AuroraGovClusterPattern().Replace(host, "${instance}.cluster-${domain}");
        }

        if (AuroraLimitlessClusterPattern().IsMatch(host))
        {
            return AuroraLimitlessClusterPattern().Replace(host, "${instance}.shardgrp-${domain}");
        }

        return null;
    }

    public static string? GetRdsRegion(string host)
    {
        return CacheMatcher(host, AuroraDnsPatterns)?.Groups[RegionGroup].Value;
    }

    private static string? GetDnsGroup(string host)
    {
        return CacheMatcher(host, AuroraDnsPatterns)?.Groups[DnsGroup].Value;
    }

    private static Match? CacheMatcher(string host, params Regex[] patterns)
    {
        if (CachedPatterns.TryGetValue(host, out Match? cachedMatch))
        {
            return cachedMatch;
        }

        var match = patterns
            .Select(p => p.Match(host))
            .FirstOrDefault(m => m.Success);

        if (match != null)
        {
            CachedPatterns[host] = match;
        }

        return match;
    }

    public static void ClearCache()
    {
        CachedPatterns.Clear();
    }
}
