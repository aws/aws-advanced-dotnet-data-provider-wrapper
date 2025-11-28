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
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Caching.Memory;

namespace AwsWrapperDataProvider.Driver.HostInfo.HostSelectors;

/// <summary>
/// Host selector that selects hosts in a round-robin fashion with support for weighted selection.
/// Hosts can be assigned weights to control how frequently they are selected.
/// </summary>
public partial class RoundRobinHostSelector : IHostSelector
{
    public static string StrategyName = "RoundRobin";

    private const int DefaultWeight = 1;

    private static readonly MemoryCache RoundRobinCache = new(new MemoryCacheOptions());
    private static readonly object Lock = new();
    private readonly TimeSpan defaultRoundRobinCacheExpire = TimeSpan.FromMinutes(10);

    [GeneratedRegex(@"(?<host>[^:/?#]*):(?<weight>[0-9]+)")]
    private static partial Regex HostWeightPairsPattern();

    public HostSpec GetHost(IList<HostSpec> hosts, HostRole hostRole, Dictionary<string, string> props)
    {
        lock (Lock)
        {
            List<HostSpec> eligibleHosts = hosts
                .Where(hostSpec => hostRole == hostSpec.Role && hostSpec.Availability == HostAvailability.Available)
                .OrderBy(hostSpec => hostSpec.Host)
                .ToList();

            if (eligibleHosts.Count == 0)
            {
                throw new InvalidOperationException(string.Format(Resources.Error_NoHostsMatching, hostRole));
            }

            // Create or update cache entries for provided hosts
            this.CreateCacheEntryForHosts(eligibleHosts, props);

            string currentClusterInfoKey = eligibleHosts[0].Host;
            RoundRobinClusterInfo clusterInfo = RoundRobinCache.Get<RoundRobinClusterInfo>(currentClusterInfoKey)!;

            HostSpec? lastHost = clusterInfo.LastHost;
            int lastHostIndex = -1;

            // Check if lastHost is in list of eligible hosts
            if (lastHost != null)
            {
                for (int i = 0; i < eligibleHosts.Count; i++)
                {
                    if (eligibleHosts[i].Host.Equals(lastHost.Host, StringComparison.OrdinalIgnoreCase))
                    {
                        lastHostIndex = i;
                        break;
                    }
                }
            }

            int targetHostIndex;

            // If the host is weighted and the lastHost is in the eligibleHosts list

            if (clusterInfo.WeightCounter > 0 && lastHostIndex != -1)
            {
                targetHostIndex = lastHostIndex;
            }
            else
            {
                // Move to next host in round-robin order
                if (lastHostIndex != -1 && lastHostIndex != eligibleHosts.Count - 1)
                {
                    targetHostIndex = lastHostIndex + 1;
                }
                else
                {
                    targetHostIndex = 0;
                }

                // Set weight counter for the selected host
                string hostId = eligibleHosts[targetHostIndex].HostId ?? eligibleHosts[targetHostIndex].Host;
                int weight = clusterInfo.ClusterWeightsMap.TryGetValue(hostId, out int w) ? w : clusterInfo.DefaultWeight;
                clusterInfo.WeightCounter = weight;
            }

            clusterInfo.WeightCounter--;
            clusterInfo.LastHost = eligibleHosts[targetHostIndex];

            return eligibleHosts[targetHostIndex];
        }
    }

    private void CreateCacheEntryForHosts(List<HostSpec> hosts, Dictionary<string, string>? props)
    {
        List<HostSpec> hostsWithCacheEntry = hosts.Where(host => RoundRobinCache.TryGetValue(host.Host, out _)).ToList();

        if (hostsWithCacheEntry.Count > 0)
        {
            // Update existing cluster info
            RoundRobinClusterInfo clusterInfo = RoundRobinCache.Get<RoundRobinClusterInfo>(hostsWithCacheEntry[0].Host)!;

            if (this.HasPropertyChanged(clusterInfo.LastClusterHostWeightPairPropertyValue, PropertyDefinition.RoundRobinHostWeightPairs, props))
            {
                clusterInfo.LastHost = null;
                clusterInfo.WeightCounter = 0;
                this.UpdateCachedHostWeightPairsProperties(clusterInfo, props);
            }

            if (this.HasPropertyChanged(clusterInfo.LastClusterDefaultWeightPropertyValue, PropertyDefinition.RoundRobinDefaultWeight, props))
            {
                clusterInfo.DefaultWeight = DefaultWeight;
                this.UpdateCachedDefaultWeightProperties(clusterInfo, props);
            }

            // Update cache entries for all hosts to point to the same cluster info
            foreach (HostSpec host in hosts)
            {
                RoundRobinCache.Set(host.Host, clusterInfo, this.defaultRoundRobinCacheExpire);
            }
        }
        else
        {
            // Create new cluster info
            RoundRobinClusterInfo clusterInfo = new RoundRobinClusterInfo();
            this.UpdateCacheProperties(clusterInfo, props);

            foreach (HostSpec host in hosts)
            {
                RoundRobinCache.Set(host.Host, clusterInfo, this.defaultRoundRobinCacheExpire);
            }
        }
    }

    private bool HasPropertyChanged(string? lastValue, AwsWrapperProperty property, Dictionary<string, string>? props)
    {
        if (props == null)
        {
            return false;
        }

        string? currentValue = property.GetString(props);
        return currentValue != null && !currentValue.Equals(lastValue, StringComparison.OrdinalIgnoreCase);
    }

    private void UpdateCacheProperties(RoundRobinClusterInfo clusterInfo, Dictionary<string, string>? props)
    {
        this.UpdateCachedDefaultWeightProperties(clusterInfo, props);
        this.UpdateCachedHostWeightPairsProperties(clusterInfo, props);
    }

    private void UpdateCachedDefaultWeightProperties(RoundRobinClusterInfo clusterInfo, Dictionary<string, string>? props)
    {
        int defaultWeight = DefaultWeight;

        if (props != null)
        {
            string? defaultWeightString = PropertyDefinition.RoundRobinDefaultWeight.GetString(props);
            if (!string.IsNullOrEmpty(defaultWeightString))
            {
                if (int.TryParse(defaultWeightString, out int parsedWeight) && parsedWeight >= DefaultWeight)
                {
                    defaultWeight = parsedWeight;
                    clusterInfo.LastClusterDefaultWeightPropertyValue = defaultWeightString;
                }
                else
                {
                    throw new InvalidOperationException(Resources.Error_InvalidRoundRobinValue);
                }
            }
        }

        clusterInfo.DefaultWeight = defaultWeight;
    }

    private void UpdateCachedHostWeightPairsProperties(RoundRobinClusterInfo clusterInfo, Dictionary<string, string>? props)
    {
        if (props == null)
        {
            return;
        }

        string? hostWeights = PropertyDefinition.RoundRobinHostWeightPairs.GetString(props);
        if (string.IsNullOrEmpty(hostWeights))
        {
            if (hostWeights == string.Empty)
            {
                clusterInfo.ClusterWeightsMap.Clear();
                clusterInfo.LastClusterHostWeightPairPropertyValue = hostWeights;
            }

            return;
        }

        string[] hostWeightPairs = hostWeights.Split(',');
        foreach (string pair in hostWeightPairs)
        {
            Match match = HostWeightPairsPattern().Match(pair.Trim());
            if (!match.Success)
            {
                throw new InvalidOperationException(Resources.Error_InvalidRoundRobinPairFormat);
            }

            string hostName = match.Groups["host"].Value.Trim();
            string hostWeight = match.Groups["weight"].Value.Trim();

            if (string.IsNullOrEmpty(hostName) || string.IsNullOrEmpty(hostWeight))
            {
                throw new InvalidOperationException(Resources.Error_InvalidRoundRobinPairEmpty);
            }

            if (int.TryParse(hostWeight, out int weight) && weight >= DefaultWeight)
            {
                clusterInfo.ClusterWeightsMap[hostName] = weight;
            }
            else
            {
                throw new InvalidOperationException(Resources.Error_InvalidRoundRobinValue);
            }
        }

        clusterInfo.LastClusterHostWeightPairPropertyValue = hostWeights;
    }

    public static void ClearCache()
    {
        RoundRobinCache.Clear();
    }

    /// <summary>
    /// Represents cluster information for round-robin selection.
    /// </summary>
    public class RoundRobinClusterInfo
    {
        public HostSpec? LastHost { get; set; }
        public Dictionary<string, int> ClusterWeightsMap { get; } = new();
        public int DefaultWeight { get; set; } = 1;
        public int WeightCounter { get; set; } = 0;
        public string? LastClusterHostWeightPairPropertyValue { get; set; } = string.Empty;
        public string? LastClusterDefaultWeightPropertyValue { get; set; } = string.Empty;
    }
}
