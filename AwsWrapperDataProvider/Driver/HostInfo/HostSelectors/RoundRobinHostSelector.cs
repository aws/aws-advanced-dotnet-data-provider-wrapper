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
using System.Data.Common;
using System.Text.RegularExpressions;
using AwsWrapperDataProvider.Driver.Utils;

namespace AwsWrapperDataProvider.Driver.HostInfo.HostSelectors;

/// <summary>
/// Host selector that selects hosts in a round-robin fashion with support for weighted selection.
/// Hosts can be assigned weights to control how frequently they are selected.
/// </summary>
public class RoundRobinHostSelector : IHostSelector
{
    public static string StrategyName { get; } = "RoundRobin";

    // Configuration properties
    public static readonly AwsWrapperProperty RoundRobinHostWeightPairs = new(
        "roundRobinHostWeightPairs",
        null,
        "Comma separated list of database host-weight pairs in the format of `<host>:<weight>`.");

    public static readonly AwsWrapperProperty RoundRobinDefaultWeight = new(
        "roundRobinDefaultWeight",
        "1",
        "The default weight for any hosts that have not been configured with the `roundRobinHostWeightPairs` parameter.");

    private const int DefaultWeight = 1;
    private static readonly Regex HostWeightPairsPattern = new(@"(?<host>[^:/?#]*):(?<weight>[0-9]+)", RegexOptions.Compiled);

    private static readonly ConcurrentDictionary<string, RoundRobinClusterInfo> RoundRobinCache = new();
    private static readonly object Lock = new();

    /// <summary>
    /// Selects the next host in round-robin order for the requested role.
    /// </summary>
    /// <param name="hosts">A list of available hosts to pick from.</param>
    /// <param name="hostRole">The desired host role - either a writer or a reader.</param>
    /// <param name="props">Connection properties that may be needed by the host selector.</param>
    /// <returns>A host matching the requested role selected in round-robin order.</returns>
    /// <exception cref="InvalidOperationException">If the host list does not contain any hosts matching the requested role.</exception>
    public HostSpec GetHost(List<HostSpec> hosts, HostRole hostRole, Dictionary<string, string> props)
    {
        lock (Lock)
        {
            var eligibleHosts = hosts
                .Where(hostSpec => hostRole == hostSpec.Role && hostSpec.Availability == HostAvailability.Available)
                .OrderBy(hostSpec => hostSpec.Host)
                .ToList();

            if (eligibleHosts.Count == 0)
            {
                throw new InvalidOperationException($"No hosts found matching role: {hostRole}");
            }

            // Create or update cache entries for provided hosts
            this.CreateCacheEntryForHosts(eligibleHosts, props);

            var currentClusterInfoKey = eligibleHosts[0].Host;
            var clusterInfo = RoundRobinCache[currentClusterInfoKey];

            var lastHost = clusterInfo.LastHost;
            var lastHostIndex = -1;

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
                var hostId = eligibleHosts[targetHostIndex].HostId ?? eligibleHosts[targetHostIndex].Host;
                var weight = clusterInfo.ClusterWeightsMap.TryGetValue(hostId, out var w) ? w : clusterInfo.DefaultWeight;
                clusterInfo.WeightCounter = weight;
            }

            clusterInfo.WeightCounter--;
            clusterInfo.LastHost = eligibleHosts[targetHostIndex];

            return eligibleHosts[targetHostIndex];
        }
    }

    private void CreateCacheEntryForHosts(List<HostSpec> hosts, Dictionary<string, string>? props)
    {
        var hostsWithCacheEntry = hosts.Where(host => RoundRobinCache.ContainsKey(host.Host)).ToList();

        if (hostsWithCacheEntry.Count > 0)
        {
            // Update existing cluster info
            var clusterInfo = RoundRobinCache[hostsWithCacheEntry[0].Host];

            if (this.HasPropertyChanged(clusterInfo.LastClusterHostWeightPairPropertyValue, RoundRobinHostWeightPairs, props))
            {
                clusterInfo.LastHost = null;
                clusterInfo.WeightCounter = 0;
                this.UpdateCachedHostWeightPairsProperties(clusterInfo, props);
            }

            if (this.HasPropertyChanged(clusterInfo.LastClusterDefaultWeightPropertyValue, RoundRobinDefaultWeight, props))
            {
                clusterInfo.DefaultWeight = DefaultWeight;
                this.UpdateCachedDefaultWeightProperties(clusterInfo, props);
            }

            // Update cache entries for all hosts to point to the same cluster info
            foreach (var host in hosts)
            {
                RoundRobinCache[host.Host] = clusterInfo;
            }
        }
        else
        {
            // Create new cluster info
            var clusterInfo = new RoundRobinClusterInfo();
            this.UpdateCacheProperties(clusterInfo, props);

            foreach (var host in hosts)
            {
                RoundRobinCache[host.Host] = clusterInfo;
            }
        }
    }

    private bool HasPropertyChanged(string? lastValue, AwsWrapperProperty property, Dictionary<string, string>? props)
    {
        if (props == null)
        {
            return false;
        }

        var currentValue = property.GetString(props);
        return currentValue != null && !currentValue.Equals(lastValue, StringComparison.OrdinalIgnoreCase);
    }

    private void UpdateCacheProperties(RoundRobinClusterInfo clusterInfo, Dictionary<string, string>? props)
    {
        this.UpdateCachedDefaultWeightProperties(clusterInfo, props);
        this.UpdateCachedHostWeightPairsProperties(clusterInfo, props);
    }

    private void UpdateCachedDefaultWeightProperties(RoundRobinClusterInfo clusterInfo, Dictionary<string, string>? props)
    {
        var defaultWeight = DefaultWeight;

        if (props != null)
        {
            var defaultWeightString = RoundRobinDefaultWeight.GetString(props);
            if (!string.IsNullOrEmpty(defaultWeightString))
            {
                if (int.TryParse(defaultWeightString, out var parsedWeight) && parsedWeight >= DefaultWeight)
                {
                    defaultWeight = parsedWeight;
                    clusterInfo.LastClusterDefaultWeightPropertyValue = defaultWeightString;
                }
                else
                {
                    throw new InvalidOperationException("Invalid round robin default weight. Weight must be a positive integer.");
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

        var hostWeights = RoundRobinHostWeightPairs.GetString(props);
        if (string.IsNullOrEmpty(hostWeights))
        {
            if (hostWeights == string.Empty)
            {
                clusterInfo.ClusterWeightsMap.Clear();
                clusterInfo.LastClusterHostWeightPairPropertyValue = hostWeights;
            }

            return;
        }

        var hostWeightPairs = hostWeights.Split(',');
        foreach (var pair in hostWeightPairs)
        {
            var match = HostWeightPairsPattern.Match(pair.Trim());
            if (!match.Success)
            {
                throw new InvalidOperationException("Invalid round robin host weight pairs format. Expected format: host1:weight1,host2:weight2");
            }

            var hostName = match.Groups["host"].Value.Trim();
            var hostWeight = match.Groups["weight"].Value.Trim();

            if (string.IsNullOrEmpty(hostName) || string.IsNullOrEmpty(hostWeight))
            {
                throw new InvalidOperationException("Invalid round robin host weight pairs format. Host name and weight cannot be empty.");
            }

            if (int.TryParse(hostWeight, out var weight) && weight >= DefaultWeight)
            {
                clusterInfo.ClusterWeightsMap[hostName] = weight;
            }
            else
            {
                throw new InvalidOperationException("Invalid round robin host weight pairs format. Weight must be a positive integer.");
            }
        }

        clusterInfo.LastClusterHostWeightPairPropertyValue = hostWeights;
    }

    public static void SetRoundRobinHostWeightPairsProperty(Dictionary<string, string> properties, List<HostSpec> hosts)
    {
        var pairs = hosts.Select(host => $"{host.HostId ?? host.Host}:{host.Weight}");
        var roundRobinHostWeightPairsString = string.Join(",", pairs);
        RoundRobinHostWeightPairs.Set(properties, roundRobinHostWeightPairsString);
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
