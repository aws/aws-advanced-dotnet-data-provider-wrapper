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

namespace AwsWrapperDataProvider.Driver.HostInfo.HostSelectors;

/// <summary>
/// Host selector that randomly selects an available host with the requested role based on weighted probabilities.
/// </summary>
public partial class WeightedRandomHostSelector : IHostSelector
{
    public static string StrategyName = "WeightedRandom";
    private const int DefaultWeight = 1;
    private readonly Random _random;
    private readonly object _lock = new();

    [GeneratedRegex(@"(?<host>[^:/?#]*):(?<weight>[0-9]+)")]
    private static partial Regex HostWeightPairsPattern();

    private Dictionary<string, int>? _cachedHostWeightMap;
    private string? _cachedHostWeightMapString;

    public static void SetHostWeightPairsProperty(Dictionary<string, string> props, IList<HostSpec> routers)
    {
        var weightPairs = string.Join(",", routers.Select(r => $"{r.Host}:{r.Weight}"));
        props[PropertyDefinition.WeightedRandomHostWeightPairs.Name] = weightPairs;
    }

    public WeightedRandomHostSelector() : this(new Random()) { }

    public WeightedRandomHostSelector(Random random)
    {
        this._random = random;
    }

    public HostSpec GetHost(IList<HostSpec> hosts, HostRole hostRole, Dictionary<string, string> props)
    {
        var hostWeightMap = this.GetHostWeightPairMap(PropertyDefinition.WeightedRandomHostWeightPairs.GetString(props));

        var eligibleHosts = hosts
            .Where(hostSpec => hostRole == hostSpec.Role && hostSpec.Availability == HostAvailability.Available)
            .OrderBy(hostSpec => hostSpec.Host)
            .ToList();

        if (eligibleHosts.Count == 0)
        {
            throw new InvalidOperationException(string.Format(Resources.Error_NoHostsMatching, hostRole));
        }

        var hostWeightRangeMap = new Dictionary<string, NumberRange>();
        int counter = 1;

        foreach (var host in eligibleHosts)
        {
            if (!hostWeightMap.ContainsKey(host.Host))
            {
                continue;
            }

            int hostWeight = hostWeightMap[host.Host];
            if (hostWeight > 0)
            {
                int rangeStart = counter;
                int rangeEnd = counter + hostWeight - 1;
                hostWeightRangeMap[host.Host] = new NumberRange(rangeStart, rangeEnd);
                counter += hostWeight;
            }
            else
            {
                hostWeightRangeMap[host.Host] = new NumberRange(counter, counter);
                counter++;
            }
        }

        int randomInt = this._random.Next(counter);

        foreach (var host in eligibleHosts)
        {
            if (hostWeightRangeMap.TryGetValue(host.Host, out var range) && range.IsInRange(randomInt))
            {
                return host;
            }
        }

        throw new InvalidOperationException(string.Format(Resources.Error_WeightedRandomUnableToGetHost, hostRole));
    }

    private Dictionary<string, int> GetHostWeightPairMap(string? hostWeightMapString)
    {
        lock (this._lock)
        {
            if (this._cachedHostWeightMapString != null &&
                this._cachedHostWeightMapString.Trim().Equals(hostWeightMapString?.Trim()) &&
                this._cachedHostWeightMap != null &&
                this._cachedHostWeightMap.Count > 0)
            {
                return this._cachedHostWeightMap;
            }

            var hostWeightMap = new Dictionary<string, int>();
            if (string.IsNullOrWhiteSpace(hostWeightMapString))
            {
                return hostWeightMap;
            }

            string[] hostWeightPairs = hostWeightMapString.Split(',');
            foreach (string hostWeightPair in hostWeightPairs)
            {
                Match match = HostWeightPairsPattern().Match(hostWeightPair.Trim());
                if (!match.Success)
                {
                    throw new InvalidOperationException(Resources.Error_WeightedRandomInvalidHostWeightPairs);
                }

                string hostName = match.Groups["host"].Value.Trim();
                string hostWeight = match.Groups["weight"].Value.Trim();

                if (string.IsNullOrEmpty(hostName) || string.IsNullOrEmpty(hostWeight))
                {
                    throw new InvalidOperationException(Resources.Error_WeightedRandomInvalidHostWeightPairs);
                }

                if (int.TryParse(hostWeight, out int weight) && weight >= DefaultWeight)
                {
                    hostWeightMap[hostName] = weight;
                }
                else
                {
                    throw new InvalidOperationException(Resources.Error_WeightedRandomInvalidHostWeightPairs);
                }
            }

            this._cachedHostWeightMap = hostWeightMap;
            this._cachedHostWeightMapString = hostWeightMapString;
            return hostWeightMap;
        }
    }

    private class NumberRange
    {
        private readonly int _start;
        private readonly int _end;

        public NumberRange(int start, int end)
        {
            this._start = start;
            this._end = end;
        }

        public bool IsInRange(int value)
        {
            return this._start <= value && value <= this._end;
        }
    }
}
