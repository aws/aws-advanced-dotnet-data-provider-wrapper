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

using System.Data.Common;

namespace AwsWrapperDataProvider.Driver.HostInfo.HostSelectors;

/// <summary>
/// Host selector that randomly selects an available host with the requested role.
/// </summary>
public class RandomHostSelector : IHostSelector
{
    public static string StrategyName { get; } = "Random";
    private static readonly Random Random = new();

    /// <summary>
    /// Selects a random host with the requested role from the given host list.
    /// </summary>
    /// <param name="hosts">A list of available hosts to pick from.</param>
    /// <param name="hostRole">The desired host role - either a writer or a reader.</param>
    /// <param name="props">Connection properties that may be needed by the host selector.</param>
    /// <returns>A host matching the requested role.</returns>
    /// <exception cref="InvalidOperationException">If the host list does not contain any hosts matching the requested role.</exception>
    public HostSpec GetHost(List<HostSpec> hosts, HostRole hostRole, Dictionary<string, string> props)
    {
        var eligibleHosts = hosts
            .Where(hostSpec => hostRole == hostSpec.Role && hostSpec.Availability == HostAvailability.Available)
            .ToList();

        if (eligibleHosts.Count == 0)
        {
            throw new InvalidOperationException($"No hosts found matching role: {hostRole}");
        }

        var randomIndex = Random.Next(eligibleHosts.Count);
        return eligibleHosts[randomIndex];
    }
}
