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
/// Host selector that selects the available host with the highest weight for the requested role.
/// </summary>
public class HighestWeightHostSelector : IHostSelector
{
    public static string StrategyName { get; } = "HighestWeight";

    public HostSpec GetHost(List<HostSpec> hosts, HostRole hostRole, Dictionary<string, string> props)
    {
        List<HostSpec> eligibleHosts = hosts
            .Where(hostSpec => hostRole == hostSpec.Role && hostSpec.Availability == HostAvailability.Available)
            .ToList();

        if (eligibleHosts.Count == 0)
        {
            throw new InvalidOperationException($"No hosts found matching role: {hostRole}");
        }

        return eligibleHosts
            .OrderByDescending(hostSpec => hostSpec.Weight)
            .First();
    }
}
