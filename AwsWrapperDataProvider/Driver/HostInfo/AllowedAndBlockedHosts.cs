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

namespace AwsWrapperDataProvider.Driver.HostInfo;

/// <summary>
/// Represents the allowed and blocked hosts for connections.
/// Used by plugins like CustomEndpoint to restrict which hosts can be connected to.
/// </summary>
public class AllowedAndBlockedHosts
{
    /// <summary>
    /// Gets the set of allowed host IDs for connections. If null or empty, all host IDs that are not in
    /// <see cref="BlockedHostIds"/> are allowed.
    /// </summary>
    public HashSet<string>? AllowedHostIds { get; }

    /// <summary>
    /// Gets the set of blocked host IDs for connections. If null or empty, all host IDs in
    /// <see cref="AllowedHostIds"/> are allowed. If <see cref="AllowedHostIds"/> is also null or empty, there
    /// are no restrictions on which hosts are allowed.
    /// </summary>
    public HashSet<string>? BlockedHostIds { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="AllowedAndBlockedHosts"/> class.
    /// </summary>
    /// <param name="allowedHostIds">The set of allowed host IDs for connections. If null or empty, all host IDs that are not in
    /// <paramref name="blockedHostIds"/> are allowed.</param>
    /// <param name="blockedHostIds">The set of blocked host IDs for connections. If null or empty, all host IDs in
    /// <paramref name="allowedHostIds"/> are allowed. If <paramref name="allowedHostIds"/> is also null or empty, there
    /// are no restrictions on which hosts are allowed.</param>
    public AllowedAndBlockedHosts(HashSet<string>? allowedHostIds, HashSet<string>? blockedHostIds)
    {
        this.AllowedHostIds = allowedHostIds != null && allowedHostIds.Count > 0 ? allowedHostIds : null;
        this.BlockedHostIds = blockedHostIds != null && blockedHostIds.Count > 0 ? blockedHostIds : null;
    }
}
