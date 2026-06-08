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
using AwsWrapperDataProvider.Driver.HostInfo;

namespace AwsWrapperDataProvider.Driver.Utils;

/// <summary>
/// Caches the result of identifying a connection keyed by the connection host name.
///
/// <para>For RDS instance endpoints the connection host already identifies the instance, so the provided host
/// specification is returned directly. For static custom domain names or IP addresses, the identified instance is
/// stable, so the identifier is cached and re-resolved against the current topology on subsequent calls. This avoids
/// re-querying the database for the same host while still reflecting topology changes such as role swaps. Cluster and
/// other dynamic endpoints are never cached since they may resolve to a different instance at any time.</para>
/// </summary>
public class HostIdCacheService : IHostIdCacheService
{
    private static readonly ConcurrentDictionary<string, CachedHostIdentity> Cache = new();

    public async Task<HostSpec?> IdentifyConnectionAsync(
        DbConnection connection,
        HostSpec connectionHostSpec,
        IPluginService pluginService,
        Dictionary<string, string> props,
        DbTransaction? transaction = null)
    {
        RdsUrlType urlType = RdsUtils.IdentifyRdsType(connectionHostSpec.Host);

        if (urlType == RdsUrlType.RdsInstance)
        {
            return connectionHostSpec;
        }

        if (urlType == RdsUrlType.IpAddress || urlType == RdsUrlType.Other)
        {
            bool isEnabled = PropertyDefinition.HostCacheEnabled.GetBoolean(props);
            string hostRegexp = PropertyDefinition.HostCacheRegexp.GetString(props) ?? ".*";
            if (isEnabled && Regex.IsMatch(connectionHostSpec.Host, hostRegexp))
            {
                return await this.GetCachedHostSpecAsync(connection, connectionHostSpec, pluginService, transaction);
            }

            return await pluginService.IdentifyConnectionAsync(connection, transaction);
        }

        // Other hosts are dynamic and may change any time so they can't be cached.
        return await pluginService.IdentifyConnectionAsync(connection, transaction);
    }

    protected virtual async Task<HostSpec?> GetCachedHostSpecAsync(
        DbConnection connection,
        HostSpec connectionHostSpec,
        IPluginService pluginService,
        DbTransaction? transaction)
    {
        if (!Cache.TryGetValue(connectionHostSpec.Host, out CachedHostIdentity? cached))
        {
            HostSpec? identified = await pluginService.IdentifyConnectionAsync(connection, transaction);
            cached = identified == null
                ? new CachedHostIdentity(null, null)
                : new CachedHostIdentity(identified.HostId, identified.Host);
            Cache[connectionHostSpec.Host] = cached;
        }

        if (cached.HostId == null && cached.Host == null)
        {
            // We've already tried to identify the connection, but we got nothing.
            return null;
        }

        IList<HostSpec> topology = pluginService.AllHosts;
        if (topology == null || topology.Count == 0)
        {
            await pluginService.ForceRefreshHostListAsync();
            topology = pluginService.AllHosts;
            if (topology == null || topology.Count == 0)
            {
                return null;
            }
        }

        return topology.FirstOrDefault(host =>
            (cached.HostId != null && cached.HostId == host.HostId)
            || (cached.Host != null && cached.Host == host.Host));
    }

    /// <summary>
    /// Clears the cached host identities. Intended for use by tests.
    /// </summary>
    internal static void ClearCache()
    {
        Cache.Clear();
    }

    private sealed class CachedHostIdentity(string? hostId, string? host)
    {
        public string? HostId { get; } = hostId;

        public string? Host { get; } = host;
    }
}
