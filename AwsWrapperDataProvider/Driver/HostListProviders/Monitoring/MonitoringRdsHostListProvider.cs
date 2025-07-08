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
using System.Data;
using System.Data.Common;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;

namespace AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;

/// <summary>
/// RDS host list provider with enhanced monitoring capabilities for cluster topology changes.
/// This provider uses background monitoring to detect topology changes and maintain up-to-date host information.
/// </summary>
public class MonitoringRdsHostListProvider : RdsHostListProvider
{
    public static readonly AwsWrapperProperty ClusterTopologyHighRefreshRateMs =
        new("ClusterTopologyHighRefreshRateMs", "100", "Cluster topology high refresh rate in milliseconds.");

    protected static readonly TimeSpan CacheCleanupInterval = TimeSpan.FromMinutes(1);
    protected static readonly TimeSpan MonitorExpirationTime = TimeSpan.FromMinutes(15);
    protected static readonly TimeSpan TopologyCacheExpirationTime = TimeSpan.FromMinutes(5);

    protected static readonly ConcurrentDictionary<string, IClusterTopologyMonitor> Monitors = new();
    protected static readonly Timer CleanupTimer;

    protected readonly IPluginService pluginService;
    protected readonly TimeSpan highRefreshRate;
    protected readonly string isWriterQuery;

    static MonitoringRdsHostListProvider()
    {
        CleanupTimer = new Timer(CleanupExpiredMonitors, null, CacheCleanupInterval, CacheCleanupInterval);
    }

    public MonitoringRdsHostListProvider(
        Dictionary<string, string> properties,
        IHostListProviderService hostListProviderService,
        string topologyQuery,
        string nodeIdQuery,
        string isReaderQuery,
        string isWriterQuery,
        IPluginService pluginService) : base(properties, hostListProviderService, topologyQuery, nodeIdQuery, isReaderQuery)
    {
        this.pluginService = pluginService;
        this.isWriterQuery = isWriterQuery;
        this.highRefreshRate = TimeSpan.FromMilliseconds(
            ClusterTopologyHighRefreshRateMs.GetLong(this.properties) ?? 100);
    }

    /// <summary>
    /// Clears all cached topology data.
    /// </summary>
    public static void ClearCache()
    {
        ClearAll();
    }

    /// <summary>
    /// Closes all active monitors and clears the cache.
    /// </summary>
    public static void CloseAllMonitors()
    {
        foreach (var monitor in Monitors.Values)
        {
            try
            {
                monitor.Dispose();
            }
            catch
            {
                // Ignore disposal errors
            }
        }

        Monitors.Clear();
        ClearCache();
    }

    /// <summary>
    /// Cleanup timer callback to remove expired monitors.
    /// </summary>
    private static void CleanupExpiredMonitors(object? state)
    {
        var expiredKeys = new List<string>();

        foreach (var kvp in Monitors)
        {
            if (kvp.Value.CanDispose)
            {
                expiredKeys.Add(kvp.Key);
            }
        }

        foreach (var key in expiredKeys)
        {
            if (Monitors.TryRemove(key, out var monitor))
            {
                try
                {
                    monitor.Dispose();
                }
                catch
                {
                    // Ignore disposal errors
                }
            }
        }
    }

    /// <summary>
    /// Initializes or retrieves the cluster topology monitor for this cluster.
    /// </summary>
    /// <returns>The cluster topology monitor instance.</returns>
    protected IClusterTopologyMonitor InitMonitor()
    {
        return Monitors.GetOrAdd(this.ClusterId, key => new ClusterTopologyMonitor(
            key,
            TopologyCache,
            this.initialHostSpec!,
            this.properties,
            this.pluginService,
            this.hostListProviderService,
            this.clusterInstanceTemplate!,
            this.topologyRefreshRate,
            this.highRefreshRate,
            TopologyCacheExpirationTime,
            this.topologyQuery,
            this.isWriterQuery,
            this.nodeIdQuery));
    }

    /// <summary>
    /// Queries for topology using the monitoring infrastructure.
    /// </summary>
    /// <param name="connection">The database connection to use for querying.</param>
    /// <returns>List of host specifications or null if query fails.</returns>
    internal override List<HostSpec> QueryForTopology(IDbConnection connection)
    {
        var monitor = Monitors.GetValueOrDefault(this.ClusterId) ?? this.InitMonitor();

        try
        {
            var task = monitor.ForceRefreshAsync((DbConnection)connection, DefaultTopologyQueryTimeoutSec * 1000);
            task.Wait(TimeSpan.FromSeconds(DefaultTopologyQueryTimeoutSec + 5));

            if (task.IsCompletedSuccessfully)
            {
                return task.Result.ToList();
            }
        }
        catch (AggregateException ex) when (ex.InnerException is TimeoutException)
        {
            // Handle timeout
        }
        catch (TimeoutException)
        {
            // Handle timeout
        }
        catch
        {
            // Handle other exceptions
        }

        return new List<HostSpec>();
    }

    /// <summary>
    /// Handles cluster ID changes by updating the monitor.
    /// </summary>
    /// <param name="oldClusterId">The previous cluster ID.</param>
    protected virtual void ClusterIdChanged(string oldClusterId)
    {
        if (Monitors.TryGetValue(oldClusterId, out var existingMonitor))
        {
            Monitors.TryAdd(this.ClusterId, existingMonitor);
            existingMonitor.SetClusterId(this.ClusterId);
            Monitors.TryRemove(oldClusterId, out _);
        }

        // Transfer cached topology data
        if (TopologyCache.TryGetValue(oldClusterId, out List<HostSpec>? existingHosts) && existingHosts != null)
        {
            TopologyCache.Set(this.ClusterId, existingHosts, TopologyCacheExpirationTime);
        }
    }

    /// <summary>
    /// Forces a refresh of the cluster topology with writer verification.
    /// </summary>
    /// <param name="shouldVerifyWriter">Whether to verify the writer connection.</param>
    /// <param name="timeoutMs">Timeout in milliseconds.</param>
    /// <returns>List of host specifications.</returns>
    /// <exception cref="TimeoutException">Thrown when the operation times out.</exception>
    public virtual async Task<IList<HostSpec>> ForceRefreshAsync(bool shouldVerifyWriter, long timeoutMs)
    {
        var monitor = Monitors.GetValueOrDefault(this.ClusterId) ?? this.InitMonitor();
        return await monitor.ForceRefreshAsync(shouldVerifyWriter, timeoutMs);
    }

    /// <summary>
    /// Forces a refresh of the cluster topology.
    /// </summary>
    /// <param name="connection">The database connection to use.</param>
    /// <returns>List of host specifications.</returns>
    public new IList<HostSpec> ForceRefresh(IDbConnection? connection)
    {
        this.EnsureInitialized();

        var monitor = Monitors.GetValueOrDefault(this.ClusterId) ?? this.InitMonitor();

        try
        {
            var task = monitor.ForceRefreshAsync((DbConnection?)connection, DefaultTopologyQueryTimeoutSec * 1000);
            task.Wait(TimeSpan.FromSeconds(DefaultTopologyQueryTimeoutSec + 5));

            if (task.IsCompletedSuccessfully)
            {
                this.hostList = task.Result.ToList();
                return this.hostList.AsReadOnly();
            }
        }
        catch (AggregateException ex) when (ex.InnerException is TimeoutException)
        {
            // Handle timeout - fall back to base implementation
        }
        catch (TimeoutException)
        {
            // Handle timeout - fall back to base implementation
        }
        catch
        {
            // Handle other exceptions - fall back to base implementation
        }

        // Fall back to base implementation if monitoring fails
        return base.ForceRefresh(connection);
    }

    /// <summary>
    /// Refreshes the cluster topology.
    /// </summary>
    /// <param name="connection">The database connection to use.</param>
    /// <returns>List of host specifications.</returns>
    public new IList<HostSpec> Refresh(IDbConnection? connection)
    {
        this.EnsureInitialized();

        // Check if we have a cached topology first
        if (TopologyCache.TryGetValue(this.ClusterId, out List<HostSpec>? cachedHosts) && cachedHosts != null)
        {
            this.hostList = cachedHosts;
            return this.hostList.AsReadOnly();
        }

        // If no cached data, force a refresh
        return this.ForceRefresh(connection);
    }
}
