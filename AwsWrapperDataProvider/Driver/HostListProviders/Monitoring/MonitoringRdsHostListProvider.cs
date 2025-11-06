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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;

/// <summary>
/// RDS host list provider with enhanced monitoring capabilities for cluster topology changes.
/// This provider uses background monitoring to detect topology changes and maintain up-to-date host information.
/// </summary>
public class MonitoringRdsHostListProvider : RdsHostListProvider, IBlockingHostListProvider
{
    private static readonly ILogger<MonitoringRdsHostListProvider> Logger = LoggerUtils.GetLogger<MonitoringRdsHostListProvider>();

    protected static readonly TimeSpan MonitorExpirationTime = TimeSpan.FromMinutes(15);
    protected static readonly TimeSpan TopologyCacheExpirationTime = TimeSpan.FromMinutes(5);

    /// <summary>
    /// MemoryCache that stores ClusterTopologyMonitors.
    /// </summary>
    internal static MemoryCache Monitors = new(new MemoryCacheOptions
    {
        SizeLimit = 100,
    });

    protected readonly IPluginService pluginService;
    protected readonly TimeSpan highRefreshRate;
    protected readonly string isWriterQuery;

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
        this.highRefreshRate = TimeSpan.FromMilliseconds(PropertyDefinition.ClusterTopologyHighRefreshRateMs.GetLong(this.properties) ?? 100);
    }

    public static void CloseAllMonitors()
    {
        Monitors.Clear();
        ClearAll();
    }

    public async Task<IList<HostSpec>> ForceRefreshAsync(bool shouldVerifyWriter, long timeoutMs)
    {
        IClusterTopologyMonitor monitor = Monitors.Get<IClusterTopologyMonitor>(this.ClusterId) ?? this.InitMonitor();

        IList<HostSpec> hosts = await monitor.ForceRefreshAsync(shouldVerifyWriter, timeoutMs);
        this.hostList = [.. hosts];
        return this.hostList.AsReadOnly();
    }

    protected virtual IClusterTopologyMonitor InitMonitor()
    {
        Logger.LogTrace("Initializing new cluster topology monitor for clusterId: {clusterId}", this.ClusterId);
        return Monitors.Set(
            this.ClusterId,
            new ClusterTopologyMonitor(
                this.ClusterId,
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
                this.nodeIdQuery),
            this.CreateCacheEntryOptions());
    }

    internal override async Task<List<HostSpec>?> QueryForTopologyAsync(DbConnection connection)
    {
        // Get monitor with automatic expiration check (like Java's monitors.get with expiration)
        var monitor = Monitors.Get<IClusterTopologyMonitor>(this.ClusterId) ?? this.InitMonitor();
        try
        {
            var topology = await monitor.ForceRefreshAsync(connection, DefaultTopologyQueryTimeoutSec * 1000);
            return [.. topology];
        }
        catch (TimeoutException)
        {
            return null;
        }
    }

    protected override void ClusterIdChanged(string oldClusterId)
    {
        Logger.LogTrace("Cluster Id changed, old cluster id: {id}", oldClusterId);
        this.TransferExistingMonitor(oldClusterId);
        this.TransferCachedTopology(oldClusterId);
    }

    private void TransferExistingMonitor(string oldClusterId)
    {
        var existingMonitor = Monitors.Get<IClusterTopologyMonitor>(oldClusterId);
        if (existingMonitor == null)
        {
            return;
        }

        var cacheOptions = this.CreateCacheEntryOptions();
        Monitors.Set(this.ClusterId, existingMonitor, cacheOptions);
        existingMonitor.SetClusterId(this.ClusterId);
        Monitors.Remove(oldClusterId);
    }

    private void TransferCachedTopology(string oldClusterId)
    {
        if (TopologyCache.TryGetValue(oldClusterId, out List<HostSpec>? existingHosts) && existingHosts != null)
        {
            TopologyCache.Set(this.ClusterId, existingHosts, TopologyCacheExpirationTime);
        }
    }

    private void OnMonitorEvicted(object key, object? value, EvictionReason reason, object? state)
    {
        if (value is IClusterTopologyMonitor evictedMonitor)
        {
            try
            {
                Logger.LogTrace("Disposing cluster topology monitor for clusterId: {clusterId} due to eviction reason: {reason}", key, reason);
                evictedMonitor.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogWarning("Error disposing clustor topology monitor: {message} ", ex.Message);
            }
        }
    }

    protected MemoryCacheEntryOptions CreateCacheEntryOptions()
    {
        return new MemoryCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = MonitorExpirationTime,
            Size = 1,
            PostEvictionCallbacks =
            {
                new PostEvictionCallbackRegistration
                {
                    EvictionCallback = this.OnMonitorEvicted,
                },
            },
        };
    }
}
