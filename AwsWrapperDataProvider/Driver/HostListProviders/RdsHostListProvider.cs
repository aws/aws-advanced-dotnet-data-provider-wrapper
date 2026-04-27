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
using System.Globalization;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.HostListProviders;

public class RdsHostListProvider : IDynamicHostListProvider
{
    protected const int DefaultTopologyQueryTimeoutSec = 5;

    private static readonly ILogger<RdsHostListProvider> Logger = LoggerUtils.GetLogger<RdsHostListProvider>();
    internal static readonly MemoryCache TopologyCache = new(new MemoryCacheOptions());
    internal static readonly MemoryCache PrimaryClusterIdCache = new(new MemoryCacheOptions());
    internal static readonly MemoryCache SuggestedPrimaryClusterIdCache = new(new MemoryCacheOptions());
    protected static readonly TimeSpan SuggestedClusterIdRefreshRate = TimeSpan.FromMinutes(10);

    protected static readonly TimeSpan MonitorExpirationTime = TimeSpan.FromMinutes(15);
    protected static readonly TimeSpan TopologyCacheExpirationTime = TimeSpan.FromMinutes(5);

    /// <summary>
    /// MemoryCache that stores ClusterTopologyMonitors.
    /// </summary>
    internal static MemoryCache Monitors = new(new MemoryCacheOptions
    {
        SizeLimit = 100,
    });

    protected readonly Lazy<object> init;
    protected readonly Dictionary<string, string> properties;
    protected readonly IHostListProviderService hostListProviderService;
    protected readonly string nodeIdQuery;

    protected readonly IPluginService pluginService;
    protected readonly TimeSpan highRefreshRate;

    protected readonly TopologyUtils topologyUtils;

    protected List<HostSpec> hostList = [];
    protected List<HostSpec> initialHostList = [];
    protected HostSpec? initialHostSpec;
    protected HostSpec? clusterInstanceTemplate;
    internal string ClusterId = string.Empty;
    protected RdsUrlType rdsUrlType = RdsUrlType.Other;
    protected TimeSpan topologyRefreshRate = TimeSpan.FromMilliseconds(30000);

    // A primary ClusterId is a ClusterId that is based off of a cluster endpoint URL
    // (rather than a GUID or a value provided by the user).
    internal bool IsPrimaryClusterId = false;

    public RdsHostListProvider(
        Dictionary<string, string> properties,
        IHostListProviderService hostListProviderService,
        string nodeIdQuery,
        IPluginService pluginService,
        TopologyUtils topologyUtils)
    {
        this.properties = properties;
        this.hostListProviderService = hostListProviderService;
        this.nodeIdQuery = nodeIdQuery;
        this.pluginService = pluginService;
        this.topologyUtils = topologyUtils;
        this.highRefreshRate = TimeSpan.FromMilliseconds(PropertyDefinition.ClusterTopologyHighRefreshRateMs.GetLong(this.properties) ?? 100);
        this.init = new Lazy<object>(() =>
        {
            this.Init();
            return default!;
        },
        isThreadSafe: true);
    }

    public static void ClearAll()
    {
        TopologyCache.Clear();
        PrimaryClusterIdCache.Clear();
        SuggestedPrimaryClusterIdCache.Clear();
    }

    public static void CloseAllMonitors()
    {
        Monitors.Clear();
        ClearAll();
    }

    internal void EnsureInitialized()
    {
        _ = this.init.Value;
    }

    protected virtual void Init()
    {
        this.initialHostList.AddRange(ConnectionPropertiesUtils.GetHostsFromProperties(
                this.properties,
                this.hostListProviderService.HostSpecBuilder,
                false));
        if (this.initialHostList.Count == 0)
        {
            // TODO: move error string to resx file.
            throw new InvalidOperationException(string.Format(Resources.Error_NotFoundInConnectionString, "primaryClusterHosts"));
        }

        Logger.LogDebug(Resources.RdsHostListProvider_Init_InitialHostCount, this.initialHostList.Count);
        foreach (var host in this.initialHostList)
        {
            Logger.LogTrace(Resources.RdsHostListProvider_Init_InitialHostsAndCluster, host.Host, RdsUtils.IsRdsClusterDns(host.Host));
        }

        this.initialHostSpec = this.initialHostList[0];
        this.hostListProviderService.InitialConnectionHostSpec = this.initialHostSpec;
        this.ClusterId = Guid.NewGuid().ToString();
        this.IsPrimaryClusterId = false;
        this.topologyRefreshRate = TimeSpan.FromMilliseconds(PropertyDefinition.ClusterTopologyRefreshRateMs.GetLong(this.properties) ?? 30000);

        HostSpecBuilder hostSpecBuilder = this.hostListProviderService.HostSpecBuilder;
        string? clusterInstancePattern = PropertyDefinition.ClusterInstanceHostPattern.GetString(this.properties);
        this.clusterInstanceTemplate = clusterInstancePattern != null
            ? ConnectionPropertiesUtils.ParseHostPortPair(clusterInstancePattern, hostSpecBuilder)
            : hostSpecBuilder
                .WithHost(RdsUtils.GetRdsInstanceHostPattern(this.initialHostSpec.Host))
                .WithPort(this.initialHostSpec.Port)
                .WithHostId(this.initialHostSpec.HostId)
                .Build();
        Logger.LogDebug(Resources.RdsHostListProvider_Init_ClusterInstanceTemplate, this.clusterInstanceTemplate.Host, this.initialHostSpec.Host);
        this.ValidateHostPattern(this.clusterInstanceTemplate.Host);
        this.rdsUrlType = RdsUtils.IdentifyRdsType(this.initialHostSpec.Host);
        string clusterIdSetting = PropertyDefinition.ClusterId.GetString(this.properties) ?? string.Empty;
        if (!string.IsNullOrEmpty(clusterIdSetting))
        {
            this.ClusterId = clusterIdSetting;
        }
        else if (this.rdsUrlType == RdsUrlType.RdsProxy)
        {
            this.ClusterId = this.initialHostSpec.GetHostAndPort();
        }
        else if (this.rdsUrlType.IsRds)
        {
            ClusterSuggestedResult? clusterSuggestedResult = this.GetSuggestedClusterId(this.initialHostSpec.GetHostAndPort());
            if (clusterSuggestedResult != null && !string.IsNullOrEmpty(clusterSuggestedResult.Value.ClusterId))
            {
                this.ClusterId = clusterSuggestedResult.Value.ClusterId;
                this.IsPrimaryClusterId = clusterSuggestedResult.Value.IsPrimaryClusterId;
            }
            else
            {
                string? clusterRdsHostUrl = RdsUtils.GetRdsClusterHostUrl(this.initialHostSpec.Host);
                if (!string.IsNullOrEmpty(clusterRdsHostUrl))
                {
                    this.ClusterId = this.clusterInstanceTemplate.IsPortSpecified ?
                        $"{clusterRdsHostUrl}:{this.clusterInstanceTemplate.Port}" :
                        clusterRdsHostUrl;
                    this.IsPrimaryClusterId = true;
                    PrimaryClusterIdCache.Set(this.ClusterId, true, SuggestedClusterIdRefreshRate);
                }
            }
        }
    }

    protected ClusterSuggestedResult? GetSuggestedClusterId(string url)
    {
        foreach (string clusterId in TopologyCache.Keys.Cast<string>())
        {
            List<HostSpec>? hosts = TopologyCache.Get<List<HostSpec>>(clusterId);
            bool isPrimaryCluster = PrimaryClusterIdCache.GetOrCreate(clusterId, entry =>
            {
                entry.AbsoluteExpirationRelativeToNow = SuggestedClusterIdRefreshRate;
                return false;
            });

            if (clusterId.Equals(url))
            {
                return new ClusterSuggestedResult(url, isPrimaryCluster);
            }

            if (hosts == null)
            {
                continue;
            }

            foreach (HostSpec hostSpec in hosts)
            {
                if (hostSpec.GetHostAndPort().Equals(url))
                {
                    return new ClusterSuggestedResult(clusterId, isPrimaryCluster);
                }
            }
        }

        return null;
    }

    protected void ValidateHostPattern(string hostPattern)
    {
        if (!RdsUtils.IsDnsPatternValid(hostPattern))
        {
            throw new InvalidOperationException(string.Format(Resources.Error_InvalidHostPattern, hostPattern));
        }

        RdsUrlType rdsUrlType = RdsUtils.IdentifyRdsType(hostPattern);
        if (rdsUrlType == RdsUrlType.RdsProxy)
        {
            throw new InvalidOperationException(Resources.Error_InvalidRdsProxyUrl);
        }

        if (rdsUrlType == RdsUrlType.RdsCustomCluster)
        {
            throw new InvalidOperationException(Resources.Error_InvalidCustomRdsProxyUrl);
        }
    }

    public async Task<IList<HostSpec>> ForceRefreshAsync(bool shouldVerifyWriter, long timeoutMs)
    {
        this.EnsureInitialized();
        if (!this.pluginService.IsDialectConfirmed)
        {
            // We need to confirm the dialect before creating a topology monitor so that it uses the correct SQL queries.
            // We will return the original hosts parsed from the connection string until the dialect has been confirmed.
            return this.initialHostList;
        }

        var hosts = await this.ForceRefreshMonitorAsync(shouldVerifyWriter, timeoutMs);
        if (hosts != null)
        {
            this.hostList = [.. hosts];
        }

        return this.hostList.AsReadOnly();
    }

    protected internal virtual async Task<IList<HostSpec>?> ForceRefreshMonitorAsync(bool shouldVerifyWriter, long timeoutMs)
    {
        IClusterTopologyMonitor monitor = Monitors.Get<IClusterTopologyMonitor>(this.ClusterId) ?? this.InitMonitor();
        try
        {
            return await monitor.ForceRefreshAsync(shouldVerifyWriter, timeoutMs);
        }
        catch (TimeoutException ex)
        {
            Logger.LogDebug(Resources.MonitoringRdsHostListProvider_QueryForTopologyAsync_TimedOut, ex.Message);
            return null;
        }
    }

    protected virtual IClusterTopologyMonitor InitMonitor()
    {
        Logger.LogTrace(Resources.MonitoringRdsHostListProvider_InitMonitor, this.ClusterId);

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
                this.nodeIdQuery,
                this.topologyUtils),
            this.CreateCacheEntryOptions());
    }

    protected virtual void ClusterIdChanged(string oldClusterId)
    {
        Logger.LogTrace(Resources.MonitoringRdsHostListProvider_ClusterIdChanged, oldClusterId);
        this.TransferExistingMonitor(oldClusterId);
        this.TransferCachedTopology(oldClusterId);
    }

    private void TransferExistingMonitor(string oldClusterId)
    {
        Logger.LogTrace(Resources.MonitoringRdsHostListProvider_TransferExistingMonitor, oldClusterId);
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
        Logger.LogTrace(Resources.MonitoringRdsHostListProvider_TransferCachedTopology, oldClusterId);
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
                Logger.LogTrace(Resources.MonitoringRdsHostListProvider_OnMonitorEvicted_Disposing, key, reason);
                evictedMonitor.Dispose();
            }
            catch (Exception ex)
            {
                Logger.LogWarning(Resources.MonitoringRdsHostListProvider_OnMonitorEvicted_Error, ex.Message);
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

    public virtual async Task<IList<HostSpec>> ForceRefreshAsync()
    {
        return await this.ForceRefreshAsync(false, DefaultTopologyQueryTimeoutSec * 1000);
    }

    public virtual string GetClusterId()
    {
        this.EnsureInitialized();
        return this.ClusterId;
    }

    public virtual async Task<HostSpec?> IdentifyConnectionAsync(DbConnection connection, DbTransaction? transaction = null)
    {
        try
        {
            string instanceName;
            await using (var command = connection.CreateCommand())
            {
                command.CommandText = this.nodeIdQuery;
                command.Transaction = transaction;
                await using var resultSet = await command.ExecuteReaderAsync();

                if (!(await resultSet.ReadAsync()))
                {
                    return null;
                }

                instanceName = Convert.ToString(resultSet.GetValue(0), CultureInfo.InvariantCulture)!;
            }

            IList<HostSpec> topology = await this.RefreshAsync();
            bool isForcedRefresh = false;

            if (topology == null)
            {
                topology = await this.ForceRefreshAsync();
                isForcedRefresh = true;

                if (topology == null)
                {
                    return null;
                }
            }

            HostSpec? foundHost = topology.FirstOrDefault(host => host.HostId == instanceName);

            if (foundHost == null && !isForcedRefresh)
            {
                topology = await this.ForceRefreshAsync();
                if (topology == null)
                {
                    return null;
                }

                foundHost = topology.FirstOrDefault(host => host.HostId == instanceName);
            }

            return foundHost;
        }
        catch (DbException ex)
        {
            Logger.LogError(ex, Resources.Error_ObtainingConnectionHostId);
            throw;
        }
    }

    public virtual async Task<IList<HostSpec>> RefreshAsync()
    {
        this.EnsureInitialized();
        FetchTopologyResult result = await this.GetTopologyAsync();
        this.hostList = result.Hosts;
        return this.hostList.AsReadOnly();
    }

    internal async Task<FetchTopologyResult> GetTopologyAsync()
    {
        this.EnsureInitialized();

        string? suggestedPrimaryClusterId = SuggestedPrimaryClusterIdCache.Get<string>(this.ClusterId);
        if (!string.IsNullOrEmpty(suggestedPrimaryClusterId) && !this.ClusterId.Equals(suggestedPrimaryClusterId))
        {
            Logger.LogDebug(Resources.RdsHostListProvider_GetTopologyAsync_ClusterIdChanged, this.ClusterId, suggestedPrimaryClusterId);
            this.ClusterIdChanged(this.ClusterId);
            this.ClusterId = suggestedPrimaryClusterId;
            this.IsPrimaryClusterId = true;
        }

        List<HostSpec>? storedHosts = TopologyCache.Get<List<HostSpec>>(this.ClusterId);
        if (storedHosts == null)
        {
            if (!this.pluginService.IsDialectConfirmed)
            {
                // We need to confirm the dialect before creating a topology monitor so that it uses the correct SQL queries.
                // We will return the original hosts parsed from the connection string until the dialect has been confirmed.
                return new FetchTopologyResult(false, this.initialHostList);
            }

            // Need to re-fetch topology via the monitor
            var hosts = await this.ForceRefreshMonitorAsync(false, DefaultTopologyQueryTimeoutSec * 1000);
            if (hosts != null && hosts.Count > 0)
            {
                Logger.LogTrace(Resources.RdsHostListProvider_GetTopologyAsync_NewTopology, LoggerUtils.LogTopology(hosts, "New"));
                return new FetchTopologyResult(false, [.. hosts]);
            }
        }

        storedHosts = TopologyCache.Get<List<HostSpec>>(this.ClusterId);
        if (storedHosts == null)
        {
            Logger.LogWarning(Resources.RdsHostListProvider_GetTopologyAsync_FallingBackToInitialHost);
            Logger.LogTrace(Resources.RdsHostListProvider_GetTopologyAsync_FallbackTopology, LoggerUtils.LogTopology(this.initialHostList, "Query failed fallback"));
            return new FetchTopologyResult(false, this.initialHostList);
        }

        Logger.LogTrace(Resources.RdsHostListProvider_GetTopologyAsync_CachedTopology, LoggerUtils.LogTopology(storedHosts, "From cache"));
        return new FetchTopologyResult(true, storedHosts);
    }

    protected readonly struct ClusterSuggestedResult(string clusterId, bool isPrimaryClusterId)
    {
        public string ClusterId { get; } = clusterId;

        public bool IsPrimaryClusterId { get; } = isPrimaryClusterId;
    }

    internal class FetchTopologyResult(bool isCachedData, List<HostSpec> hosts)
    {
        public bool IsCachedData { get; } = isCachedData;

        public List<HostSpec> Hosts { get; } = hosts;
    }
}
