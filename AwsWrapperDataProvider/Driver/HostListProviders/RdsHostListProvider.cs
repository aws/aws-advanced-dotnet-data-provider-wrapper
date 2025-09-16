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

using System.Data;
using System.Data.Common;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
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

    protected readonly Lazy<object> init;
    protected readonly Dictionary<string, string> properties;
    protected readonly IHostListProviderService hostListProviderService;
    protected readonly string topologyQuery;
    protected readonly string nodeIdQuery;
    protected readonly string isReaderQuery;

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
        string topologyQuery,
        string nodeIdQuery,
        string isReaderQuery)
    {
        this.properties = properties;
        this.hostListProviderService = hostListProviderService;
        this.topologyQuery = topologyQuery;
        this.nodeIdQuery = nodeIdQuery;
        this.isReaderQuery = isReaderQuery;
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

    internal void EnsureInitialized()
    {
        _ = this.init.Value;
    }

    protected void Init()
    {
        this.initialHostList.AddRange(ConnectionPropertiesUtils.GetHostsFromProperties(
                this.properties,
                this.hostListProviderService.HostSpecBuilder,
                false));
        if (this.initialHostList.Count == 0)
        {
            // TODO: move error string to resx file.
            throw new InvalidOperationException("No primaryClusterHosts found in connection string");
        }

        this.initialHostSpec = this.initialHostList[0];
        this.hostListProviderService.InitialConnectionHostSpec = this.initialHostSpec;
        Logger.LogTrace("Initial host spec: {hostSpec}", this.hostListProviderService.InitialConnectionHostSpec);
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

    private void ValidateHostPattern(string hostPattern)
    {
        if (!RdsUtils.IsDnsPatternValid(hostPattern))
        {
            // TODO : move error string to resx file.
            throw new InvalidOperationException($"Invalid host pattern: {hostPattern}");
        }

        RdsUrlType rdsUrlType = RdsUtils.IdentifyRdsType(hostPattern);
        if (rdsUrlType == RdsUrlType.RdsProxy)
        {
            // TODO : move error string to resx file.
            throw new InvalidOperationException("An RDS Proxy url can't be used as the 'clusterInstanceHostPattern' configuration setting.");
        }

        if (rdsUrlType == RdsUrlType.RdsCustomCluster)
        {
            // TODO : move error string to resx file.
            throw new InvalidOperationException("An RDS Custom Cluster endpoint can't be used as the 'clusterInstanceHostPattern' configuration setting.");
        }
    }

    public virtual IList<HostSpec> ForceRefresh()
    {
        return this.ForceRefresh(null);
    }

    public virtual IList<HostSpec> ForceRefresh(IDbConnection? connection)
    {
        this.EnsureInitialized();
        IDbConnection? currentConnection = connection ?? this.hostListProviderService.CurrentConnection;
        FetchTopologyResult result = this.GetTopology(currentConnection, true);
        Logger.LogTrace(LoggerUtils.LogTopology(result.Hosts, null));

        this.hostList = result.Hosts;
        return this.hostList.AsReadOnly();
    }

    public virtual string GetClusterId()
    {
        this.EnsureInitialized();
        return this.ClusterId;
    }

    public virtual HostSpec? IdentifyConnection(DbConnection connection)
    {
        try
        {
            string instanceName;
            using (var command = connection.CreateCommand())
            {
                command.CommandText = this.nodeIdQuery;
                using var resultSet = command.ExecuteReader();

                if (!resultSet.Read())
                {
                    return null;
                }

                instanceName = resultSet.GetString(0);
            }

            IList<HostSpec> topology = this.Refresh(connection);
            bool isForcedRefresh = false;

            // TODO Clean up if statement
            if (topology == null)
            {
                topology = this.ForceRefresh(connection);
                isForcedRefresh = true;

                if (topology == null)
                {
                    return null;
                }
            }

            HostSpec? foundHost = topology
                .Where(host => host.HostId == instanceName)
                .FirstOrDefault();

            if (foundHost == null && !isForcedRefresh)
            {
                topology = this.ForceRefresh(connection);
                if (topology == null)
                {
                    return null;
                }

                foundHost = topology
                    .Where(host => host.HostId == instanceName)
                    .FirstOrDefault();
            }

            return foundHost;
        }
        catch (DbException ex)
        {
            Logger.LogError(ex, "An error occurred while obtaining the connection's host ID.");
            throw;
        }

        throw new InvalidOperationException("An error occurred while obtaining the connection's host ID.");
    }

    public virtual HostRole GetHostRole(IDbConnection connection)
    {
        using IDbCommand command = connection.CreateCommand();
        command.CommandText = this.isReaderQuery;
        using IDataReader reader = command.ExecuteReader();

        if (reader.Read())
        {
            bool isReader = reader.GetBoolean(0);
            return isReader ? HostRole.Reader : HostRole.Writer;
        }

        // TODO : move error string to resx file.
        throw new InvalidOperationException("Failed to determine host role from the database.");
    }

    public virtual IList<HostSpec> Refresh()
    {
        return this.Refresh(null);
    }

    public virtual IList<HostSpec> Refresh(IDbConnection? connection)
    {
        this.EnsureInitialized();
        IDbConnection? currentConnection = connection ?? this.hostListProviderService.CurrentConnection;
        FetchTopologyResult result = this.GetTopology(currentConnection, false);
        Logger.LogTrace(LoggerUtils.LogTopology(result.Hosts, result.IsCachedData ? "From cache" : "New Topology"));
        this.hostList = result.Hosts;
        return this.hostList.AsReadOnly();
    }

    internal FetchTopologyResult GetTopology(IDbConnection? connection, bool forceUpdate)
    {
        this.EnsureInitialized();

        string? suggestedPrimaryClusterId = SuggestedPrimaryClusterIdCache.Get<string>(this.ClusterId);
        if (!string.IsNullOrEmpty(suggestedPrimaryClusterId) && !this.ClusterId.Equals(suggestedPrimaryClusterId))
        {
            this.ClusterIdChanged(this.ClusterId);
            this.ClusterId = suggestedPrimaryClusterId;
            this.IsPrimaryClusterId = true;
        }

        List<HostSpec>? cachedHosts = TopologyCache.Get<List<HostSpec>>(this.ClusterId);
        bool needToSuggest = cachedHosts == null && this.IsPrimaryClusterId;
        if (cachedHosts == null || forceUpdate)
        {
            if (connection == null || connection.State != ConnectionState.Open)
            {
                return new FetchTopologyResult(false, this.initialHostList);
            }

            List<HostSpec>? hosts = this.QueryForTopology(connection);
            if (hosts != null && hosts.Count > 0)
            {
                TopologyCache.Set(this.ClusterId, hosts, this.topologyRefreshRate);
                if (needToSuggest)
                {
                    this.SuggestPrimaryCluster(hosts);
                }

                return new FetchTopologyResult(false, hosts);
            }
        }

        return cachedHosts == null ? new FetchTopologyResult(false, this.initialHostList) : new FetchTopologyResult(true, cachedHosts);
    }

    private void SuggestPrimaryCluster(List<HostSpec> primaryClusterHosts)
    {
        if (primaryClusterHosts.Count == 0)
        {
            return;
        }

        HashSet<string> primaryClusterHostUrls = [.. primaryClusterHosts.Select(x => x.GetHostAndPort())];

        foreach (string clusterId in TopologyCache.Keys.Cast<string>())
        {
            List<HostSpec>? clusterHosts = TopologyCache.Get<List<HostSpec>>(clusterId);
            bool isPrimaryCluster = PrimaryClusterIdCache.GetOrCreate(clusterId, entry =>
            {
                entry.AbsoluteExpirationRelativeToNow = SuggestedClusterIdRefreshRate;
                return false;
            });
            string? suggestedPrimaryClusterId = SuggestedPrimaryClusterIdCache.Get<string>(clusterId);
            if (isPrimaryCluster || !string.IsNullOrEmpty(suggestedPrimaryClusterId) || clusterHosts == null || clusterHosts.Count == 0)
            {
                continue;
            }

            foreach (HostSpec hostSpec in clusterHosts)
            {
                if (primaryClusterHostUrls.Contains(hostSpec.GetHostAndPort()))
                {
                    SuggestedPrimaryClusterIdCache.Set(clusterId, this.ClusterId, SuggestedClusterIdRefreshRate);
                    break;
                }
            }
        }
    }

    internal virtual List<HostSpec>? QueryForTopology(IDbConnection connection)
    {
        using IDbCommand command = connection.CreateCommand();
        command.CommandTimeout = DefaultTopologyQueryTimeoutSec;
        command.CommandText = this.topologyQuery;
        using IDataReader reader = command.ExecuteReader();

        List<HostSpec> hosts = [];
        List<HostSpec> writers = [];

        while (reader.Read())
        {
            // According to the topology query the result set
            // should contain 5 columns: node ID, 1/0 (writer/reader), CPU utilization, node lag in time, last update timestamp.
            string hostName = reader.GetString(0);
            bool isWriter = reader.GetBoolean(1);
            double cpuUtilization = reader.GetDouble(2);
            double nodeLag = reader.GetDouble(3);
            DateTime lastUpdateTime;
            try
            {
                lastUpdateTime = reader.GetDateTime(4);
            }
            catch (Exception)
            {
                lastUpdateTime = DateTime.UtcNow;
            }

            long weight = (long)((Math.Round(nodeLag) * 100L) + Math.Round(cpuUtilization));
            string endpoint = this.clusterInstanceTemplate!.Host.Replace("?", hostName);
            int port = this.clusterInstanceTemplate.IsPortSpecified
                ? this.clusterInstanceTemplate.Port
                : this.initialHostSpec!.Port;

            HostSpec hostSpec = this.hostListProviderService.HostSpecBuilder
                .WithHost(endpoint)
                .WithHostId(hostName)
                .WithPort(port)
                .WithRole(isWriter ? HostRole.Writer : HostRole.Reader)
                .WithAvailability(HostAvailability.Available)
                .WithWeight(weight)
                .WithLastUpdateTime(lastUpdateTime)
                .Build();
            hostSpec.AddAlias(hostName);

            if (!isWriter)
            {
                hosts.Add(hostSpec);
            }
            else
            {
                writers.Add(hostSpec);
            }
        }

        if (writers.Count == 0)
        {
            // invalid topology
            hosts.Clear();
        }
        else if (writers.Count == 1)
        {
            hosts.Add(writers[0]);
        }
        else
        {
            // Take the latest updated writer node as the current writer. All others will be ignored.
            hosts.Add(writers.MaxBy(x => x.LastUpdateTime)!);
        }

        return hosts;
    }

    protected readonly struct ClusterSuggestedResult(string clusterId, bool isPrimaryClusterId)
    {
        public string ClusterId { get; } = clusterId;

        public bool IsPrimaryClusterId { get; } = isPrimaryClusterId;
    }

    protected virtual void ClusterIdChanged(string clusterId)
    {
        // Do nothing.
    }

    internal class FetchTopologyResult(bool isCachedData, List<HostSpec> hosts)
    {
        public bool IsCachedData { get; } = isCachedData;

        public List<HostSpec> Hosts { get; } = hosts;
    }
}
