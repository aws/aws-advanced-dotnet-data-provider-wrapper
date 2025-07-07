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
using AwsWrapperDataProvider.Driver.HostListProviders;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;

namespace AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;

/// <summary>
/// Implementation of cluster topology monitoring with background refresh capabilities.
/// </summary>
public class ClusterTopologyMonitor : IClusterTopologyMonitor
{
    protected const int DefaultTopologyQueryTimeoutMs = 1000;
    protected const int CloseConnectionNetworkTimeoutMs = 500;
    protected const int DefaultConnectionTimeoutMs = 5000;
    protected const int DefaultSocketTimeoutMs = 5000;

    // Keep monitoring topology with a high rate for 30s after failover
    protected static readonly TimeSpan HighRefreshPeriodAfterPanic = TimeSpan.FromSeconds(30);
    protected static readonly TimeSpan IgnoreTopologyRequestDuration = TimeSpan.FromSeconds(10);

    protected readonly TimeSpan refreshRate;
    protected readonly TimeSpan highRefreshRate;
    protected readonly TimeSpan topologyCacheExpiration;
    protected readonly Dictionary<string, string> properties;
    protected readonly Dictionary<string, string> monitoringProperties;
    protected readonly IPluginService pluginService;
    protected readonly HostSpec initialHostSpec;
    protected readonly MemoryCache topologyMap;
    protected readonly string topologyQuery;
    protected readonly string nodeIdQuery;
    protected readonly string writerTopologyQuery;
    protected readonly IHostListProviderService hostListProviderService;
    protected readonly HostSpec clusterInstanceTemplate;
    protected readonly CancellationTokenSource cancellationTokenSource = new();
    protected readonly object topologyUpdatedLock = new();
    protected readonly SemaphoreSlim topologyUpdatedSemaphore = new(0);
    protected readonly ConcurrentDictionary<string, Task> nodeThreads = new();
    protected readonly Task monitoringTask;
    protected readonly object disposeLock = new();

    protected string clusterId;
    protected HostSpec? writerHostSpec;
    protected DbConnection? monitoringConnection;
    protected bool isVerifiedWriterConnection = false;
    protected DateTime highRefreshRateEndTime = DateTime.MinValue;
    protected volatile bool requestToUpdateTopology = false;
    protected DateTime ignoreNewTopologyRequestsEndTime = DateTime.MinValue;
    protected volatile bool nodeThreadsStop = false;
    protected DbConnection? nodeThreadsWriterConnection;
    protected HostSpec? nodeThreadsWriterHostSpec;
    protected DbConnection? nodeThreadsReaderConnection;
    protected IList<HostSpec>? nodeThreadsLatestTopology;
    protected bool disposed = false;

    public ClusterTopologyMonitor(
        string clusterId,
        MemoryCache topologyMap,
        HostSpec initialHostSpec,
        Dictionary<string, string> properties,
        IPluginService pluginService,
        IHostListProviderService hostListProviderService,
        HostSpec clusterInstanceTemplate,
        TimeSpan refreshRate,
        TimeSpan highRefreshRate,
        TimeSpan topologyCacheExpiration,
        string topologyQuery,
        string writerTopologyQuery,
        string nodeIdQuery)
    {
        this.clusterId = clusterId;
        this.topologyMap = topologyMap;
        this.initialHostSpec = initialHostSpec;
        this.pluginService = pluginService;
        this.hostListProviderService = hostListProviderService;
        this.clusterInstanceTemplate = clusterInstanceTemplate;
        this.properties = properties;
        this.refreshRate = refreshRate;
        this.highRefreshRate = highRefreshRate;
        this.topologyCacheExpiration = topologyCacheExpiration;
        this.topologyQuery = topologyQuery;
        this.writerTopologyQuery = writerTopologyQuery;
        this.nodeIdQuery = nodeIdQuery;

        this.monitoringProperties = new Dictionary<string, string>(properties);

        // Process monitoring-specific properties
        var keysToRemove = new List<string>();
        foreach (var kvp in properties.Where(p => p.Key.StartsWith("topology-monitoring-")))
        {
            string newKey = kvp.Key.Substring("topology-monitoring-".Length);
            this.monitoringProperties[newKey] = kvp.Value;
            keysToRemove.Add(kvp.Key);
        }

        foreach (string key in keysToRemove)
        {
            this.monitoringProperties.Remove(key);
        }

        // Set default values if not provided
        if (!this.monitoringProperties.ContainsKey("SocketTimeout"))
        {
            this.monitoringProperties["SocketTimeout"] = DefaultSocketTimeoutMs.ToString();
        }

        if (!this.monitoringProperties.ContainsKey("ConnectTimeout"))
        {
            this.monitoringProperties["ConnectTimeout"] = DefaultConnectionTimeoutMs.ToString();
        }

        this.monitoringTask = Task.Run(this.RunMonitoringLoop, this.cancellationTokenSource.Token);
    }

    public bool CanDispose => true;

    public void SetClusterId(string clusterId)
    {
        this.clusterId = clusterId;
    }

    public async Task<IList<HostSpec>> ForceRefreshAsync(bool shouldVerifyWriter, long timeoutMs)
    {
        if (this.ignoreNewTopologyRequestsEndTime > DateTime.UtcNow)
        {
            // Previous failover has just completed. We can use results of it without triggering a new topology update.
            if (this.topologyMap.TryGetValue(this.clusterId, out IList<HostSpec>? currentHosts) && currentHosts != null)
            {
                return currentHosts;
            }
        }

        if (shouldVerifyWriter)
        {
            var connectionToClose = Interlocked.Exchange(ref this.monitoringConnection, null);
            this.isVerifiedWriterConnection = false;
            this.CloseConnection(connectionToClose, true);
        }

        return await this.WaitTillTopologyGetsUpdatedAsync(timeoutMs);
    }

    public async Task<IList<HostSpec>> ForceRefreshAsync(DbConnection? connection, long timeoutMs)
    {
        if (this.isVerifiedWriterConnection)
        {
            // Push monitoring thread to refresh topology with a verified connection
            return await this.WaitTillTopologyGetsUpdatedAsync(timeoutMs);
        }

        // Otherwise use provided unverified connection to update topology
        return await this.FetchTopologyAndUpdateCacheAsync(connection) ?? new List<HostSpec>();
    }

    protected async Task<IList<HostSpec>> WaitTillTopologyGetsUpdatedAsync(long timeoutMs)
    {
        this.topologyMap.TryGetValue(this.clusterId, out IList<HostSpec>? currentHosts);

        lock (this.topologyUpdatedLock)
        {
            this.requestToUpdateTopology = true;
        }

        if (timeoutMs == 0)
        {
            return currentHosts ?? new List<HostSpec>();
        }

        var timeout = TimeSpan.FromMilliseconds(timeoutMs);
        var cancellationToken = new CancellationTokenSource(timeout).Token;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await this.topologyUpdatedSemaphore.WaitAsync(1000, cancellationToken);

                if (this.topologyMap.TryGetValue(this.clusterId, out IList<HostSpec>? latestHosts) &&
                    !ReferenceEquals(currentHosts, latestHosts))
                {
                    return latestHosts ?? new List<HostSpec>();
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException($"Topology was not updated within {timeoutMs} milliseconds");
        }

        return currentHosts ?? new List<HostSpec>();
    }

    protected async Task RunMonitoringLoop()
    {
        try
        {
            while (!this.cancellationTokenSource.Token.IsCancellationRequested)
            {
                if (this.IsInPanicMode())
                {
                    if (this.nodeThreads.IsEmpty)
                    {
                        // Start node threads
                        this.nodeThreadsStop = false;
                        this.nodeThreadsWriterConnection = null;
                        this.nodeThreadsReaderConnection = null;
                        this.nodeThreadsWriterHostSpec = null;
                        this.nodeThreadsLatestTopology = null;

                        var hosts = this.topologyMap.Get<IList<HostSpec>>(this.clusterId) ?? await this.OpenAnyConnectionAndUpdateTopologyAsync();

                        if (hosts != null && !this.isVerifiedWriterConnection)
                        {
                            foreach (var hostSpec in hosts)
                            {
                                this.nodeThreads.TryAdd(hostSpec.Host, Task.Run(() => this.RunNodeMonitoringAsync(hostSpec, this.writerHostSpec)));
                            }
                        }
                    }
                    else
                    {
                        // Check if writer is already detected
                        if (this.nodeThreadsWriterConnection != null && this.nodeThreadsWriterHostSpec != null)
                        {
                            this.CloseConnection(this.monitoringConnection);
                            this.monitoringConnection = this.nodeThreadsWriterConnection;
                            this.writerHostSpec = this.nodeThreadsWriterHostSpec;
                            this.isVerifiedWriterConnection = true;
                            this.highRefreshRateEndTime = DateTime.UtcNow.Add(HighRefreshPeriodAfterPanic);

                            if (this.ignoreNewTopologyRequestsEndTime == DateTime.MinValue)
                            {
                                this.ignoreNewTopologyRequestsEndTime = DateTime.MinValue;
                            }
                            else
                            {
                                this.ignoreNewTopologyRequestsEndTime = DateTime.UtcNow.Add(IgnoreTopologyRequestDuration);
                            }

                            this.nodeThreadsStop = true;
                            this.nodeThreads.Clear();
                            continue;
                        }
                        else
                        {
                            // Update node threads with new nodes in the topology
                            var hosts = this.nodeThreadsLatestTopology;
                            if (hosts != null && !this.nodeThreadsStop)
                            {
                                foreach (var hostSpec in hosts)
                                {
                                    this.nodeThreads.TryAdd(hostSpec.Host, Task.Run(() => this.RunNodeMonitoringAsync(hostSpec, this.writerHostSpec)));
                                }
                            }
                        }
                    }

                    await this.DelayAsync(true);
                }
                else
                {
                    // Regular mode (not panic mode)
                    if (!this.nodeThreads.IsEmpty)
                    {
                        this.nodeThreadsStop = true;
                        this.nodeThreads.Clear();
                    }

                    var hosts = await this.FetchTopologyAndUpdateCacheAsync(this.monitoringConnection);
                    if (hosts == null)
                    {
                        // Can't get topology, switch to panic mode
                        var conn = Interlocked.Exchange(ref this.monitoringConnection, null);
                        this.isVerifiedWriterConnection = false;
                        this.CloseConnection(conn);
                        continue;
                    }

                    if (this.highRefreshRateEndTime > DateTime.UtcNow)
                    {
                        this.highRefreshRateEndTime = DateTime.MinValue;
                    }

                    await this.DelayAsync(false);
                }

                if (this.ignoreNewTopologyRequestsEndTime > DateTime.UtcNow)
                {
                    this.ignoreNewTopologyRequestsEndTime = DateTime.MinValue;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
        }
        finally
        {
            var conn = Interlocked.Exchange(ref this.monitoringConnection, null);
            this.CloseConnection(conn);
        }
    }

    protected bool IsInPanicMode()
    {
        return this.monitoringConnection == null || !this.isVerifiedWriterConnection;
    }

    protected async Task RunNodeMonitoringAsync(HostSpec hostSpec, HostSpec? writerHostSpec)
    {
        DbConnection? connection = null;
        bool updateTopology = false;

        try
        {
            while (!this.nodeThreadsStop && !this.cancellationTokenSource.Token.IsCancellationRequested)
            {
                if (connection == null)
                {
                    try
                    {
                        connection = await this.pluginService.ForceConnectAsync(hostSpec, this.monitoringProperties);
                        this.pluginService.SetAvailability(hostSpec.AsAliases(), HostAvailability.Available);
                    }
                    catch
                    {
                        this.pluginService.SetAvailability(hostSpec.AsAliases(), HostAvailability.Unavailable);
                    }
                }

                if (connection != null)
                {
                    string? writerId = null;
                    try
                    {
                        writerId = await this.GetWriterNodeIdAsync(connection);
                    }
                    catch
                    {
                        this.CloseConnection(connection);
                        connection = null;
                    }

                    if (!string.IsNullOrEmpty(writerId))
                    {
                        if (Interlocked.CompareExchange(ref this.nodeThreadsWriterConnection, connection, null) == null)
                        {
                            await this.FetchTopologyAndUpdateCacheAsync(connection);
                            this.nodeThreadsWriterHostSpec = hostSpec;
                            this.nodeThreadsStop = true;
                            connection = null; // Prevent disposal
                            return;
                        }
                        else
                        {
                            this.CloseConnection(connection);
                        }
                    }
                    else if (connection != null)
                    {
                        // Reader connection
                        if (this.nodeThreadsWriterConnection == null)
                        {
                            if (updateTopology)
                            {
                                await this.ReaderThreadFetchTopologyAsync(connection, writerHostSpec);
                            }
                            else if (Interlocked.CompareExchange(ref this.nodeThreadsReaderConnection, connection, null) == null)
                            {
                                updateTopology = true;
                                await this.ReaderThreadFetchTopologyAsync(connection, writerHostSpec);
                            }
                        }
                    }
                }

                await Task.Delay(100, this.cancellationTokenSource.Token);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
        finally
        {
            this.CloseConnection(connection);
        }
    }

    protected async Task ReaderThreadFetchTopologyAsync(DbConnection connection, HostSpec? writerHostSpec)
    {
        if (connection == null)
        {
            return;
        }

        try
        {
            var hosts = await this.QueryForTopologyAsync(connection);
            if (hosts == null)
            {
                return;
            }

            this.nodeThreadsLatestTopology = hosts;

            var latestWriterHostSpec = hosts.FirstOrDefault(x => x.Role == HostRole.Writer);
            if (latestWriterHostSpec != null && writerHostSpec != null &&
                !latestWriterHostSpec.GetHostAndPort().Equals(writerHostSpec.GetHostAndPort()))
            {
                this.UpdateTopologyCache(hosts);
            }
        }
        catch
        {
            // Ignore errors
        }
    }

    protected async Task<IList<HostSpec>?> OpenAnyConnectionAndUpdateTopologyAsync()
    {
        bool writerVerifiedByThisThread = false;
        if (this.monitoringConnection == null)
        {
            try
            {
                var conn = await this.pluginService.ForceConnectAsync(this.initialHostSpec, this.monitoringProperties);
                if (Interlocked.CompareExchange(ref this.monitoringConnection, conn, null) == null)
                {
                    try
                    {
                        if (!string.IsNullOrEmpty(await this.GetWriterNodeIdAsync(this.monitoringConnection)))
                        {
                            this.isVerifiedWriterConnection = true;
                            writerVerifiedByThisThread = true;
                            this.writerHostSpec = this.initialHostSpec;
                        }
                    }
                    catch
                    {
                        // Ignore
                    }
                }
                else
                {
                    this.CloseConnection(conn);
                }
            }
            catch
            {
                return null;
            }
        }

        var hosts = await this.FetchTopologyAndUpdateCacheAsync(this.monitoringConnection);
        if (writerVerifiedByThisThread)
        {
            if (this.ignoreNewTopologyRequestsEndTime == DateTime.MinValue)
            {
                this.ignoreNewTopologyRequestsEndTime = DateTime.MinValue;
            }
            else
            {
                this.ignoreNewTopologyRequestsEndTime = DateTime.UtcNow.Add(IgnoreTopologyRequestDuration);
            }
        }

        if (hosts == null)
        {
            var connToClose = Interlocked.Exchange(ref this.monitoringConnection, null);
            this.isVerifiedWriterConnection = false;
            this.CloseConnection(connToClose);
        }

        return hosts;
    }

    protected async Task<string?> GetNodeIdAsync(DbConnection connection)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = this.nodeIdQuery;
            using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return reader.GetString(0);
            }
        }
        catch
        {
            // Ignore
        }

        return null;
    }

    protected async Task<string?> GetWriterNodeIdAsync(DbConnection connection)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = this.writerTopologyQuery;
            using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return reader.GetString(0);
            }
        }
        catch
        {
            // Ignore
        }

        return null;
    }

    protected void CloseConnection(DbConnection? connection, bool unstableConnection = false)
    {
        try
        {
            if (connection != null && connection.State != ConnectionState.Closed)
            {
                connection.Close();
            }
        }
        catch
        {
            // Ignore
        }
    }

    protected async Task DelayAsync(bool useHighRefreshRate)
    {
        if (this.highRefreshRateEndTime > DateTime.UtcNow)
        {
            useHighRefreshRate = true;
        }

        if (this.requestToUpdateTopology)
        {
            useHighRefreshRate = true;
        }

        var delay = useHighRefreshRate ? this.highRefreshRate : this.refreshRate;
        var start = DateTime.UtcNow;
        var end = start.Add(delay);

        do
        {
            await Task.Delay(50, this.cancellationTokenSource.Token);
        }
        while (!this.requestToUpdateTopology && DateTime.UtcNow < end && !this.cancellationTokenSource.Token.IsCancellationRequested);
    }

    protected async Task<IList<HostSpec>?> FetchTopologyAndUpdateCacheAsync(DbConnection? connection)
    {
        if (connection == null)
        {
            return null;
        }

        try
        {
            var hosts = await this.QueryForTopologyAsync(connection);
            if (hosts?.Count > 0)
            {
                this.UpdateTopologyCache(hosts);
            }

            return hosts;
        }
        catch
        {
            return null;
        }
    }

    protected void UpdateTopologyCache(IList<HostSpec> hosts)
    {
        lock (this.topologyUpdatedLock)
        {
            this.topologyMap.Set(this.clusterId, hosts, this.topologyCacheExpiration);
            this.requestToUpdateTopology = false;
            this.topologyUpdatedSemaphore.Release();
        }
    }

    protected async Task<IList<HostSpec>?> QueryForTopologyAsync(DbConnection connection)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandTimeout = DefaultTopologyQueryTimeoutMs / 1000;
            command.CommandText = this.topologyQuery;
            using var reader = await command.ExecuteReaderAsync();

            var hosts = new List<HostSpec>();
            var writers = new List<HostSpec>();

            while (await reader.ReadAsync())
            {
                try
                {
                    string hostName = reader.GetString(0);
                    bool isWriter = reader.GetBoolean(1);
                    double cpuUtilization = reader.GetDouble(2);
                    double nodeLag = reader.GetDouble(3);
                    DateTime lastUpdateTime;
                    try
                    {
                        lastUpdateTime = reader.GetDateTime(4);
                    }
                    catch
                    {
                        lastUpdateTime = DateTime.UtcNow;
                    }

                    long weight = (long)((Math.Round(nodeLag) * 100L) + Math.Round(cpuUtilization));
                    string endpoint = this.clusterInstanceTemplate.Host.Replace("?", hostName);
                    int port = this.clusterInstanceTemplate.IsPortSpecified
                        ? this.clusterInstanceTemplate.Port
                        : this.initialHostSpec.Port;

                    var hostSpec = this.hostListProviderService.HostSpecBuilder
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
                catch
                {
                    return null;
                }
            }

            if (writers.Count == 0)
            {
                hosts.Clear();
            }
            else if (writers.Count == 1)
            {
                hosts.Add(writers[0]);
            }
            else
            {
                // Take the latest updated writer node as the current writer
                hosts.Add(writers.OrderByDescending(x => x.LastUpdateTime).First());
            }

            return hosts;
        }
        catch
        {
            return null;
        }
    }

    public void Dispose()
    {
        lock (this.disposeLock)
        {
            if (this.disposed)
            {
                return;
            }

            this.disposed = true;
        }

        this.nodeThreadsStop = true;
        this.cancellationTokenSource.Cancel();

        try
        {
            this.monitoringTask.Wait(TimeSpan.FromSeconds(30));
        }
        catch
        {
            // Ignore
        }

        var conn = Interlocked.Exchange(ref this.monitoringConnection, null);
        this.CloseConnection(conn);

        this.cancellationTokenSource.Dispose();
        this.topologyUpdatedSemaphore.Dispose();
    }
}
