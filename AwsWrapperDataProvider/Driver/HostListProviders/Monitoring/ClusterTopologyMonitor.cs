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
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace AwsWrapperDataProvider.Driver.HostListProviders.Monitoring;

/// <summary>
/// Implementation of cluster topology monitoring with background refresh capabilities.
/// </summary>
public class ClusterTopologyMonitor : IClusterTopologyMonitor
{
    private static readonly ILogger<ClusterTopologyMonitor> Logger = LoggerUtils.GetLogger<ClusterTopologyMonitor>();
    protected const int DefaultTopologyQueryTimeoutMs = 1000;

    // Keep monitoring topology with a high rate for 30s after failover
    protected static readonly TimeSpan HighRefreshPeriodAfterPanic = TimeSpan.FromSeconds(30);
    protected static readonly TimeSpan IgnoreTopologyRequestDuration = TimeSpan.FromSeconds(10);

    protected readonly TimeSpan refreshRate;
    protected readonly TimeSpan highRefreshRate;
    protected readonly TimeSpan topologyCacheExpiration;
    protected readonly Dictionary<string, string> properties;
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
    protected bool isVerifiedWriterConnection;
    protected DateTime highRefreshRateEndTime = DateTime.MinValue;
    protected volatile bool requestToUpdateTopology;
    protected DateTime ignoreNewTopologyRequestsEndTime = DateTime.MinValue;
    protected volatile bool nodeThreadsStop;
    protected DbConnection? nodeThreadsWriterConnection;
    protected HostSpec? nodeThreadsWriterHostSpec;
    protected DbConnection? nodeThreadsReaderConnection;
    protected IList<HostSpec>? nodeThreadsLatestTopology;
    protected bool disposed;

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
        this.refreshRate = refreshRate;
        this.highRefreshRate = highRefreshRate;
        this.topologyCacheExpiration = topologyCacheExpiration;
        this.topologyQuery = topologyQuery;
        this.writerTopologyQuery = writerTopologyQuery;
        this.nodeIdQuery = nodeIdQuery;

        // Use original properties directly
        this.properties = properties;

        this.monitoringTask = Task.Run(this.RunMonitoringLoop, this.cancellationTokenSource.Token);
    }

    public bool CanDispose => true;

    public void SetClusterId(string clusterId)
    {
        this.clusterId = clusterId;
    }

    public async Task RunMonitoringLoop()
    {
        Logger.LogInformation("Starting monitoring loop for cluster: {ClusterId}", this.clusterId);

        try
        {
            while (!this.cancellationTokenSource.Token.IsCancellationRequested)
            {
                if (this.IsInPanicMode())
                {
                    Logger.LogWarning("Cluster {ClusterId} is in panic mode. Attempting to recover...", this.clusterId);

                    if (this.nodeThreads.IsEmpty)
                    {
                        Logger.LogInformation("No active node threads for cluster {ClusterId}. Starting node threads...", this.clusterId);

                        // Start node threads
                        this.nodeThreadsStop = false;
                        this.nodeThreadsWriterConnection = null;
                        this.nodeThreadsReaderConnection = null;
                        this.nodeThreadsWriterHostSpec = null;
                        this.nodeThreadsLatestTopology = null;

                        var hosts = this.topologyMap.Get<IList<HostSpec>>(this.clusterId) ??
                                    await this.OpenAnyConnectionAndUpdateTopologyAsync();

                        if (hosts != null && !this.isVerifiedWriterConnection)
                        {
                            foreach (var hostSpec in hosts)
                            {
                                NodeMonitoringTask nodeMonitoringTask = new(this, hostSpec, this.writerHostSpec);
                                this.nodeThreads.TryAdd(hostSpec.Host,
                                    Task.Run(() => nodeMonitoringTask.RunNodeMonitoringAsync(hostSpec, this.writerHostSpec)));
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
                                this.ignoreNewTopologyRequestsEndTime =
                                    DateTime.UtcNow.Add(IgnoreTopologyRequestDuration);
                            }

                            this.nodeThreadsStop = true;
                            this.nodeThreads.Clear();
                            continue;
                        }

                        // Update node threads with new nodes in the topology
                        var hosts = this.nodeThreadsLatestTopology;
                        if (hosts != null && !this.nodeThreadsStop)
                        {
                            foreach (var hostSpec in hosts)
                            {
                                NodeMonitoringTask nodeMonitoringTask = new(this, hostSpec, this.writerHostSpec);
                                this.nodeThreads.TryAdd(hostSpec.Host,
                                    Task.Run(() => nodeMonitoringTask.RunNodeMonitoringAsync(hostSpec, this.writerHostSpec)));
                            }
                        }
                    }

                    await this.DelayAsync(true);
                }
                else
                {
                    Logger.LogInformation("Cluster {ClusterId} is in regular mode. Fetching topology...", this.clusterId);

                    // Regular mode (not panic mode)
                    if (!this.nodeThreads.IsEmpty)
                    {
                        this.nodeThreadsStop = true;
                        this.nodeThreads.Clear();
                    }

                    var hosts = await this.FetchTopologyAndUpdateCacheAsync(this.monitoringConnection);
                    if (hosts == null)
                    {
                        Logger.LogError("Failed to fetch topology for cluster {ClusterId}. Switching to panic mode.", this.clusterId);

                        // Can't get topology, switch to panic mode
                        var conn = Interlocked.Exchange(ref this.monitoringConnection, null);
                        this.isVerifiedWriterConnection = false;
                        this.CloseConnection(conn);
                        continue;
                    }

                    if (this.highRefreshRateEndTime > DateTime.MinValue && this.highRefreshRateEndTime < DateTime.UtcNow)
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
            Logger.LogInformation("Monitoring loop for cluster {ClusterId} was canceled.", this.clusterId);
        }
        finally
        {
            Logger.LogInformation("Cleaning up resources for cluster {ClusterId}.", this.clusterId);
            var conn = Interlocked.Exchange(ref this.monitoringConnection, null);
            this.CloseConnection(conn);
        }
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
            this.CloseConnection(connectionToClose);
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

        if (currentHosts != null)
        {
            Logger.LogDebug("Current topology for cluster {ClusterId}: {CurrentHosts}",
                this.clusterId,
                string.Join(", ", currentHosts.Select(h => h.Host)));
        }

        lock (this.topologyUpdatedLock)
        {
            this.requestToUpdateTopology = true;
        }

        if (timeoutMs == 0)
        {
            return currentHosts ?? new List<HostSpec>();
        }

        TimeSpan timeout = TimeSpan.FromMilliseconds(timeoutMs);
        CancellationToken cancellationToken = new CancellationTokenSource(timeout).Token;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await this.topologyUpdatedSemaphore.WaitAsync(1000, cancellationToken);

                if (this.topologyMap.TryGetValue(this.clusterId, out IList<HostSpec>? latestHosts) &&
                    !ReferenceEquals(currentHosts, latestHosts))
                {
                    Logger.LogInformation("Topology updated for cluster {ClusterId}. New topology: {NewHosts}",
                        this.clusterId,
                        string.Join(", ", latestHosts!.Select(h => h.HostId + ": " + h.Role)));
                    return latestHosts!;
                }
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogWarning("Timeout occurred while waiting for topology update for cluster {ClusterId}.", this.clusterId);
            throw new TimeoutException($"Topology was not updated within {timeoutMs} milliseconds");
        }

        Logger.LogError("Unable to update topology for cluster {ClusterId}.", this.clusterId);
        throw new Exception($"Unable to update Topology: {this.clusterId}");
    }

    protected bool IsInPanicMode()
    {
        return this.monitoringConnection == null || !this.isVerifiedWriterConnection;
    }

    protected async Task<IList<HostSpec>?> OpenAnyConnectionAndUpdateTopologyAsync()
    {
        Logger.LogInformation("Attempting to open a connection and update topology for cluster {ClusterId}.", this.clusterId);

        bool writerVerifiedByThisThread = false;
        if (this.monitoringConnection == null)
        {
            DbConnection? newConnection = null;
            try
            {
                newConnection = this.pluginService.ForceOpenConnection(this.initialHostSpec, this.properties, false);
                if (Interlocked.CompareExchange(ref this.monitoringConnection, newConnection, null) == null)
                {
                    // SUCCESS: This thread won the race
                    try
                    {
                        if (!string.IsNullOrEmpty(await this.GetWriterNodeIdAsync(this.monitoringConnection)))
                        {
                            this.isVerifiedWriterConnection = true;
                            writerVerifiedByThisThread = true;
                            if (RdsUtils.IsRdsInstance(this.initialHostSpec.Host))
                            {
                                this.writerHostSpec = this.initialHostSpec;
                            }
                            else
                            {
                                string? nodeId = await this.GetNodeIdAsync(this.monitoringConnection);
                                if (!string.IsNullOrEmpty(nodeId))
                                {
                                    this.writerHostSpec = this.CreateHost(nodeId, true, 0, null);
                                }
                            }
                        }
                    }
                    catch
                    {
                        // Ignore
                    }

                    // Don't close this connection - it's now the monitoring connection
                    newConnection = null;
                }
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Failed to open initial connection for cluster {ClusterId}.", this.clusterId);
            }
            finally
            {
                // Clean up if this thread lost the race or if there was an exception
                if (newConnection != null)
                {
                    try
                    {
                        newConnection.Close();
                    }
                    catch
                    {
                        // Ignore cleanup errors
                    }
                }
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
            Logger.LogWarning("Failed to fetch topology for cluster {ClusterId}. Closing connection.", this.clusterId);
            var connToClose = Interlocked.Exchange(ref this.monitoringConnection, null);
            this.isVerifiedWriterConnection = false;
            this.CloseConnection(connToClose);
        }

        return hosts;
    }

    protected HostSpec CreateHost(string nodeId, bool isWriter, long weight, DateTime? lastUpdateTime)
    {
        string endpoint = this.clusterInstanceTemplate.Host.Replace("?", nodeId);
        int port = this.clusterInstanceTemplate.IsPortSpecified
            ? this.clusterInstanceTemplate.Port
            : this.initialHostSpec.Port;

        HostSpec host = this.hostListProviderService.HostSpecBuilder
            .WithHost(endpoint)
            .WithHostId(nodeId)
            .WithPort(port)
            .WithRole(isWriter ? HostRole.Writer : HostRole.Reader)
            .WithAvailability(HostAvailability.Available)
            .WithWeight(weight)
            .WithLastUpdateTime(lastUpdateTime)
            .Build();

        host.AddAlias(nodeId);
        return host;
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

    protected void CloseConnection(DbConnection? connection) => connection?.Close();

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

                    var hostSpec = this.CreateHost(hostName, isWriter, weight, lastUpdateTime);
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

            Logger.LogInformation("Topology query completed for cluster {ClusterId}. Found {HostCount} hosts: {Hosts}",
                this.clusterId,
                hosts.Count,
                string.Join(", ", hosts.Select(h => h.HostId + ": " + h.Role)));
            return hosts;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error occurred while querying topology for cluster {ClusterId}.", this.clusterId);
            return null;
        }
    }

    public void Dispose()
    {
        Logger.LogInformation("Disposing resources for cluster {ClusterId}.", this.clusterId);

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

        Logger.LogInformation("Resources disposed for cluster {ClusterId}.", this.clusterId);
    }

    private class NodeMonitoringTask(
        ClusterTopologyMonitor monitor,
        HostSpec hostSpec,
        HostSpec? writerHostSpec)
    {
        private readonly ClusterTopologyMonitor monitor = monitor;
        private readonly HostSpec hostSpec = hostSpec;
        private readonly HostSpec? writerHostSpec = writerHostSpec;

        public async Task RunNodeMonitoringAsync(HostSpec hostSpec, HostSpec? writerHostSpec)
        {
            DbConnection? connection = null;
            bool updateTopology = false;

            try
            {
                while (!this.monitor.nodeThreadsStop && !this.monitor.cancellationTokenSource.Token.IsCancellationRequested)
                {
                    if (connection == null)
                    {
                        try
                        {
                            connection = this.monitor.pluginService.ForceOpenConnection(hostSpec, this.monitor.properties, false);
                            this.monitor.pluginService.SetAvailability(hostSpec.AsAliases(), HostAvailability.Available);
                        }
                        catch
                        {
                            this.monitor.pluginService.SetAvailability(hostSpec.AsAliases(), HostAvailability.Unavailable);
                            connection = null;
                        }
                    }

                    if (connection != null)
                    {
                        string? writerId = null;
                        try
                        {
                            writerId = await this.monitor.GetWriterNodeIdAsync(connection);
                        }
                        catch
                        {
                            this.monitor.CloseConnection(connection);
                            connection = null;
                        }

                        if (!string.IsNullOrEmpty(writerId))
                        {
                            if (Interlocked.CompareExchange(ref this.monitor.nodeThreadsWriterConnection, connection, null) == null)
                            {
                                await this.monitor.FetchTopologyAndUpdateCacheAsync(connection);
                                this.monitor.nodeThreadsWriterHostSpec = hostSpec;
                                this.monitor.nodeThreadsStop = true;
                            }
                            else
                            {
                                this.monitor.CloseConnection(connection);
                            }

                            connection = null; // Prevent disposal
                            return;
                        }
                        else if (connection != null)
                        {
                            // Reader connection
                            if (this.monitor.nodeThreadsWriterConnection == null)
                            {
                                if (updateTopology)
                                {
                                    await this.ReaderThreadFetchTopologyAsync(connection, writerHostSpec);
                                }
                                else if (Interlocked.CompareExchange(ref this.monitor.nodeThreadsReaderConnection, connection, null) == null)
                                {
                                    updateTopology = true;
                                    await this.ReaderThreadFetchTopologyAsync(connection, writerHostSpec);
                                }
                            }
                        }
                    }

                    await Task.Delay(100, this.monitor.cancellationTokenSource.Token);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
            finally
            {
                this.monitor.CloseConnection(connection);
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
                var hosts = await this.monitor.QueryForTopologyAsync(connection);
                if (hosts == null)
                {
                    return;
                }

                this.monitor.nodeThreadsLatestTopology = hosts;

                var latestWriterHostSpec = hosts.FirstOrDefault(x => x.Role == HostRole.Writer);
                if (latestWriterHostSpec != null && writerHostSpec != null &&
                    !latestWriterHostSpec.GetHostAndPort().Equals(writerHostSpec.GetHostAndPort()))
                {
                    this.monitor.UpdateTopologyCache(hosts);
                }
            }
            catch
            {
                // Ignore errors
            }
        }
    }
}
