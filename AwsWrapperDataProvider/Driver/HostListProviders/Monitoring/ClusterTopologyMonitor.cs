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
using Amazon.Auth.AccessControlPolicy;
using AwsWrapperDataProvider.Driver.Exceptions;
using AwsWrapperDataProvider.Driver.HostInfo;
using AwsWrapperDataProvider.Driver.Utils;
using AwsWrapperDataProvider.Properties;
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
        try
        {
            Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_StartMonitoringThread,
                this.initialHostSpec.Host));

            while (!this.cancellationTokenSource.Token.IsCancellationRequested)
            {
                if (this.IsInPanicMode())
                {
                    if (this.nodeThreads.IsEmpty)
                    {
                        Logger.LogTrace(Resources.ClusterTopologyMonitor_StartingNodeMonitoringThreads);

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
                                    Task.Run(() =>
                                        nodeMonitoringTask.RunNodeMonitoringAsync()));
                            }
                        }
                    }
                    else
                    {
                        // Check if writer is already detected
                        if (this.nodeThreadsWriterConnection != null && this.nodeThreadsWriterHostSpec != null)
                        {
                            Logger.LogTrace(string.Format(
                                Resources.ClusterTopologyMonitor_WriterPickedUpFromNodeMonitors,
                                this.nodeThreadsWriterHostSpec.Host));

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
                                    Task.Run(() =>
                                        nodeMonitoringTask.RunNodeMonitoringAsync()));
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

                    if (this.highRefreshRateEndTime > DateTime.MinValue && this.highRefreshRateEndTime < DateTime.UtcNow)
                    {
                        this.highRefreshRateEndTime = DateTime.MinValue;
                    }

                    if (this.highRefreshRateEndTime == DateTime.MinValue)
                    {
                        Logger.LogTrace("Topology query completed. Found {HostCount} hosts: {Hosts}",
                            hosts.Count,
                            string.Join(", ", hosts.Select(h => h.HostId + ": " + h.Role)));
                    }

                    await this.DelayAsync(false);
                }

                if (this.ignoreNewTopologyRequestsEndTime > DateTime.MinValue && this.ignoreNewTopologyRequestsEndTime < DateTime.UtcNow)
                {
                    this.ignoreNewTopologyRequestsEndTime = DateTime.MinValue;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Suppress Error.
        }
        catch (Exception ex)
        {
            Logger.Log(
                LogLevel.Trace,
                string.Format(Resources.ClusterTopologyMonitor_ExceptionDuringMonitoringStop, this.initialHostSpec.Host),
                ex);
        }
        finally
        {
            var conn = Interlocked.Exchange(ref this.monitoringConnection, null);
            this.CloseConnection(conn);

            Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_StopMonitoringThread, this.initialHostSpec.Host));
        }
    }

    public async Task<IList<HostSpec>> ForceRefreshAsync(bool shouldVerifyWriter, long timeoutMs)
    {
        if (this.ignoreNewTopologyRequestsEndTime > DateTime.UtcNow)
        {
            // Previous failover has just completed. We can use results of it without triggering a new topology update.
            if (this.topologyMap.TryGetValue(this.clusterId, out IList<HostSpec>? currentHosts) && currentHosts != null)
            {
                Logger.LogTrace(Resources.ClusterTopologyMonitor_IgnoringTopologyRequest);
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

        lock (this.topologyUpdatedLock)
        {
            this.requestToUpdateTopology = true;
        }

        if (timeoutMs == 0)
        {
            Logger.LogTrace(Resources.ClusterTopologyMonitor_TimeoutSetToZero);
            return currentHosts ?? new List<HostSpec>();
        }

        TimeSpan timeout = TimeSpan.FromMilliseconds(timeoutMs);
        CancellationToken cancellationToken = new CancellationTokenSource(timeout).Token;

        // TODO: FIX THIS LOL
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await this.topologyUpdatedSemaphore.WaitAsync(1000, cancellationToken);

                if (this.topologyMap.TryGetValue(this.clusterId, out IList<HostSpec>? latestHosts) &&
                    !ReferenceEquals(currentHosts, latestHosts))
                {
                    return latestHosts!;
                }
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogDebug(Resources.ClusterTopologyMonitor_Interrupted);
            throw;
        }

        throw new TimeoutException(string.Format(Resources.ClusterTopologyMonitor_TopologyNotUpdated, timeout));
    }

    protected bool IsInPanicMode()
    {
        return this.monitoringConnection == null || !this.isVerifiedWriterConnection;
    }

    protected async Task<IList<HostSpec>?> OpenAnyConnectionAndUpdateTopologyAsync()
    {
        bool writerVerifiedByThisThread = false;
        if (this.monitoringConnection == null)
        {
            DbConnection? newConnection = null;

            try
            {
                newConnection = this.pluginService.ForceOpenConnection(this.initialHostSpec, this.properties, false);

                if (Interlocked.CompareExchange(ref this.monitoringConnection, newConnection, null) == null)
                {
                    Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_OpenedMonitoringConnection, this.initialHostSpec.Host));

                    if (!string.IsNullOrEmpty(await this.GetWriterNodeIdAsync(this.monitoringConnection)))
                    {
                        this.isVerifiedWriterConnection = true;
                        writerVerifiedByThisThread = true;
                        if (RdsUtils.IsRdsInstance(this.initialHostSpec.Host))
                        {
                            this.writerHostSpec = this.initialHostSpec;
                            Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_WriterMonitoringConnection));
                        }
                        else
                        {
                            string? nodeId = await this.GetNodeIdAsync(this.monitoringConnection);
                            if (!string.IsNullOrEmpty(nodeId))
                            {
                                this.writerHostSpec = this.CreateHost(nodeId, true, 0, null);
                                Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_WriterMonitoringConnection, this.writerHostSpec.Host));
                            }
                        }
                    }
                }

                // Don't close this connection - it's now the monitoring connection
                newConnection = null;
            }
            catch (DbException)
            {
                // Suppress connection errors and continue
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

        IList<HostSpec>? hosts = await this.FetchTopologyAndUpdateCacheAsync(this.monitoringConnection);
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
        catch (DbException ex)
        {
            Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_ErrorFetchingTopology, ex));
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
                    DateTime lastUpdateTime = reader.IsDBNull(4)
                        ? DateTime.UtcNow
                        : reader.GetDateTime(4);

                    long weight = (long)((Math.Round(nodeLag) * 100L) + Math.Round(cpuUtilization));

                    var hostSpec = this.CreateHost(hostName, isWriter, weight, lastUpdateTime);
                    hostSpec.AddAlias(hostName);

                    (isWriter ? writers : hosts).Add(hostSpec);
                }
                catch (Exception ex)
                {
                    Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_ErrorProcessingQueryResults, ex));
                    return null;
                }
            }

            if (writers.Count == 0)
            {
                Logger.LogWarning(Resources.ClusterTopologyMonitor_InvalidTopology);
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

    private class NodeMonitoringTask(
        ClusterTopologyMonitor monitor,
        HostSpec hostSpec,
        HostSpec? writerHostSpec)
    {
        private static readonly ILogger<NodeMonitoringTask> Logger = LoggerUtils.GetLogger<NodeMonitoringTask>();

        public async Task RunNodeMonitoringAsync()
        {
            DbConnection? connection = null;
            bool updateTopology = false;

            try
            {
                while (!monitor.nodeThreadsStop && !monitor.cancellationTokenSource.Token.IsCancellationRequested)
                {
                    if (connection == null)
                    {
                        try
                        {
                            connection = monitor.pluginService.ForceOpenConnection(hostSpec, monitor.properties, false);
                            monitor.pluginService.SetAvailability(hostSpec.AsAliases(), HostAvailability.Available);
                        }
                        catch
                        {
                            monitor.pluginService.SetAvailability(hostSpec.AsAliases(), HostAvailability.Unavailable);
                            connection = null;
                        }
                    }

                    if (connection != null)
                    {
                        string? writerId = null;
                        try
                        {
                            writerId = await monitor.GetWriterNodeIdAsync(connection);
                        }
                        catch (DbException)
                        {
                            monitor.CloseConnection(connection);
                            connection = null;
                        }

                        if (!string.IsNullOrEmpty(writerId))
                        {
                            if (Interlocked.CompareExchange(ref monitor.nodeThreadsWriterConnection, connection, null) == null)
                            {
                                Logger.LogInformation(string.Format(Resources.NodeMonitoringTask_DetectedWriter, writerId));
                                await monitor.FetchTopologyAndUpdateCacheAsync(connection);
                                monitor.nodeThreadsWriterHostSpec = hostSpec;
                                monitor.nodeThreadsStop = true;
                            }
                            else
                            {
                                monitor.CloseConnection(connection);
                            }

                            connection = null; // Prevent disposal
                            return;
                        }
                        else if (connection != null)
                        {
                            // Reader connection
                            if (monitor.nodeThreadsWriterConnection == null)
                            {
                                if (updateTopology)
                                {
                                    await this.ReaderThreadFetchTopologyAsync(connection);
                                }
                                else if (Interlocked.CompareExchange(ref monitor.nodeThreadsReaderConnection, connection, null) == null)
                                {
                                    updateTopology = true;
                                    await this.ReaderThreadFetchTopologyAsync(connection);
                                }
                            }
                        }
                    }

                    await Task.Delay(100, monitor.cancellationTokenSource.Token);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
            finally
            {
                monitor.CloseConnection(connection);
                Logger.LogTrace(Resources.NodeMonitoringTask_ThreadCompleted);
            }
        }

        protected async Task ReaderThreadFetchTopologyAsync(DbConnection connection)
        {
            try
            {
                var hosts = await monitor.QueryForTopologyAsync(connection);
                if (hosts == null)
                {
                    return;
                }

                monitor.nodeThreadsLatestTopology = hosts;

                var latestWriterHostSpec = hosts.FirstOrDefault(x => x.Role == HostRole.Writer);
                if (latestWriterHostSpec != null && writerHostSpec != null &&
                    !latestWriterHostSpec.GetHostAndPort().Equals(writerHostSpec.GetHostAndPort()))
                {
                    Logger.LogTrace(string.Format(
                        Resources.NodeMonitoringTask_WriterNodeChanged,
                        writerHostSpec.Host,
                        latestWriterHostSpec.Host));

                    monitor.UpdateTopologyCache(hosts);

                    Logger.LogTrace("Topology Cache Updated. Found {HostCount} hosts: {Hosts}",
                        hosts.Count,
                        string.Join(", ", hosts.Select(h => h.HostId + ": " + h.Role)));
                }
            }
            catch
            {
                // Ignore errors
            }
        }
    }
}
