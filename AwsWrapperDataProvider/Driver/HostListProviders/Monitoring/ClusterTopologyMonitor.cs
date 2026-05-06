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
using System.Globalization;
using System.Runtime.CompilerServices;
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
    protected const int DefaultTopologyQueryTimeoutSec = 1;
    protected const int DefaultConnectTimeout = 5;
    protected const int DefaultCommandTimeout = 5;

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
    protected readonly string nodeIdQuery;
    protected readonly IHostListProviderService hostListProviderService;
    protected readonly HostSpec clusterInstanceTemplate;
    protected readonly CancellationTokenSource ctsTopologyMonitoring = new();
    protected readonly object topologyUpdatedLock = new();
    protected readonly ConcurrentDictionary<string, Lazy<Task>> nodeThreads = new();
    protected readonly Task monitoringTask;
    protected readonly object disposeLock = new();
    protected readonly SemaphoreSlim monitoringConnectionSemaphore = new(1, 1);
    protected readonly TopologyUtils topologyUtils;

    protected CancellationTokenSource ctsNodeMonitoring;
    protected string clusterId;
    protected HostSpec? writerHostSpec;
    protected DbConnection? monitoringConnection;
    protected bool isVerifiedWriterConnection;
    protected DateTime highRefreshRateEndTime = DateTime.MinValue;
    protected long ignoreNewTopologyRequestsEndTime = -1;
    protected DbConnection? nodeThreadsWriterConnection;
    protected HostSpec? nodeThreadsWriterHostSpec;
    protected DbConnection? nodeThreadsReaderConnection;
    protected volatile IList<HostSpec>? nodeThreadsLatestTopology;
    protected bool disposed = false;

    // Stable reader topology tracking
    protected readonly ConcurrentDictionary<string, IList<HostSpec>> readerTopologiesById = new();
    protected readonly ConcurrentDictionary<string, bool> completedOneCycle = new();
    protected static readonly TimeSpan StableTopologiesDuration = TimeSpan.FromSeconds(15);
    protected long stableTopologiesStartTicks = 0;

    private int requestToUpdateTopology = 0;
    private int nodeThreadsStop = 0;

    protected bool RequestToUpdateTopology
    {
        get => Volatile.Read(ref this.requestToUpdateTopology) == 1;
        set => Interlocked.Exchange(ref this.requestToUpdateTopology, value ? 1 : 0);
    }

    protected bool NodeThreadsStop
    {
        get => Volatile.Read(ref this.nodeThreadsStop) == 1;
        set => Interlocked.Exchange(ref this.nodeThreadsStop, value ? 1 : 0);
    }

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
        string nodeIdQuery,
        TopologyUtils topologyUtils)
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
        this.nodeIdQuery = nodeIdQuery;
        this.topologyUtils = topologyUtils;

        Dictionary<string, string> monitoringConnProperties = new(properties);

        foreach (string key in properties.Keys)
        {
            if (key.StartsWith(PropertyDefinition.ClusterTopologyMonitoringPropertyPrefix))
            {
                monitoringConnProperties[key[PropertyDefinition.ClusterTopologyMonitoringPropertyPrefix.Length..]] =
                    properties[key];
                monitoringConnProperties.Remove(key);
            }
        }

        this.properties = monitoringConnProperties;

        // Ensure monitoring connection has connect and command timeout defaults
        this.pluginService.TargetConnectionDialect.EnsureMonitoringTimeouts(this.properties, DefaultConnectTimeout, DefaultCommandTimeout);

        // Make sure node monitoring tasks are cancelled once topology monitoring task is cancelled
        this.ctsNodeMonitoring = CancellationTokenSource.CreateLinkedTokenSource(this.ctsTopologyMonitoring.Token);

        this.monitoringTask = Task.Run(this.RunMonitoringLoop, this.ctsTopologyMonitoring.Token);
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
            Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_StartMonitoringThread, this.initialHostSpec.Host));

            while (!this.ctsTopologyMonitoring.Token.IsCancellationRequested)
            {
                if (this.IsInPanicMode())
                {
                    if (this.nodeThreads.IsEmpty)
                    {
                        Logger.LogTrace(Resources.ClusterTopologyMonitor_StartingNodeMonitoringThreads);

                        // Start node threads
                        this.NodeThreadsStop = false;
                        this.nodeThreadsWriterConnection = null;
                        this.nodeThreadsReaderConnection = null;
                        this.nodeThreadsWriterHostSpec = null;
                        this.nodeThreadsLatestTopology = null;

                        var hosts = this.topologyMap.Get<IList<HostSpec>>(this.clusterId) ??
                                    await this.OpenAnyConnectionAndUpdateTopologyAsync();

                        this.ctsNodeMonitoring.Cancel();
                        Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_RestartCancellationToken);
                        this.ctsNodeMonitoring = CancellationTokenSource.CreateLinkedTokenSource(this.ctsTopologyMonitoring.Token);

                        if (hosts != null && !this.isVerifiedWriterConnection)
                        {
                            foreach (var hostSpec in hosts)
                            {
                                NodeMonitoringTask nodeMonitoringTask = new(this, hostSpec, this.writerHostSpec);

                                // Run new task only if not existed
                                var lazyTask = this.nodeThreads.GetOrAdd(
                                    hostSpec.Host,
                                    _ => new Lazy<Task>(
                                        () =>
                                        {
                                            var task = Task.Run(() => nodeMonitoringTask.RunNodeMonitoringAsync(this.ctsNodeMonitoring.Token), this.ctsNodeMonitoring.Token);
                                            Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_MonitoringTask, RuntimeHelpers.GetHashCode(nodeMonitoringTask), hostSpec);
                                            return task;
                                        },
                                        LazyThreadSafetyMode.ExecutionAndPublication));
                                _ = lazyTask.Value;
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

                            await this.DisposeConnectionAsync(this.monitoringConnection);
                            Interlocked.Exchange(ref this.monitoringConnection, this.nodeThreadsWriterConnection);
                            this.writerHostSpec = this.nodeThreadsWriterHostSpec;
                            this.isVerifiedWriterConnection = true;
                            this.highRefreshRateEndTime = DateTime.UtcNow.Add(HighRefreshPeriodAfterPanic);

                            // We verify the writer on initial connection and on failover, but we only want to ignore new topology
                            // requests after failover. To accomplish this, the first time we verify the writer we set the ignore end
                            // time to 0. Any future writer verifications will set it to a positive value.
                            if (Interlocked.CompareExchange(ref this.ignoreNewTopologyRequestsEndTime, 0, -1) != -1)
                            {
                                Interlocked.Exchange(ref this.ignoreNewTopologyRequestsEndTime, DateTime.UtcNow.Add(IgnoreTopologyRequestDuration).Ticks);
                                Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_MonitoringTime, new DateTime(this.ignoreNewTopologyRequestsEndTime, DateTimeKind.Utc));
                            }

                            this.NodeThreadsStop = true;
                            this.ctsNodeMonitoring.Cancel();
                            Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_Clearing);
                            this.nodeThreads.Clear();
                            this.stableTopologiesStartTicks = 0;
                            this.readerTopologiesById.Clear();
                            this.completedOneCycle.Clear();
                            continue;
                        }

                        // Update node threads with new nodes in the topology
                        var hosts = this.nodeThreadsLatestTopology;
                        if (hosts != null && !this.NodeThreadsStop)
                        {
                            foreach (var hostSpec in hosts)
                            {
                                NodeMonitoringTask nodeMonitoringTask = new(this, hostSpec, this.writerHostSpec);

                                // Run new task only if not existed
                                var lazyTask = this.nodeThreads.GetOrAdd(
                                    hostSpec.Host,
                                    _ => new Lazy<Task>(
                                        () =>
                                        {
                                            var task = Task.Run(() => nodeMonitoringTask.RunNodeMonitoringAsync(this.ctsNodeMonitoring.Token), this.ctsNodeMonitoring.Token);
                                            Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_MonitoringTask, RuntimeHelpers.GetHashCode(task), hostSpec);
                                            return task;
                                        },
                                        LazyThreadSafetyMode.ExecutionAndPublication));
                                _ = lazyTask.Value;
                            }
                        }
                    }

                    this.CheckForStableReaderTopologies();
                    await this.DelayAsync(true);
                }
                else
                {
                    Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_RegularMode);

                    // Regular mode (not panic mode)
                    if (!this.nodeThreads.IsEmpty)
                    {
                        this.ctsNodeMonitoring.Cancel();
                        Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_Clearing);
                        this.nodeThreads.Clear();
                        this.stableTopologiesStartTicks = 0;
                        this.readerTopologiesById.Clear();
                        this.completedOneCycle.Clear();
                    }

                    var hosts = await this.FetchTopologyAndUpdateCacheAsync(this.monitoringConnection);
                    if (hosts == null)
                    {
                        // Can't get topology, switch to panic mode
                        Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_NullConnection, RuntimeHelpers.GetHashCode(this.monitoringConnection));
                        var conn = Interlocked.Exchange(ref this.monitoringConnection, null);

                        this.isVerifiedWriterConnection = false;
                        await this.DisposeConnectionAsync(conn);
                        continue;
                    }

                    if (this.highRefreshRateEndTime > DateTime.MinValue && this.highRefreshRateEndTime < DateTime.UtcNow)
                    {
                        this.highRefreshRateEndTime = DateTime.MinValue;
                    }

                    if (this.highRefreshRateEndTime == DateTime.MinValue)
                    {
                        Logger.LogTrace(LoggerUtils.LogTopology(hosts, string.Format(Resources.ClusterTopologyMonitor_RunMonitoringLoop_FoundHosts, hosts.Count)));
                    }

                    await this.DelayAsync(false);
                }

                long endTime = Interlocked.Read(ref this.ignoreNewTopologyRequestsEndTime);
                if (endTime > 0 && endTime < DateTime.UtcNow.Ticks)
                {
                    Interlocked.Exchange(ref this.ignoreNewTopologyRequestsEndTime, 0);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Suppress Error.
        }
        catch (Exception ex)
        {
            Logger.LogTrace(ex, string.Format(Resources.ClusterTopologyMonitor_ExceptionDuringMonitoringStop, this.initialHostSpec.Host));
        }
        finally
        {
            Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_CancellingMonitoringNodes, RuntimeHelpers.GetHashCode(this.monitoringConnection));
            this.ctsNodeMonitoring.Cancel();

            try
            {
                var tasks = this.nodeThreads.Values.Select(lz => lz.Value).ToArray();
                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, Resources.ClusterTopologyMonitor_RunMonitoringLoop_NodeFailedShutdown);
            }

            Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_NullConnection, RuntimeHelpers.GetHashCode(this.monitoringConnection));

            var conn = Interlocked.Exchange(ref this.monitoringConnection, null);
            await this.DisposeConnectionAsync(conn);

            Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_StopMonitoringThread, this.initialHostSpec.Host));
        }
    }

    public async Task<IList<HostSpec>> ForceRefreshAsync(bool shouldVerifyWriter, long timeoutMs)
    {
        if (Interlocked.Read(ref this.ignoreNewTopologyRequestsEndTime) > DateTime.UtcNow.Ticks)
        {
            // Previous failover has just completed. We can use results of it without triggering a new topology update.
            if (this.topologyMap.TryGetValue(this.clusterId, out IList<HostSpec>? currentHosts) && currentHosts != null)
            {
                Logger.LogTrace(LoggerUtils.LogTopology(currentHosts, Resources.ClusterTopologyMonitor_IgnoringTopologyRequest));
                return currentHosts;
            }
        }

        if (shouldVerifyWriter)
        {
            Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_NullConnection, RuntimeHelpers.GetHashCode(this.monitoringConnection));
            var connectionToClose = Interlocked.Exchange(ref this.monitoringConnection, null);
            this.isVerifiedWriterConnection = false;
            await this.DisposeConnectionAsync(connectionToClose);
        }

        return this.WaitTillTopologyGetsUpdated(timeoutMs);
    }

    protected IList<HostSpec> WaitTillTopologyGetsUpdated(long timeoutMs)
    {
        this.topologyMap.TryGetValue(this.clusterId, out IList<HostSpec>? currentHosts);
        Logger.LogTrace(LoggerUtils.LogTopology(currentHosts, Resources.ClusterTopologyMonitor_RunMonitoringLoop_CurrentTopology));

        lock (this.topologyUpdatedLock)
        {
            this.RequestToUpdateTopology = true;
        }

        if (timeoutMs == 0)
        {
            Logger.LogTrace(LoggerUtils.LogTopology(currentHosts, Resources.ClusterTopologyMonitor_TimeoutSetToZero));
            return currentHosts ?? [];
        }

        DateTime endTime = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        Logger.LogTrace(Resources.ClusterTopologyMonitor_WaitTillTopologyGetsUpdated_EndTime, timeoutMs, endTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));
        IList<HostSpec>? latestHosts = [];
        lock (this.topologyUpdatedLock)
        {
            while (DateTime.UtcNow < endTime)
            {
                // Break only if the cache has a new topology (different reference).
                // If the cache entry expired (TryGetValue returns false), keep waiting.
                if (this.topologyMap.TryGetValue(this.clusterId, out latestHosts)
                    && !ReferenceEquals(currentHosts, latestHosts))
                {
                    break;
                }

                Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_WaitingUpdatedLock);
                if (Monitor.Wait(this.topologyUpdatedLock, 1000))
                {
                    Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_WokeUpMonitorWaiting, "Pulse");
                    if (this.topologyMap.TryGetValue(this.clusterId, out latestHosts) &&
                        !ReferenceEquals(currentHosts, latestHosts))
                    {
                        Logger.LogTrace(LoggerUtils.LogTopology(latestHosts, Resources.ClusterTopologyMonitor_RunMonitoringLoop_TopologyUpdated));
                        return latestHosts ?? [];
                    }
                    else
                    {
                        Logger.LogTrace(LoggerUtils.LogTopology(latestHosts, Resources.ClusterTopologyMonitor_RunMonitoringLoop_TopologyNotUpdated));
                    }
                }
                else
                {
                    Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_WokeUpMonitorWaiting, "Timeout");
                }
            }
        }

        if (DateTime.UtcNow > endTime)
        {
            throw new TimeoutException(string.Format(Resources.ClusterTopologyMonitor_TopologyNotUpdated, timeoutMs));
        }

        Logger.LogTrace(LoggerUtils.LogTopology(latestHosts, Resources.ClusterTopologyMonitor_RunMonitoringLoop_TopologyUpdated));
        return latestHosts ?? [];
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
                newConnection = await this.pluginService.ForceOpenConnection(this.initialHostSpec, this.properties, null, true);

                if (Interlocked.CompareExchange(ref this.monitoringConnection, newConnection, null) == null)
                {
                    Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_OpenedMonitoringConnection, RuntimeHelpers.GetHashCode(this.monitoringConnection), this.initialHostSpec.Host));

                    if (await this.topologyUtils.IsWriterInstanceAsync(this.monitoringConnection, this.ctsTopologyMonitoring.Token))
                    {
                        this.isVerifiedWriterConnection = true;
                        writerVerifiedByThisThread = true;
                        if (RdsUtils.IsRdsInstance(this.initialHostSpec.Host))
                        {
                            Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_IsRdsInstance, this.initialHostSpec.Host);
                            this.writerHostSpec = this.initialHostSpec;
                            Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_WriterMonitoringConnection, this.writerHostSpec));
                        }
                        else
                        {
                            Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_IsNotRdsInstance, this.initialHostSpec.Host);
                            var (nodeId, nodeName) = await this.GetNodeIdAsync(this.monitoringConnection);
                            if (!string.IsNullOrEmpty(nodeId) && !string.IsNullOrEmpty(nodeName))
                            {
                                HostSpec instanceTemplate = await this.GetInstanceTemplateAsync(nodeName, this.monitoringConnection);
                                this.writerHostSpec = this.topologyUtils.CreateHost(nodeId, nodeName, true, 0, null, this.initialHostSpec, instanceTemplate);
                                Logger.LogTrace(string.Format(Resources.ClusterTopologyMonitor_WriterMonitoringConnection, this.writerHostSpec));
                            }
                        }
                    }
                }

                // Don't close this connection - it's now the monitoring connection
                newConnection = null;
            }
            catch (Exception ex) when (ex is DbException or EndOfStreamException)
            {
                // Suppress connection errors and continue
                Logger.LogWarning(ex, Resources.Error_ThrownErrorIgnoredFindingMonitoringConnection, $"{nameof(ex)}");
            }
            finally
            {
                // Clean up if this thread lost the race or if there was an exception
                if (newConnection != null)
                {
                    try
                    {
                        await this.DisposeConnectionAsync(newConnection);
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
            // We verify the writer on initial connection and on failover, but we only want to ignore new topology
            // requests after failover. To accomplish this, the first time we verify the writer we set the ignore end
            // time to 0. Any future writer verifications will set it to a positive value.
            if (Interlocked.CompareExchange(ref this.ignoreNewTopologyRequestsEndTime, 0, -1) != -1)
            {
                Interlocked.Exchange(ref this.ignoreNewTopologyRequestsEndTime, DateTime.UtcNow.Add(IgnoreTopologyRequestDuration).Ticks);
                Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_MonitoringTime, new DateTime(this.ignoreNewTopologyRequestsEndTime, DateTimeKind.Utc));
            }
        }

        if (hosts == null)
        {
            Logger.LogTrace(Resources.ClusterTopologyMonitor_RunMonitoringLoop_NullConnection, RuntimeHelpers.GetHashCode(this.monitoringConnection));
            var connToDispose = Interlocked.Exchange(ref this.monitoringConnection, null);

            this.isVerifiedWriterConnection = false;
            await this.DisposeConnectionAsync(connToDispose);
        }

        return hosts;
    }

    protected async Task<(string? NodeId, string? NodeName)> GetNodeIdAsync(DbConnection connection)
    {
        await this.monitoringConnectionSemaphore.WaitAsync();
        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = this.nodeIdQuery;
            await using var reader = await command.ExecuteReaderAsync(this.ctsTopologyMonitoring.Token);
            if (await reader.ReadAsync(this.ctsTopologyMonitoring.Token))
            {
                return await reader.IsDBNullAsync(0) || await reader.IsDBNullAsync(1)
                    ? (null, null)
                    : (Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture), Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
            }
        }
        catch (Exception ex)
        {
            // Ignore
            Logger.LogWarning(ex, Resources.ClusterTopologyMonitor_ExceptionIgnoredTopologyMonitoringConnection, RuntimeHelpers.GetHashCode(connection));
            throw;
        }
        finally
        {
            this.monitoringConnectionSemaphore.Release();
        }

        return (null, null);
    }

    /// <summary>
    /// Returns the instance template for the given node.
    /// Overridden by <see cref="GlobalAuroraTopologyMonitor"/> to resolve region-specific templates.
    /// </summary>
    protected virtual Task<HostSpec> GetInstanceTemplateAsync(string instanceId, DbConnection connection)
    {
        return Task.FromResult(this.clusterInstanceTemplate);
    }

    protected async Task DisposeConnectionAsync(DbConnection? connection)
    {
        await this.monitoringConnectionSemaphore.WaitAsync();
        try
        {
            if (connection != null)
            {
                Logger.LogTrace(Resources.ClusterTopologyMonitor_DisposeConnectionAsync_ConnectionDisposed, connection.GetType().FullName, RuntimeHelpers.GetHashCode(connection));
                await connection.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            this.monitoringConnectionSemaphore.Release();
        }
    }

    protected async Task DelayAsync(bool useHighRefreshRate)
    {
        if (this.highRefreshRateEndTime > DateTime.MinValue && this.highRefreshRateEndTime < DateTime.UtcNow)
        {
            useHighRefreshRate = true;
        }

        if (this.RequestToUpdateTopology)
        {
            useHighRefreshRate = true;
        }

        var delay = useHighRefreshRate ? this.highRefreshRate : this.refreshRate;
        var start = DateTime.UtcNow;
        var end = start.Add(delay);

        do
        {
            await Task.Delay(50, this.ctsTopologyMonitoring.Token);
        }
        while (!this.RequestToUpdateTopology && DateTime.UtcNow < end && !this.ctsTopologyMonitoring.Token.IsCancellationRequested);
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
            Logger.LogTrace(LoggerUtils.LogTopology(hosts, Resources.ClusterTopologyMonitor_UpdateTopologyCache_TopologyUpdate));
            this.RequestToUpdateTopology = false;
            Logger.LogTrace(Resources.ClusterTopologyMonitor_UpdateTopologyCache_NotifyingLock);
            Monitor.PulseAll(this.topologyUpdatedLock);
        }
    }

    /// <summary>
    /// Checks whether all reader node monitors report a consistent topology for a sustained period.
    /// If so, updates the topology cache — unblocking <see cref="WaitTillTopologyGetsUpdated"/>.
    /// This handles the case where the writer is unreachable but no actual failover occurred.
    /// </summary>
    protected void CheckForStableReaderTopologies()
    {
        var latestHosts = this.topologyMap.Get<IList<HostSpec>>(this.clusterId);
        if (latestHosts == null || latestHosts.Count == 0)
        {
            Interlocked.Exchange(ref this.stableTopologiesStartTicks, 0);
            return;
        }

        // Ensure all node monitors have completed at least one cycle
        var readerIds = latestHosts.Select(h => h.HostId).Where(id => id != null).ToList();
        foreach (var id in readerIds)
        {
            if (!this.completedOneCycle.GetValueOrDefault(id!, false))
            {
                Interlocked.Exchange(ref this.stableTopologiesStartTicks, 0);
                return;
            }
        }

        // Get the first reader topology as reference
        var readerTopologyFirstEntry = this.readerTopologiesById.Values.FirstOrDefault();
        if (readerTopologyFirstEntry == null || readerTopologyFirstEntry.Count == 0)
        {
            Interlocked.Exchange(ref this.stableTopologiesStartTicks, 0);
            return;
        }

        // Check whether all reader topologies match (comparing host, port, role, availability)
        var referenceKey = readerTopologyFirstEntry
            .Select(h => (h.Host, h.Port, h.Role, h.Availability))
            .OrderBy(t => t.Host)
            .ToList();

        foreach (var topology in this.readerTopologiesById.Values.Skip(1))
        {
            var key = topology
                .Select(h => (h.Host, h.Port, h.Role, h.Availability))
                .OrderBy(t => t.Host)
                .ToList();

            if (!referenceKey.SequenceEqual(key))
            {
                Interlocked.Exchange(ref this.stableTopologiesStartTicks, 0);
                return;
            }
        }

        // All reader topologies match
        long startTicks = Interlocked.Read(ref this.stableTopologiesStartTicks);
        if (startTicks == 0)
        {
            Interlocked.Exchange(ref this.stableTopologiesStartTicks, DateTime.UtcNow.Ticks);
            return;
        }

        var elapsed = DateTime.UtcNow - new DateTime(startTicks, DateTimeKind.Utc);
        if (elapsed >= StableTopologiesDuration)
        {
            Interlocked.Exchange(ref this.stableTopologiesStartTicks, 0);
            Logger.LogDebug(
                Resources.ClusterTopologyMonitor_MatchingReaderTopologies,
                (long)elapsed.TotalMilliseconds);
            this.UpdateTopologyCache(readerTopologyFirstEntry);
        }
    }

    protected virtual async Task<IList<HostSpec>?> QueryForTopologyAsync(DbConnection connection)
    {
        await this.monitoringConnectionSemaphore.WaitAsync();
        try
        {
            Logger.LogDebug(Resources.ClusterTopologyMonitor_QueryForTopologyAsync, DefaultTopologyQueryTimeoutSec);

            return await this.topologyUtils.QueryForTopologyAsync(
                connection,
                this.initialHostSpec,
                this.clusterInstanceTemplate,
                this.hostListProviderService,
                this.ctsTopologyMonitoring.Token);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, string.Format(Resources.ClusterTopologyMonitor_ErrorProcessingQueryResults, ex));
            return null;
        }
        finally
        {
            this.monitoringConnectionSemaphore.Release();
        }
    }

    public void Dispose()
    {
        Logger.LogTrace(Resources.ClusterTopologyMonitor_Dispose_DisposingClusterTopologyMonitor, this.initialHostSpec.Host);
        lock (this.disposeLock)
        {
            if (this.disposed)
            {
                return;
            }

            this.disposed = true;
        }

        this.NodeThreadsStop = true;
        this.ctsTopologyMonitoring.Cancel();

        try
        {
            this.monitoringTask.Wait(TimeSpan.FromSeconds(30));
        }
        catch (Exception ex)
        {
            Logger.LogWarning(Resources.ClusterTopologyMonitor_Dispose_ErrorOccured, ex.Message);
        }

        var conn = Interlocked.Exchange(ref this.monitoringConnection, null);
        conn?.Dispose();

        var readerConn = Interlocked.Exchange(ref this.nodeThreadsReaderConnection, null);
        readerConn?.Dispose();

        var writerConn = Interlocked.Exchange(ref this.nodeThreadsWriterConnection, null);
        writerConn?.Dispose();

        this.ctsTopologyMonitoring.Dispose();
    }

    private class NodeMonitoringTask(
        ClusterTopologyMonitor monitor,
        HostSpec hostSpec,
        HostSpec? writerHostSpec)
    {
        private static readonly ILogger<NodeMonitoringTask> NodeMonitorLogger = LoggerUtils.GetLogger<NodeMonitoringTask>();

        private bool writerChanged = false;

        public async Task RunNodeMonitoringAsync(CancellationToken token)
        {
            DbConnection? connection = null;
            bool updateTopology = false;
            DateTime start = DateTime.UtcNow;

            LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Trace, Resources.ClusterTopologyMonitor_RunNodeMonitoringAsync_StartNode);
            try
            {
                while (!monitor.NodeThreadsStop && !token.IsCancellationRequested)
                {
                    if (connection == null)
                    {
                        try
                        {
                            connection = await monitor.pluginService.ForceOpenConnection(hostSpec, monitor.properties, null, true);
                            monitor.pluginService.SetAvailability(hostSpec.AsAliases(), HostAvailability.Available);
                        }
                        catch (Exception ex) when (ex is DbException or EndOfStreamException)
                        {
                            monitor.pluginService.SetAvailability(hostSpec.AsAliases(), HostAvailability.Unavailable);
                            await monitor.DisposeConnectionAsync(connection);
                            connection = null;
                            monitor.completedOneCycle[hostSpec.HostId!] = true;
                            monitor.readerTopologiesById.TryRemove(hostSpec.HostId!, out _);
                        }
                    }

                    if (connection != null)
                    {
                        bool isWriter = false;
                        try
                        {
                            isWriter = await monitor.topologyUtils.IsWriterInstanceAsync(connection, token);
                        }
                        catch (Exception ex) when (ex is DbException or EndOfStreamException)
                        {
                            await monitor.DisposeConnectionAsync(connection);
                            connection = null;
                        }

                        if (isWriter && connection != null)
                        {
                            try
                            {
                                if (await monitor.pluginService.GetHostRole(connection) != HostRole.Writer)
                                {
                                    // The first connection after failover may be stale.
                                    isWriter = false;
                                }
                            }
                            catch (DbException)
                            {
                                // Invalid connection, retry on the next loop iteration.
                                await monitor.DisposeConnectionAsync(connection);
                                connection = null;
                                monitor.completedOneCycle[hostSpec.HostId!] = true;
                                monitor.readerTopologiesById.TryRemove(hostSpec.HostId!, out _);
                                await Task.Delay(100, token);
                                continue;
                            }
                        }

                        if (isWriter)
                        {
                            if (Interlocked.CompareExchange(ref monitor.nodeThreadsWriterConnection, connection, null) == null)
                            {
                                LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Information, string.Format(Resources.NodeMonitoringTask_DetectedWriter, hostSpec.Host));
                                await monitor.FetchTopologyAndUpdateCacheAsync(connection);
                                hostSpec.Availability = HostAvailability.Available;
                                monitor.nodeThreadsWriterHostSpec = hostSpec;
                                monitor.NodeThreadsStop = true;
                            }
                            else
                            {
                                await monitor.DisposeConnectionAsync(connection);
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
                                else
                                {
                                    await this.ReaderThreadFetchTopologyAsync(connection);
                                }
                            }
                        }
                    }

                    monitor.completedOneCycle[hostSpec.HostId!] = true;
                    await Task.Delay(100, token);
                }
            }
            catch (OperationCanceledException ex)
            {
                // Expected
                LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Trace, ex, Resources.ClusterTopologyMonitor_RunNodeMonitoringAsync_OperationCancelled, ex.Message);
            }
            catch (Exception ex)
            {
                LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Warning, ex, Resources.ClusterTopologyMonitor_RunNodeMonitoringAsync_UnknownException, ex.Message);
            }
            finally
            {
                monitor.completedOneCycle[hostSpec.HostId!] = true;
                monitor.readerTopologiesById.TryRemove(hostSpec.HostId!, out _);
                await monitor.DisposeConnectionAsync(connection);
                LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Trace, string.Format(Resources.NodeMonitoringTask_ThreadCompleted, RuntimeHelpers.GetHashCode(this), (DateTime.UtcNow - start).TotalMilliseconds));
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
                monitor.readerTopologiesById[hostSpec.HostId!] = hosts;
                var latestWriterHostSpec = hosts.FirstOrDefault(x => x.Role == HostRole.Writer);

                if (this.writerChanged)
                {
                    monitor.UpdateTopologyCache(hosts);
                    return;
                }

                if (latestWriterHostSpec != null && writerHostSpec != null &&
                    latestWriterHostSpec.GetHostAndPort() != writerHostSpec.GetHostAndPort())
                {
                    LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Trace, string.Format(
                        Resources.NodeMonitoringTask_WriterNodeChanged,
                        writerHostSpec.Host,
                        latestWriterHostSpec.Host));

                    this.writerChanged = true;
                    monitor.UpdateTopologyCache(hosts);
                }
            }
            catch (Exception ex)
            {
                // Ignore errors
                LoggerUtils.MonitoringLogWithHost(hostSpec, NodeMonitorLogger, LogLevel.Trace, ex, Resources.ClusterTopologyMonitor_ReaderThreadFetchTopologyAsync_ExceptionIgnored, ex.Message);
            }
        }
    }
}
